import time
import boto3
from pyspark.sql import SparkSession
from src.base import BaseConfig
from datetime import datetime
from botocore.config import Config

bucket_name = BaseConfig.S3_BUCKET
glue_role = "arn:aws:iam::559050214252:role/service-role/AWSGlueServiceRole-test"


def get_s3_subfolders(s3, bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    return [obj["Prefix"].split("/")[-2] for obj in response.get("CommonPrefixes", [])]


# service 리스트 가져오기
def get_service_list(s3):
    service_list = get_s3_subfolders(s3, bucket_name, "results/")

    return service_list


def get_crawler_state(glue, crawler_name, status=None):
    if status == "last":
        try:
            response = glue.get_crawler(Name=crawler_name)
            last_run_status = (
                response.get("Crawler", {})
                .get("LastCrawl", {})
                .get("Status", "UNKNOWN")
            )
            return last_run_status
        except glue.exceptions.EntityNotFoundException:
            return "NOT_FOUND"
    else:
        try:
            response = glue.get_crawler(Name=crawler_name)
            return response.get("Crawler", {}).get("State", "UNKNOWN")
        except glue.exceptions.EntityNotFoundException:
            return "NOT_FOUND"


def create_database(glue, service_name):
    """create database if not exists"""
    try:
        glue.get_database(Name=service_name)
        # print(f"Database '{service_name}' already exists. \n")
        return False
    except glue.exceptions.EntityNotFoundException:
        database_input = {"Name": service_name}

        glue.create_database(DatabaseInput=database_input)

        print(f"Database '{service_name}' created successfully. \n")


def create_crawler_if_not_exists(
    glue, crawler_name, s3_path, database_name, role, existing_crawlers
):
    """create crawler if not exists"""
    if crawler_name in existing_crawlers:
        # print(f"Crawler '{crawler_name}' already exists. SKIP! \n")
        return False
    else:
        glue.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_path}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
        )
        print(f"Crawler '{crawler_name}' successfully created! (S3 path: {s3_path}) \n")
        return True


def create_crawler(glue, s3, existing_crawlers):
    # table case - 서비스별 테이블에 대한 전체 크롤러 생성
    service_list = get_service_list(s3)
    for service in service_list:
        create_database(glue, service)
        response = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=f"results/{service}/", Delimiter="/"
        )
        middle_dirs = [
            obj["Prefix"].split("/")[-2] for obj in response.get("CommonPrefixes", [])
        ]
        for mid in middle_dirs:
            table_list = get_s3_subfolders(s3, bucket_name, f"results/{service}/{mid}/")
            for table in table_list:
                crawler_name = f"crawler_{service}_{table}"
                s3_path = f"s3://{bucket_name}/results/{service}/{mid}/{table}/"
                create_crawler_if_not_exists(
                    glue, crawler_name, s3_path, service, glue_role, existing_crawlers
                )

    # etl-batch-status에 대한 크롤러 생성
    crawler_name = "crawler_etl_batch_status"
    # Check if the crawler already exists before trying to create it
    if crawler_name not in existing_crawlers:
        s3_path = f"s3://{bucket_name}/etl-batch-status/"
        create_crawler_if_not_exists(
            glue,
            crawler_name,
            s3_path,
            "table_monitoring",
            glue_role,
            existing_crawlers,
        )


# 크롤러 생성
def create_crawler_and_start(schema, data_layer, table_name):
    try:
        # AWS 클라이언트 설정
        s3 = boto3.client("s3")
        glue = boto3.client(
            "glue",
            region_name="ap-northeast-2",
            config=Config(
                retries={
                    "max_attempts": 10,  # 기본은 4
                    "mode": "standard",  # 또는 'adaptive' (네트워크 지연 반영)
                }
            ),
        )

        crawler_name = f"crawler_{schema}_{data_layer}_{table_name}"
        existing_crawlers = glue.list_crawlers()["CrawlerNames"]
        print(f"[{datetime.now()}] calling list_crawlers()...")

        if crawler_name not in existing_crawlers:
            # 크롤러 생성
            s3_path = f"s3://{bucket_name}/results/{schema}/{data_layer}/{table_name}/"
            create_crawler_if_not_exists(
                glue, crawler_name, s3_path, schema, glue_role, existing_crawlers
            )
        else:
            glue.start_crawler(Name=crawler_name)
            print(f"crawler_name: {crawler_name} STARTED")
            # 크롤러 상태 확인
            crawler_status = get_crawler_state(glue, crawler_name)

            while crawler_status != "READY":
                print("wait 5 seconds ...")
                time.sleep(5)
                crawler_status = get_crawler_state(glue, crawler_name)

        print(f"Crawler '{crawler_name}' RUN SUCCESS \n")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e


def repair_glue_partitions(
    spark: SparkSession, schema: str, data_layer: str, table_name: str
):
    try:
        """Repair Glue catalog partitions for the specified table"""
        table_name = table_name.split(" ")[0] if " " in table_name else table_name
        print(">> \n")
        print(f"MSCK REPAIR TABLE {schema}.{data_layer}_{table_name}")
        spark.sql(f"MSCK REPAIR TABLE {schema}.{data_layer}_{table_name}")
        print(
            f">>> Repair Glue Partition '{schema}.{data_layer}_{table_name}' SUCCESS \n"
        )
    except Exception as e:
        print(f">>> Repair Glue Partition FAILED: {e}, \n")
        create_crawler_and_start(schema, data_layer, table_name)
