from pyspark.sql.functions import *
from pyspark.sql import DataFrame, SparkSession
from functools import reduce
from .glue_utils import repair_glue_partitions
from datetime import datetime
from .date_utils import get_date
from .db_utils import S3Handler
from pyspark.sql.types import DateType
import boto3
import logging


def filter_by_time(
    sdf: DataFrame, start_date: str, end_date: str, timefield: str = "dt_utc"
):
    """Filter DataFrame by single time field"""
    return sdf.filter(
        (col(timefield) >= start_date) & (col(timefield) < end_date)
    ).withColumn(timefield, col(timefield).cast(DateType()))


def filter_by_time_end(sdf: DataFrame, end_date: str, timefield: str = "dt_utc"):
    """Filter DataFrame by single time field"""
    return sdf.filter((col(timefield) < end_date)).withColumn(
        timefield, col(timefield).cast(DateType())
    )


def filter_by_timezone(sdf: DataFrame, timezone: str, start_date: str, end_date: str):
    """Filter DataFrame by timezone with start and end date"""
    other_timezone = "dt_kst" if timezone == "utc" else "dt_utc"
    return (
        sdf.filter(
            (col(f"dt_{timezone}") >= start_date) & (col(f"dt_{timezone}") < end_date)
        )
        .withColumn("timezone_type", lit(f"{timezone}"))
        .withColumnRenamed(f"dt_{timezone}", "bdate")
        .withColumn("bdate", col("bdate").cast(DateType()))
        .drop(other_timezone)
    )


def filter_by_timezone_end(sdf: DataFrame, timezone: str, end_date: str):
    """Filter DataFrame by timezone with only end date"""
    other_timezone = "dt_kst" if timezone == "utc" else "dt_utc"
    return (
        sdf.filter(col(f"dt_{timezone}") < end_date)
        .withColumn("timezone_type", lit(f"{timezone}"))
        .withColumnRenamed(f"dt_{timezone}", "bdate")
        .withColumn("bdate", col("bdate").cast(DateType()))
        .drop(other_timezone)
    )


def union_dataframe(*dfs: DataFrame):
    valid_dfs = [df for df in dfs if isinstance(df, DataFrame)]

    if not valid_dfs:
        print("No valid DataFrames to union.")
        return None

    if len(valid_dfs) == 1:
        return valid_dfs[0]

    return reduce(lambda df1, df2: df1.unionByName(df2), valid_dfs)


def get_job_info(file_name: str) -> dict:
    """Get job information from file name and dates"""
    data_layer = file_name.split("_")[0]
    table_name = file_name.split(f"{data_layer}_")[1].split(".py")[0]
    start_date, end_date = get_date()
    batch_bdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "data_layer": data_layer,
        "table_name": table_name,
        "start_date": start_date,
        "end_date": end_date,
        "batch_bdate": batch_bdate,
    }


def write_to_s3(
    spark: SparkSession, df: DataFrame, job_info: dict, partition_cols: list = None
) -> None:
    """Write DataFrame to S3 and manage table metadata"""
    s3_writer = S3Handler()
    try:
        s3_writer.write_table(
            df=df,
            data_layer=job_info["data_layer"],
            schema_name=job_info["schema_name"],
            table_name=job_info["table_name"],
            partition_cols=partition_cols,
        )
        print(f">>> S3 write SUCCESS \n")

        s3_writer.table_manager(
            spark=spark,
            schema_name=job_info["schema_name"],
            table_name=job_info["table_name"],
            batch_bdate=job_info["batch_bdate"],
            start_date=job_info["start_date"],
            end_date=job_info["end_date"],
            status=1,
        )
        print(f">>> Table Manager SUCCESS \n")

        repair_glue_partitions(
            spark,
            job_info["schema_name"],
            job_info["data_layer"],
            job_info["table_name"],
        )

    except Exception as e:
        print(f">>> S3 write FAILED: {e}")
        s3_writer.table_manager(
            spark=spark,
            schema_name=job_info["schema_name"],
            table_name=job_info["table_name"],
            batch_bdate=job_info["batch_bdate"],
            start_date=job_info["start_date"],
            end_date=job_info["end_date"],
            status=0,
        )
        print(f">>> Table Manager SUCCESS \n")
        raise Exception("fail")


def _get_log_from_s3(S3_BUCKET, S3_PATH):
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_PATH)
        return response["Body"].read().decode("utf-8")
    except:
        return ""


def run_pipeline(spark, STEPS, S3_BUCKET, S3_PATH):
    # 로깅 설정
    logging.basicConfig(
        filename="pipeline_status.log",  # 로컬에 임시로 저장
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    s3_client = boto3.client("s3")
    # S3에서 마지막 실행 단계 확인
    last_step = None
    last_status = None
    log_content = _get_log_from_s3(S3_BUCKET, S3_PATH)

    if log_content:
        # 로그의 각 라인을 역순으로 확인
        for line in reversed(log_content.split("\n")):
            if "Completed" in line or "Failed" in line:
                for step_name, _ in STEPS:
                    if step_name in line:
                        last_step = step_name
                        last_status = "Completed" if "Completed" in line else "Failed"
                        break
                if last_step:
                    break

    # 실행 시작 지점 결정
    print(">>>")
    start_execution = True
    if last_step:
        if last_step == STEPS[-1][0] and last_status == "Completed":
            # 마지막 스텝이 성공적으로 완료되었다면 처음부터 실행
            start_execution = True
        else:
            # 중간에 실패했거나 중간 스텝까지만 완료된 경우 해당 스텝부터 실행
            start_execution = False
            # 실패한 스텝의 인덱스를 찾아서 그 스텝부터 시작
            for i, (step_name, _) in enumerate(STEPS):
                if step_name == last_step:
                    STEPS = STEPS[i:]
                    break

    for step_name, step_func in STEPS:
        if not start_execution:
            if last_step == step_name:
                start_execution = True

        try:
            print(f"Job Will Be Start FROM {step_name}")
            logging.info(f"Starting {step_name}")
            step_func(spark)
            logging.info(f"Completed {step_name}")
        except Exception as e:
            error_msg = f"Failed {step_name}: {str(e)}"
            logging.error(error_msg)
            # S3에 로그 업데이트
            with open("pipeline_status.log", "r") as f:
                s3_client.put_object(Bucket=S3_BUCKET, Key=S3_PATH, Body=f.read())
            raise

        # 각 단계 성공 후 로그 업데이트
        with open("pipeline_status.log", "r") as f:
            s3_client.put_object(Bucket=S3_BUCKET, Key=S3_PATH, Body=f.read())


def compare_bdate_df(
    spark, df1: DataFrame, df2: DataFrame, df1_col: str, df2_col: str
) -> bool:
    df1_max = spark.table(df1).agg(max(df1_col).alias("max_date")).collect()[0][0]
    df2_max = spark.table(df2).agg(max(df2_col).alias("max_date")).collect()[0][0]

    return df1_max == df2_max


# def date_format(start_date: str, end_date: str) -> DataFrame:
#     return spark.sql()
