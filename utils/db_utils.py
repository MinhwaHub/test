from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List
import boto3
from .decorators import timing_decorator
from src.base import BaseConfig
import pandas as pd


@dataclass
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


class BaseDBReader(ABC):
    def __init__(self, spark: SparkSession, config: DBConfig):
        self.spark = spark
        self.config = config
        self.jdbc_url = self._create_jdbc_url()

    @property
    @abstractmethod
    def jdbc_prefix(self) -> str:
        """Return JDBC URL prefix for the specific database"""
        pass

    @property
    @abstractmethod
    def driver(self) -> str:
        """Return JDBC driver class name"""
        pass

    def _create_jdbc_url(self) -> str:
        return f"{self.jdbc_prefix}://{self.config.host}:{self.config.port}/{self.config.dbname}"

    @timing_decorator
    def read_table(
        self,
        table_name: str,
        time_field: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        select_field: Optional[str] = "*",
        query: Optional[str] = None,
        options: Optional[dict] = None,
        mode: str = "append",
    ):
        """
        Read table from database with optional additional options
        """
        print(">>>")
        print(f"Reading table: {table_name.split(' ')[0]} \n")
        print(">>>")
        reader = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("user", self.config.user)
            .option("password", self.config.password)
            .option("driver", self.driver)
        )

        if options:
            for key, value in options.items():
                reader = reader.option(key, value)

        # time setting
        if self.jdbc_prefix == "jdbc:postgresql":
            time_query = f"{time_field} AS ts_utc, TO_CHAR({time_field},'YYYY-MM-DD') AS dt_utc, {time_field} + INTERVAL '9 hours' AS ts_kst, TO_CHAR({time_field} + INTERVAL '9 hours', 'YYYY-MM-DD') AS dt_kst FROM {table_name} WHERE TO_CHAR({time_field}, 'YYYY-MM-DD') >= '{start_date}'  AND TO_CHAR({time_field}, 'YYYY-MM-DD') < '{end_date}' "
        elif self.jdbc_prefix == "jdbc:mysql":
            if mode == "overwrite":
                time_query = f"FROM_UNIXTIME({time_field}) AS ts_utc, DATE(FROM_UNIXTIME({time_field})) AS dt_utc, DATE_ADD(FROM_UNIXTIME({time_field}), INTERVAL 9 HOUR) AS ts_kst, DATE(DATE_ADD(FROM_UNIXTIME({time_field}), INTERVAL 9 HOUR)) AS dt_kst FROM {table_name} WHERE {time_field} < UNIX_TIMESTAMP('{end_date} 00:00:00')"
            else:
                time_query = f"FROM_UNIXTIME({time_field}) AS ts_utc, DATE(FROM_UNIXTIME({time_field})) AS dt_utc, DATE_ADD(FROM_UNIXTIME({time_field}), INTERVAL 9 HOUR) AS ts_kst, DATE(DATE_ADD(FROM_UNIXTIME({time_field}), INTERVAL 9 HOUR)) AS dt_kst FROM {table_name} WHERE {time_field} >= UNIX_TIMESTAMP('{start_date} 00:00:00') AND {time_field} < UNIX_TIMESTAMP('{end_date} 00:00:00')"

        if time_field:
            final_query = f"SELECT {select_field} , {time_query}"
            if query:
                final_query += f"AND {query}"
            print(f"Excute Query: {final_query} \n")
            reader = reader.option("query", final_query)
        else:
            if query:
                final_query = f"SELECT {select_field} FROM {table_name} WHERE {query}"
                print(f"Excute Query: {final_query} \n")
                reader = reader.option("query", final_query)
            elif select_field:
                final_query = f"SELECT {select_field} FROM {table_name}"
                print(f"Excute Query: {final_query} \n")
                reader = reader.option("query", final_query)
            else:
                print("Attach Table")
                reader = reader.option("dbtable", table_name)

        return reader.load()


class PostgreSQLReader(BaseDBReader):
    @property
    def jdbc_prefix(self) -> str:
        return "jdbc:postgresql"

    @property
    def driver(self) -> str:
        return "org.postgresql.Driver"


class MySQLReader(BaseDBReader):
    @property
    def jdbc_prefix(self) -> str:
        return "jdbc:mysql"

    @property
    def driver(self) -> str:
        return "com.mysql.cj.jdbc.Driver"


class S3Handler:
    def __init__(self, profile_name: str = "s3"):
        # self.session = boto3.Session(profile_name=profile_name)
        # self.credentials = self.session.get_credentials()

        # # 액세스 키, 비밀 키, 세션 토큰 가져오기
        # aws_access_key = self.credentials.access_key
        # aws_secret_key = self.credentials.secret_key
        # aws_token = self.credentials.token

        # # S3 리소스를 생성하여 S3 객체에 접근
        # self.s3_resource = self.session.resource(
        #     "s3",
        #     aws_access_key_id=aws_access_key,
        #     aws_secret_access_key=aws_secret_key,
        #     aws_session_token=aws_token,
        # )
        self.s3_client = boto3.client(profile_name)

    def _get_s3_path(self, schema_name: str, data_layer: str, table_name: str) -> str:
        """Generate S3 path for the table"""
        table_name = table_name.split(" ")[0]
        return f"{BaseConfig.S3_PATH}results/{schema_name}/{data_layer}/{data_layer}_{table_name}/"

    @timing_decorator
    def write_table(
        self,
        df: DataFrame,
        data_layer: str,
        schema_name: str,
        table_name: str,
        partition_cols: Optional[List[str]],
        mode: str = "append",
        format: str = "parquet",
    ):
        """
        Write the DataFrame to S3 with partitioning support.

        Args:
            df: DataFrame to save.
            path: S3 path where the data should be saved.
            partition_cols: List of columns to partition by.
            mode: Write mode ('append', 'overwrite', etc.)
            format: Data format ('parquet', 'csv', etc.)
        """
        if partition_cols is None:
            partition_cols = ["bdate"] if table_name.endswith("_d") else ["dt_utc"]
        elif partition_cols == []:
            partition_cols = None

        def delete_partition_data(path: str, partition_values: List[str]):
            bucket = BaseConfig.S3_BUCKET
            print(bucket)
            prefix = path.split(bucket + "/")[1]
            print(prefix)
            time_partition = (
                "dt_utc" if "dl_" in prefix or not prefix.endswith("_d/") else "bdate"
            )

            # Determine target prefix(es) to delete
            prefixes = (
                [f"{prefix}{time_partition}={value}/" for value in partition_values]
                if partition_values
                else [prefix]
            )

            for target_prefix in prefixes:
                print(target_prefix)
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=target_prefix
                )
                print(response)
                if "Contents" in response:
                    objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                    if objects:
                        self.s3_client.delete_objects(
                            Bucket=bucket, Delete={"Objects": objects}
                        )
                        print(f"Deleted objects under: {target_prefix}")

        def save_with_partition(
            df: DataFrame,
            partition_cols: List[str],
            data_layer: str,
            schema_name: str,
            table_name: str,
            format: str = "parquet",
            mode: str = "append",
        ):
            """
            Save DataFrame to S3 with partitioning.

            Args:
                df: DataFrame to save.
                partition_cols: List of partition columns.
                path: S3 path where the data should be saved.
                format: Data format ('parquet', 'csv', etc.)
                mode: Write mode ('append', 'overwrite', etc.)
            """
            # Extract bucket name from path
            print("save_with_partition")
            path = self._get_s3_path(schema_name, data_layer, table_name)

            writer = df.write.format(format).mode(mode)
            if partition_cols:
                print(f"partition_cols: {partition_cols}")

                partition_values_map = {}
                for col in partition_cols:
                    values = [row[col] for row in df.select(col).distinct().collect()]
                    partition_values_map[col] = values
                    delete_partition_data(path, values)

                writer = writer.partitionBy(*partition_cols)
                print(f"Appended data to partition: {partition_values_map} \n")
            else:
                delete_partition_data(path, None)

            # Save to S3
            print(f" data will be {mode} to {path} \n")
            writer.save(path)
            print("sae  ")

        # Start saving the DataFrame with partitioning
        save_with_partition(
            df=df,
            partition_cols=partition_cols,
            data_layer=data_layer,
            schema_name=schema_name,
            table_name=table_name,
            format=format,
            mode=mode,
        )

    def table_manager(
        self,
        spark,
        schema_name,
        table_name,
        batch_bdate,
        start_date,
        end_date,
        status: bool = 0,
        mode: str = "append",
        format: str = "parquet",
    ):
        table_name = table_name.split(" ")[0]
        base_dict = {
            "schema_name": schema_name,
            "table_name": table_name,
            "batch_bdate": batch_bdate[:10],
            "batch_ts": batch_bdate,
            "start_date": start_date,
            "end_date": end_date,
            "status": status,
        }

        df = spark.createDataFrame([base_dict])
        df.write.format(format).mode(mode).partitionBy("batch_bdate").save(
            f"{BaseConfig.ETL_STATUS}"
        )
        # df = pd.DataFrame([base_dict])
        # df.to_parquet(
        #     f"{BaseConfig.ETL_STATUS}", partition_cols=["batch_bdate"], mode=mode
        # )

    def append_table(
        self,
        df: DataFrame,
        data_layer: str,
        schema_name: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        mode: str = "append",
        format: str = "parquet",
    ):
        """Append DataFrame to S3 without deleting existing data"""
        if partition_cols is None:
            partition_cols = ["bdate"] if table_name.endswith("_d") else ["dt_utc"]
        elif partition_cols == []:
            partition_cols = None
        path = self._get_s3_path(schema_name, data_layer, table_name)

        # 단순히 추가만 하는 로직
        writer = df.write.format(format).mode(mode)
        if partition_cols:
            partition_cols = ["bdate"] if table_name.endswith("_d") else partition_cols
            writer = writer.partitionBy(*partition_cols)

            # 로깅 용도
            partition_values = {
                col: [row[col] for row in df.select(col).distinct().collect()]
                for col in partition_cols
            }
            print(f"Appending data to partitions: {partition_values}")

        print(f"Appending data to {path}")
        writer.save(path)
