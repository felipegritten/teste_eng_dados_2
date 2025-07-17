from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import DataFrame
from pyspark.sql import Window
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import botocore
import boto3
from datetime import date

class Utils:
    @staticmethod
    def create_glue_database(database_name, location_uri):
        try:
            response = boto3.client('glue').create_database(
                DatabaseInput={
                    'Name': f'{database_name}',
                    'Description': 'Criado dentro do job Glue via boto3',
                    'LocationUri': f'{location_uri}'
                })
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                print(f"Database {database_name} ja existe")


class BronzeClientesSinteticos:
    def __init__(self, raw_data_s3: str):
        self.raw_data_s3: str = raw_data_s3
        self.schema_data_s3 = self.return_schema()

    @staticmethod
    def return_schema() -> StructType:
        schema_: StructType = StructType([
                                StructField("cod_cliente", StringType())
                                , StructField("nm_cliente", StringType())
                                , StructField("nm_pais_cliente", StringType())
                                , StructField("nm_cidade_cliente", StringType())
                                , StructField("nm_rua_cliente", StringType())
                                , StructField("num_casa_cliente", StringType())
                                , StructField("telefone_cliente", StringType())
                                , StructField("dt_nascimento_cliente", DateType())
                                , StructField("dt_atualizacao", DateType())
                                , StructField("tp_pessoa", StringType())
                                , StructField("vl_renda", FloatType())
                                              ])
        return schema_

    def transform_dataset(self):
        df_raw: DataFrame = (
                            spark.read.format('csv')
                                 .option("header", True)
                                 .schema(self.schema_data_s3)
                                 .load(self.raw_data_s3)
                                    .withColumn("nm_cliente", F.upper(F.col("nm_cliente")))
                                    .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
                            )
        return df_raw

class SilverClientesSinteticos():
    def __init__(self, table: str):
        self.table: str = table
    def transform_bronze_table(self):
        window = Window.partitionBy("cod_cliente").orderBy("dt_atualizacao")
        df_bronze: DataFrame = (
                                spark.read.table(self.table)
                                    .withColumn("row_number", F.row_number().over(window))
                                    .filter(F.col("row_number") == "1")
                                    .withColumn("num_telefone_cliente", F.when(F.col("num_telefone_cliente").rlike(r"^\(\d{2}\)\d{5}-\d{4}$"), F.col("num_telefone_cliente")).otherwise(F.lit(None)))
                               )
        return df_bronze


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

Utils.create_glue_database("bronze_clientes", "s3://bucket-bronze/")
BronzeClientesSinteticos(raw_data_s3="s3://itau-bucket/clientes-sinteticos.csv").transform_dataset() \
    .withColumn("anomesdia", F.lit(f"{date.today().strftime('%Y%m%d')}")) \
    .write.saveAsTable(name="bronze_clientes.tabela_cliente",
                       mode='append',
                       format="parquet",
                       partitionBy="anomesdia",
                       path="s3://bucket-bronze/tabela_cliente_landing")

Utils.create_glue_database("silver_clientes", location_uri="s3://bucket-silver/")
SilverClientesSinteticos(table="bronze_clientes.tabela_cliente")\
    .transform_bronze_table()\
    .withColumn("anomesdia", F.lit(f"{date.today().strftime('%Y%m%d')}"))\
    .drop(F.col("row_number"))\
    .write.saveAsTable(name="silver_clientes.tabela_cliente",
                       mode='append',
                       format="parquet",
                       partitionBy="anomesdia",
                       path="s3://bucket-silver/tb_cliente")
job.commit()