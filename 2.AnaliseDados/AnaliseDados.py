from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

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

df_origem = spark.read.format('csv').option("header", True).schema(schema_).load("s3://itau-bucket/clientes-sinteticos.csv")

df_mais_atualizado = df_origem.select(F.col("cod_cliente"), F.col("nm_cliente"), F.col("dt_atualizacao"))\
                             .groupBy(F.col("cod_cliente")).count().orderBy(F.col("count").desc()).limit(5).show(10, False)
# +-----------+-----+
# |cod_cliente|count|
# +-----------+-----+
# |479        |5    |
# |878        |5    |
# |396        |5    |
# |855        |4    |
# |620        |3    |
# +-----------+-----+

df_origem.withColumn("idade", F.round(F.months_between(F.current_date(), F.col("dt_nascimento_cliente")) / F.lit(12), 0).cast(IntegerType())).show(10, False)
# +-----------+
# |idade_media|
# +-----------+
# |50         |
# +-----------+