import os

# Aponta para H:\ para arquivos temporários do Snowflake
os.environ['TMP'] = r'H:\temp_snowflake'
os.environ['TEMP'] = r'H:\temp_snowflake'

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    hadoop_path = "C:\\hadoop"  # seu path do Hadoop
    os.environ['HADOOP_HOME'] = hadoop_path
    os.environ['PATH'] = f"{os.environ['PATH']};{hadoop_path}\\bin"
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

SF_USER = "SEU USER"
SF_PASSWORD = "SUA SENHA"
SF_ACCOUNT = "SEU ID"
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE = "MEU_DB"
SF_SCHEMA = "PUBLIC"

spark = get_spark_session("UploadRefinedToSnowflake")

conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    schema=SF_SCHEMA
)

# Garante o uso do schema
conn.cursor().execute(f"USE SCHEMA {SF_SCHEMA}")

REFINED_PATH = "data_lake/refined/"

for dataset_dir in os.listdir(REFINED_PATH):
    dataset_path = os.path.join(REFINED_PATH, dataset_dir)

    if os.path.isdir(dataset_path):
        print(f"\n Enviando dataset: {dataset_dir} para Snowflake...")

        parquet_files = []
        for root, dirs, files in os.walk(dataset_path):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))

        if not parquet_files:
            print(f"⚠️ Nenhum arquivo parquet encontrado em {dataset_path}, pulando...")
            continue

        df_spark = spark.read.parquet(*parquet_files)
        df_pandas = df_spark.toPandas()

        table_name = f"{dataset_dir}_refined".lower()
        qualified_table_name = f"{SF_DATABASE}.{SF_SCHEMA}.{table_name}"

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {qualified_table_name} (
            {', '.join([f'"{col}" STRING' for col in df_pandas.columns])}
        )
        """
        conn.cursor().execute(create_table_sql)

        success, nchunks, nrows, _ = write_pandas(conn, df_pandas, table_name.upper(), database=SF_DATABASE, schema=SF_SCHEMA)
        print(f" {nrows} linhas enviadas para {qualified_table_name}")

conn.close()
spark.stop()
print("\n Upload concluído!")
