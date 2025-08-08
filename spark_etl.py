# spark_etl.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ======================
# CONFIGURAÇÃO DO SPARK
# ======================
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] = f"{os.environ['PATH']};{os.environ['HADOOP_HOME']}\\bin"

spark = SparkSession.builder \
    .appName("BrazilianEcommerceETL") \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ======================
# DIRETÓRIOS
# ======================
INPUT_DIR = "data_lake/raw/brazilian_ecommerce"
OUTPUT_DIR = "data_lake/parquet/brazilian_ecommerce_v4"

# ======================
# FUNÇÃO DE LOG
# ======================
def log(message):
    print(f"\n[LOG] {message}")
    print("-" * 60)

# ======================
# PROCESSAMENTO PRINCIPAL
# ======================
try:
    # Verifica se o diretório de entrada existe
    if not os.path.exists(INPUT_DIR):
        raise FileNotFoundError(f"Diretório de entrada não encontrado: {INPUT_DIR}")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    log("Iniciando conversão CSV -> Parquet")
    
    for file_name in os.listdir(INPUT_DIR):
        if file_name.endswith('.csv'):
            table_name = file_name.replace('.csv', '')
            input_path = os.path.join(INPUT_DIR, file_name)
            output_path = os.path.join(OUTPUT_DIR, table_name)
            
            log(f"Processando: {file_name}")
            
            # Leitura com tratamento de esquema
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .csv(input_path)
            
            # Conversão segura para Parquet
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(output_path)
            
            # Verificação
            parquet_df = spark.read.parquet(output_path)
            log(f"✅ {file_name} convertido\n"
                f"Registros: {parquet_df.count():,}\n"
                f"Esquema: {parquet_df.schema}")
    
    log("Processo concluído com sucesso!")
    print(f"Arquivos Parquet salvos em: {os.path.abspath(OUTPUT_DIR)}")

except Exception as e:
    log(f"❌ Erro durante o processamento")
    print(f"Tipo do erro: {type(e).__name__}")
    print(f"Mensagem: {str(e)}")
    print("\nDica: Verifique se:\n"
          "1. O Hadoop está em C:\\hadoop\\bin\n"
          "2. O Python está na variável PATH\n"
          "3. Os arquivos CSV estão em UTF-8")

finally:
    spark.stop()
    print("\nSessão Spark finalizada.")