# read_parquet.py
import os
from pyspark.sql import SparkSession

# ======================
# CONFIGURAÇÃO OBRIGATÓRIA (Windows)
# ======================
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] = f"{os.environ['PATH']};{os.environ['HADOOP_HOME']}\\bin"

spark = SparkSession.builder \
    .appName("ReadParquetFiles") \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ======================
# LEITURA DOS DADOS
# ======================
try:
    # Caminho absoluto para evitar erros
    parquet_path = os.path.abspath("data_lake/parquet/brazilian_ecommerce_v4/olist_order_payments_dataset")
    
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Caminho não encontrado: {parquet_path}")

    df = spark.read.parquet(parquet_path)
    
    print("✅ Leitura bem-sucedida!")
    print(f"Total de registros: {df.count()}")
    df.show(5, truncate=False)

except Exception as e:
    print(f"❌ Erro ao ler os dados: {type(e).__name__}")
    print(f"Mensagem: {str(e)}")
    print("\nSoluções possíveis:")
    print("1. Verifique se o caminho está correto")
    print("2. Confira se o Hadoop está em C:\\hadoop")
    print("3. Execute como administrador")

finally:
    spark.stop()