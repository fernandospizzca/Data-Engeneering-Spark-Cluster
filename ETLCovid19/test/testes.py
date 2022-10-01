from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf, SparkFiles
import shutil

spark = SparkSession \
    .builder \
    .appName('Teste') \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.15.6:27017/CovidDB") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.15.6:27017/CovidDB") \
    .getOrCreate()

# df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
#                 .option("uri", "mongodb://192.168.15.6:27017/CovidDB.dados_covid_19_sp")\
#                 .load()
# df = spark.read.csv('/tmp/spark-events/Dados-covid-19-estado.csv', inferSchema=True, header=True, sep=';')

# df.show()
url = 'https://www.seade.gov.br/wp-content/uploads/coronavirus-files/Dados-covid-19-estado.csv' # novo

print(url)
print('Realizando Extratacao COVID')
# spark.sparkContext.addFile(url)
src_path = SparkFiles.get("Dados-covid-19-estado.csv")
dst_path = "/tmp/spark-events/"
shutil.copy(src_path, dst_path)
df = spark.read.csv(dst_path +'/Dados-covid-19-estado.csv', inferSchema=True, header=True, sep=';')

# df = spark.read.csv(url, inferSchema=True, header=True, sep=';')
# df = spark.sparkContext.textFile(spark.SparkFiles.get("Dados-covid-19-estado.csv"))

# df = spark.read.format("csv").option('inferSchema', 'true')\
#                              .option('header', 'true')\
#                              .option('sep', ';')\
#                              .load(SparkFiles.get("Dados-covid-19-estado.csv"))

print('FIM da Extratacao COVID')
df.show()