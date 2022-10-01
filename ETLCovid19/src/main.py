from cmath import e
from tkinter import E
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf, SparkFiles
import sys
# from Coleta import Coleta_rawData
from ETLCovid import ETLCovid
from ETLVac import ETLVac
from joinBases import JoinBases

appName = "ETL_covid_vac_TCC"

spark = SparkSession \
    .builder \
    .appName(appName) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0") \
    .config("spark.mongodb.input.uri", "mongodb://ferhspz:27017/CovidDB") \
    .config("spark.mongodb.output.uri", "mongodb://ferhspz:27017/CovidDB") \
    .getOrCreate()

try:
    
    # Coleta_rawData.runColetaRaw()
    dfCov = ETLCovid.runETLCovid(spark)
    dfVac = ETLVac.runETLVac(spark)
    JoinBases.runJoinBases(spark, dfCov, dfVac)

# código a tentar
except Exception as e:
    print("ERROR SCRIPT RUN: {0}".format(str(e)))
    sys.exit(-1)
    # código a executar no caso da exceção
finally:
    # código que é executado sempre, independente de haver uma exceção em andamento ou não
    spark.stop()
