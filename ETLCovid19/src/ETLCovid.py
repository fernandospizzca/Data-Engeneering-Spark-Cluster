from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark import SparkFiles
from datetime import *
import shutil
import urllib
from smb.SMBHandler import SMBHandler
import subprocess
import os

class ETLCovid():

########################################################
    # def __init__(self, spark):
    #     self.__spark = spark
########################################################

    # def runETLCovid(self):
    def runETLCovid(spark):

        #Rodar de segunda a sexta após as 16:00 devido disponibilização dos dados de vacinação.
        #Não havendo dados de vacinação não haverá coleta de dados deste.

        # url = "https://www.seade.gov.br/wp-content/uploads/2021/06/Dados-covid-19-estado.csv" #antigo
        url = 'https://www.seade.gov.br/wp-content/uploads/coronavirus-files/Dados-covid-19-estado.csv' # novo

        # print(url)
        print('Realizando Extratacao COVID')
        spark.sparkContext.addFile(url)

        src_path = SparkFiles.get("Dados-covid-19-estado.csv")
        dst = '/usr/local/spark2/shareTemp/'
        shutil.copy(src_path, dst)
        
        # Copiando arquivo de entrada para o worker
        # subprocess.run(["scp", dst + "Dados-covid-19-estado.csv", f"ferhs@worker2:{dst}"])
        subprocess.run(["scp", dst + "Dados-covid-19-estado.csv", f"ferhs@worker3:{dst}"])

        df = spark.read.csv('/usr/local/spark2/shareTemp/Dados-covid-19-estado.csv', inferSchema=True, header=True, sep=';')
        
        print('FIM da Extratacao COVID')

        print('Realizando Tratamento dos Dados COVID')
        df = df.na.fill(0)
        df = df.withColumn('Data', f.regexp_replace('Data', 'jan', '01'))\
            .withColumn('Data', f.regexp_replace('Data', 'fev', '02'))\
            .withColumn('Data', f.regexp_replace('Data', 'mar', '03'))\
            .withColumn('Data', f.regexp_replace('Data', 'abr', '04'))\
            .withColumn('Data', f.regexp_replace('Data', 'mai', '05'))\
            .withColumn('Data', f.regexp_replace('Data', 'jun', '06'))\
            .withColumn('Data', f.regexp_replace('Data', 'jul', '07'))\
            .withColumn('Data', f.regexp_replace('Data', 'ago', '08'))\
            .withColumn('Data', f.regexp_replace('Data', 'set', '09'))\
            .withColumn('Data', f.regexp_replace('Data', 'out', '10'))\
            .withColumn('Data', f.regexp_replace('Data', 'nov', '11'))\
            .withColumn('Data', f.regexp_replace('Data', 'dez', '12'))

        df = df.withColumn('Total_de_Casos', f.lit(f.col(df.columns[1])))\
            .withColumn('Casos_por_Dia', f.lit(f.col(df.columns[2])))\
            .withColumn('Obitos_por_Dia', f.lit(f.col(df.columns[3])))\
            .drop(df.columns[1])\
            .drop(df.columns[2])\
            .drop(df.columns[3])

        df = df.withColumn('Data', f.from_unixtime(f.unix_timestamp('Data', 'dd/MM/yyyy')))
        df = df.withColumn('Data', f.lit(f.substring(df.Data,1,10)))
        print('FIM do Tratamento dos Dados COVID')

        print('Realizando Gravacao dos Dados COVID')
        #Inicio gravação dos dados da covid via mongodb
        df.write.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://ferhspz:27017/CovidDB.dados_covid_19_sp")\
                .mode("overwrite")\
                .save()
        #Fim gravação dos dados da covid via mongodb
        print('FIM da Gravacao dos Dados COVID')

        os.remove(dst + "Dados-covid-19-estado.csv")

        return df