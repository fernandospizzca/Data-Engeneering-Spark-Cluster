import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SparkFiles
from datetime import *

class Coleta_rawData():

    def runColetaRaw():

        appName = "Coleta_RawData_Covid_Vac"

        spark = SparkSession \
                .builder \
                .appName(appName) \
                .master('local[*]')\
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0") \
                .config("spark.mongodb.input.uri", "mongodb://192.168.1.6:27017/CovidDB") \
                .config("spark.mongodb.output.uri", "mongodb://192.168.1.6:27017/CovidDB") \
                .getOrCreate()
        spark

        #Rodar de segunda a sexta após as 16:00 devido disponibilização dos dados de vacinação.
        #Não havendo dados de vacinação não haverá coleta de dados deste.

        # url = "https://www.seade.gov.br/wp-content/uploads/2021/06/Dados-covid-19-estado.csv" #antigo
        url = 'https://www.seade.gov.br/wp-content/uploads/coronavirus-files/Dados-covid-19-estado.csv' # novo

        # print(url)
        print('Realizando Extratacao Raw Data COVID')
        spark.sparkContext.addFile(url)

        df = spark.read.csv(SparkFiles.get('Dados-covid-19-estado.csv'), inferSchema=True, header=True, sep=';')
        print('FIM da Extratacao Raw Data COVID')

        print('Realizando Gravacao Raw Data COVID')
        #Inicio gravação dos dados da covid via mongodb
        df.write.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://192.168.1.6:27017/CovidDB.raw_data_covid_sp")\
                .mode("overwrite")\
                .save()
        #Fim gravação dos dados da covid via mongodb
        print('FIM da Gravacao Raw Data COVID')

        #####Dados Vacianação######
        #Rodar de segunda a sexta após as 16:00 devido disponibilização dos dados de vacinação.
        #Não havendo dados de vacinação não haverá coleta de dados deste.

        hora = str(datetime.now().hour) + str(datetime.now().minute)
        dataAtual = date.today()

        if(int(hora) < 1600):
            dataAtual = dataAtual - timedelta(1)
            print(f'Coleta do dia anterior - {dataAtual}')
    
        mesAtual = str(dataAtual.month).rjust(2, '0')
        anoAtual = str(dataAtual.year).rjust(4, '0')
        diaAtual = str(dataAtual.day).rjust(2, '0')
        dataAtualFmt = anoAtual+mesAtual+diaAtual

        print('Realizando Extratacao Raw Data Vacinacao')
        # Trata Url da evolução da vacinação
        urlVac = f"https://www.saopaulo.sp.gov.br/wp-content/uploads/{anoAtual}/{mesAtual}/{dataAtualFmt}_evolucao_aplicacao_doses.csv"

        arqVac = urlVac[58::1]

        print(urlVac)
        print(arqVac)

        spark.sparkContext.addFile(urlVac)

        # Create an expected schema
        columns = StructType([StructField('Data', StringType(), True),
                                StructField('Dose', StringType(), True),
                                StructField('Qtde', IntegerType(), True)])

        dfv = spark.read.csv(SparkFiles.get(arqVac), schema=columns, header=True, sep=';')    
        print('FIM da Extratacao Raw Data Vacinacao')

        print('Realizando Gravacao Raw Data COVID')
        #Inicio gravação dos dados da covid via mongodb
        dfv.write.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://ferhspz:27017/CovidDB.raw_data_vaci_sp")\
                .mode("overwrite")\
                .save()
        #Fim gravação dos dados da covid via mongodb
        print('FIM da Gravacao Raw Data COVID')


        print('Lendo Base Raw Data COVID')
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                        .option("uri", "mongodb://ferhspz:27017/CovidDB.raw_data_covid_sp")\
                        .load()
        print('Leitura Concluida')


        print('Lendo Base Raw Data COVID')
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                        .option("uri", "mongodb://ferhspz:27017/CovidDB.raw_data_vaci_sp")\
                        .load()
        print('Leitura Concluida')