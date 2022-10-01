from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql.types import *
from datetime import *
import shutil
import subprocess
import os

class ETLVac():

    def runETLVac(spark):

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

        print('Realizando Extratacao Vacinacao')
        # Trata Url da evolução da vacinação
        urlVac = f"https://www.saopaulo.sp.gov.br/wp-content/uploads/{anoAtual}/{mesAtual}/{dataAtualFmt}_evolucao_aplicacao_doses.csv"

        arqVac = urlVac[58::1]

        print(urlVac)
        print(arqVac)
 
        spark.sparkContext.addFile(urlVac)
        src_path = SparkFiles.get(arqVac)
        dst = '/usr/local/spark2/shareTemp/'
        shutil.copy(src_path, dst)

        #Copiando arquivo de entrada para o worker
        # subprocess.run(["scp", dst + arqVac, f"ferhs@worker2:{dst}"])
        subprocess.run(["scp", dst + arqVac, f"ferhs@worker3:{dst}"])

        # Create an expected schema
        columns = StructType([StructField('Data', StringType(), True),
                              StructField('Dose', StringType(), True),
                              StructField('Qtde', IntegerType(), True)])

        df = spark.read.csv(SparkFiles.get(dst + arqVac), schema=columns, header=True, sep=';')
        
        print('FIM da Extratacao Vacinacao')

        print('Realizando Tratamento dos Dados da Vacinacao')

        dfVac = df.filter(f.col("Qtde").cast("int").isNotNull())
        dfVac = dfVac.na.fill(0)

        dfVac = dfVac.withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de janeiro de ', '/01/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de fevereiro de ', '/02/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de março de ', '/03/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de abril de ', '/04/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de maio de ', '/05/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de junho de ', '/06/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de julho de ', '/07/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de agosto de ', '/08/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de setembro de ', '/09/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de outubro de ', '/10/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de novembro de ', '/11/'))\
                    .withColumn('Data', f.regexp_replace(dfVac.columns[0], ' de dezembro de ', '/12/'))
        
        dfVac = dfVac.withColumn('month', f.split(dfVac.columns[0], '/').getItem(0)) \
                     .withColumn('day', f.split(dfVac.columns[0], '/').getItem(1)) \
                     .withColumn('year', f.split(dfVac.columns[0], '/').getItem(2))\
                     .withColumn('slash', f.lit('/'))

        dfVac = dfVac.withColumn('Tp_Dose', f.lit(f.col(dfVac.columns[1])))\
            .withColumn('Qtd_Doses_Apl_Dia', f.lit(f.col(dfVac.columns[2])))\
            .drop(dfVac.columns[1])\
            .drop(dfVac.columns[2])

        dfVac = dfVac.withColumn('Data', f.concat(f.lpad(dfVac.day, 2, '0'),  dfVac.slash, f.lpad(dfVac.month, 2, '0'), dfVac.slash, dfVac.year))
        
        dfVac = dfVac.withColumn('Data', f.from_unixtime(f.unix_timestamp('Data', 'dd/MM/yyyy')))
        dfVac = dfVac.withColumn('Data', f.lit(f.substring(dfVac.Data,1,10)))
        
        dfVac = dfVac.drop(dfVac.columns[1])\
                     .drop(dfVac.columns[2])\
                     .drop(dfVac.columns[3])\
                     .drop(dfVac.columns[4])
        dfVac.printSchema()
        dfVac.show(10,False)

        print('FIM do Tratamento dos Dados da Vacinacao')

        print('Realizando Gravacao dos Dados da Vacinacao')
        #Inicio gravação dos dados da vacinação contra o covid via mongodb
        dfVac.write.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://ferhspz:27017/CovidDB.dados_vacinacao_covid_19_sp")\
                .mode("overwrite")\
                .save()
        #Fim gravação dos dados da vacinação contra o covid via mongodb
        print('FIM da Gravacao dos Dados Vacinacao')

        os.remove(dst + arqVac)

        return dfVac