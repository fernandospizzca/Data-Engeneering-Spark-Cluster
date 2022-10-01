from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf, SparkFiles

class JoinBases:

    def runJoinBases(spark, dfCov, dfVac):
        
        print('Realizando Tratamento das Bases COVID_Vacinacao')
        dfjoin = dfCov.alias('cov').select(f.col('cov.Data'), f.col('cov.Casos_por_Dia'), f.col('cov.Obitos_por_Dia'))\
                      .join(dfVac.alias('vac').select(f.col('vac.Data').alias('dataVac'),f.col('vac.Qtd_Doses_Apl_Dia'), f.col('vac.Tp_Dose'))\
                      , f.col('cov.Data') == f.col('dataVac'), "left").na.fill(0).na.fill("")
        
        dfjoin = dfjoin.drop(dfjoin.columns[3])

        dfD = dfVac.groupBy('Data').agg(f.sum('Qtd_Doses_Apl_Dia').alias('Total_de_Doses_Apl_Dia'))

        dfjoin2 = dfjoin.join(dfD.alias('sjoin').select(f.col('sjoin.Data').alias('sjoinData'), f.col('sjoin.Total_de_Doses_Apl_Dia'))\
                                 , dfjoin.Data == f.col('sjoinData'), "left").na.fill(0).na.fill("")
        dfjoin.unpersist()
        dfD.unpersist()
        
        dfjoin2 = dfjoin2.drop(dfjoin2.sjoinData)

        dflst = dfjoin2.groupBy('Data').agg(f.collect_list(f.struct('Tp_Dose', 'Qtd_Doses_Apl_Dia')).alias('Lista_Vac'))

        dfjoin3 = dfjoin2.join(dflst.alias('join3').select(f.col('join3.Data').alias('join3Dt'), f.col('join3.Lista_Vac'))\
                                    , dfjoin2.Data == f.col('join3Dt'), 'left').na.fill(0).na.fill("")

        dfcons = dfjoin3.select('Data', 'Casos_por_Dia', 'Obitos_por_Dia', 'Total_de_Doses_Apl_Dia', 'Lista_Vac').orderBy('Data')
        dfcons = dfcons.dropDuplicates(['Data'])

        dfcons.printSchema()

        print('FIM do Tratamento das Bases COVID_Vacinacao')

        print('Realizando Gravacao da Juncao de Bases COVID_Vacinacao')
        #Inicio gravação dos dados agrupados de covid e vacinação
        dfcons.write.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://ferhspz:27017/CovidDB.dados_consolidados_vac_casos_covid_sp")\
                .mode("overwrite")\
                .save()
                
        dfjoin2.unpersist()
        dflst.unpersist()
        dfjoin3.unpersist()
        dfcons.unpersist()
        #Fim gravação dos dados agrupados de covid e vacinação
        print('FIM da Gravacao da Juncao de Bases COVID_Vacinacao')

        return None