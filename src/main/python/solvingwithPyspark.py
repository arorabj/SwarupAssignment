import pyspark
from pyspark.sql import SparkSession
import configparser as cp

confPath = '../../../resources/config/application.properties'
fileName="../../../resources/inbound/US_COVID_SHORT_SAMPLE_DataChallenge.csv"
env = "dev"
#get variables from config File
props = cp.RawConfigParser()
props.read(confPath)

executionMode = props.get(env,'executionMode')
mode = props.get(env,'dataWriteMode')
url_connect = props.get(env,'urlConnect')
properties = props.get(env,'connProperties')

table = "Covid_Data_Analysis"

spark = SparkSession.builder.appName("DemoApp").master(executionMode).getOrCreate()
dfRaw = spark.read.format('csv').option('sep',',').option("header", "true").schema('submission_date string, state string, total_cases int, new_case int, total_deaths int, new_death int ').load(fileName)
print ("Count after loading raw data :" + str(dfRaw.count()))

dfRaw.createOrReplaceTempView("rawData")

modifiedDF = spark.sql(""" select 
                                submission_date,
                                state,
                                total_cases,
                                new_case,
                                total_deaths,
                                new_death, 
                                case when new_case > 50 then 'HIGH' when new_case >20 and new_case <=50 then 'MEDIUM' when new_case <=20 then 'LOW' end as covid_case_rate , 
                                case when new_death > 10 then 'HIGH' when new_death >5 and new_death <=10 then 'MEDIUM' when new_death <=5 then 'LOW' end as covid_death_rate 
                            from rawData 
                            where submission_date is not null""")

print ("Count after loading new columns :" + str(dfRaw.count()))

print (modifiedDF.show(1000))
#modifiedDF.write.option('driver', 'org.postgresql.Driver').jdbc(url_connect, table, mode, properties)




