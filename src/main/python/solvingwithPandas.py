import pandas as pd
import numpy as np
import configparser as cp

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.max_colwidth', None)

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

dfRaw =pd.read_csv(fileName)

print ("Count after loading raw data :" + str(dfRaw.submission_date.count()))

dfModified = dfRaw.copy()
dfModified['total_cases'] = dfModified['total_cases'].str.replace(',','').astype('int')
dfModified['new_case'] = dfModified['new_case'].str.replace(',','').astype('int')
dfModified['total_deaths'] = dfModified['total_deaths'].str.replace(',','').astype('int')
dfModified['new_death'] = dfModified['new_death'].str.replace(',','').astype('int')

conditions_covid_case_rate = [
    (dfModified['new_case'] > 50),
    (dfModified['new_case'] > 20) & (dfModified['new_case'] <= 50),
    (dfModified['new_case'] <= 20)
    ]
conditions_covid_death_rate = [
    (dfModified['total_deaths'] > 10),
    (dfModified['total_deaths'] > 5) & (dfModified['total_deaths'] <= 10),
    (dfModified['total_deaths'] <= 5)
    ]
conditions_values = ['HIGH','MEDIUM','LOW']


dfModified['covid_case_rate'] = np.select(conditions_covid_case_rate, conditions_values)
dfModified['covid_death_rate'] = np.select(conditions_covid_death_rate, conditions_values)

dfModified.to_sql(table)

