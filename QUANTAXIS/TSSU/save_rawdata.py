import QUANTAXIS as QA
import pandas as pd
import pymongo
import json
import datetime
from QUANTAXIS.QAUtil import QASETTING
from QUANTAXIS.TSData.TSRawdata import TSRawdata


import numpy as np

#get all history data from tdx
# date = datetime.date.today()
# data=QA.QAFetch.QATdx.QA_fetch_get_stock_day('00001','2017-01-01','2019-01-31')

def date2str(data):

    if 'datetime' in data.columns:
        data.datetime = data.datetime.apply(str)
    return json.loads(data.to_json(orient='records'))

#get data from local database
code = '000002'
start = '2007-09-18'
end = '2019-08-18'
data = QA.QA_fetch_stock_day_adv(code=code, start=start, end=end)
result = data.data
result = result.sort_index(ascending=False)
result = result.reset_index(level=1)
result = result.drop(columns='code')
result['date'] = result.index
result = result.rename(columns={'close': 'y'})
# print(result)
rawdata = TSRawdata(result)
# print(rawdata.data)

#upload to mongodb
outcome = rawdata.data

client = QASETTING.client
database = client['mydatabase']
datacol = database[code+str(datetime.date.today())]
outcome = date2str(outcome)
datacol.insert_many(outcome)

def getrawfrommongodb():
    client = QASETTING.client
    database = client['mydatabase']
    datacol = database[code + str(datetime.date.today())]
    cursor = datacol.find()
    outcome = pd.DataFrame(list(cursor))
    outcome = outcome.drop(columns = '_id')
    outcome['datetime'] = pd.to_datetime(outcome['datetime'])
    rawdata = TSRawdata(outcome)
    return rawdata
rawdatafrommongo = getrawfrommongodb()
print(rawdatafrommongo.data)
