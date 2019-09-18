import QUANTAXIS as QA
import pandas as pd
import json
import datetime
from QUANTAXIS.QAUtil import QASETTING
from QUANTAXIS.TSData.TSRawdata import TSRawdata


def TS_fetch_stock_day_adv(code, start, end):
    #get all history data from tdx
    # date = datetime.date.today()
    # data=QA.QAFetch.QATdx.QA_fetch_get_stock_day('00001','2017-01-01','2019-01-31')
    #get data from local database

    data = QA.QA_fetch_stock_day_adv(code=code, start=start, end=end)
    result = data.data
    result = result.sort_index(ascending=True)
    result = result.reset_index(level=1)
    result = result.drop(columns='code')
    result['date'] = result.index
    result = result.rename(columns={'close': 'y'})
    # print(result)
    rawdata = TSRawdata(result)
    # print(rawdata.data)
    return rawdata

#upload to mongodb
# outcome = rawdata.data
#
# client = QASETTING.client
# database = client['mydatabase']
# datacol = database[code+str(datetime.date.today())]
# outcome = date2str(outcome)
# datacol.insert_many(outcome)

def getrawfrommongodb(start,end,databaseid,collectionid,client = QASETTING.client):
    database = client[databaseid]
    datacol = database[collectionid]
    cursor = datacol.find()
    outcome = pd.DataFrame(list(cursor))
    outcome = outcome.drop(columns = '_id')
    outcome['datetime'] = pd.to_datetime(outcome['datetime'])
    outcome.set_index('datetime', inplace=True)
    #inplace=True
    outcome = outcome[start:end]
    outcome['datetime'] = outcome.index
    rawdata = TSRawdata(outcome)
    return rawdata
# rawdatafrommongo = getrawfrommongodb()
# print(rawdatafrommongo.data)