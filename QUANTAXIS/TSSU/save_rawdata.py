import QUANTAXIS as QA
import pandas as pd
import pymongo
import json
import datetime
from QUANTAXIS.QAUtil import QASETTING
import numpy as np
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_get_next_day,
    QA_util_get_real_date,
    QA_util_log_info,
    QA_util_to_json_from_pandas,
    trade_date_sse
)

from QUANTAXIS.TSUtil.TSDate import TS_util_date2str
from QUANTAXIS.TSFetch.fetchdata import TS_fetch_stock_day_adv



def TS_SU_save_stock_day(code=None,start=None, end=None, client=QASETTING.client, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''

    database = client.mydatabase
    coll_stock_day = database['rawdatatest']
    err = []

    def __saving_work(code,coll_stock_day):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving STOCK_DAY==== {}'.format(str(code)),
                ui_log
            )

            # 首选查找数据库 是否 有 这个代码的数据
            ref = coll_stock_day.find()

            if ref.count() > 0:

                print(code+'has been already contained ')
            # 当前数据库中没有这个代码的股票数据
            else:
                #get raw data
                rawdata =TS_fetch_stock_day_adv(code=code, start=start,end=end)
                print('get raw data')
                # upload to mongodb
                outcome = rawdata.data
                outcome = TS_util_date2str(outcome)
                outcome = json.loads(outcome.to_json(orient='records'))

                print('insert data')
                coll_stock_day.insert_many(outcome)
                print('finish insert')

        except Exception as error0:
            print(error0)
            err.append(str(code))


    __saving_work(code = code, coll_stock_day = coll_stock_day)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock day ^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)