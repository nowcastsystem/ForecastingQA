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


def TS_SU_save_prediction(name='prediction', prediction = None, client=QASETTING.client, ui_log=None):
    '''
     save forecasting results. A dataframe with there columns. The datetime index, the target y, and the prediction.
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    '''

    database = client.mydatabase
    coll_prediction = database[name]
    err = []

    def __saving_work(coll_prediction, prediction):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving prediction==== {}'.format('123'),
                ui_log
            )

            prediction = json.loads(prediction.to_json(orient='records'))

            print('insert data')
            print('remove old prediction')
            coll_prediction.drop()
            print('save new prediction')
            coll_prediction.insert_many(prediction)
            print('finish insert')

        except Exception as error0:
            print(error0)
            err.append(str(prediction))


    __saving_work(coll_prediction = coll_prediction, prediction = prediction)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save prediction ^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)