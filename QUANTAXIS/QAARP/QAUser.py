# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2019 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import pandas as pd
import datetime
import uuid
from pymongo import ASCENDING, DESCENDING
from QUANTAXIS.QAARP.QAPortfolio import QA_Portfolio
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info
from QUANTAXIS.QAUtil.QARandom import QA_util_random_with_topic
from QUANTAXIS.QAUtil.QASetting import QA_Setting, DATABASE
from QUANTAXIS.QAUtil.QADate_trade import QA_util_get_next_day, QA_util_get_real_date
from QUANTAXIS.QAUtil.QAParameter import MARKET_TYPE, FREQUENCE
from QUANTAXIS.TSUser.Super_User import Super_User

class QA_User(Super_User):
    pass


if __name__ == '__main__':

    # 测试不对
    user = QA_User(user_cookie='user_admin')
    folio = user.new_portfolio('folio_admin')
    ac1 = user.get_portfolio(folio).new_account('account_admin')

    print(user)
    print(user.get_portfolio(folio))
    print(user.get_portfolio(folio).get_account(ac1))
