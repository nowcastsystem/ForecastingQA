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
from QUANTAXIS.TSUser.Super_User import Super_User
from QUANTAXIS.QAUtil import QASETTING
import pymongo
import datetime

class TS_User(Super_User):
    # pass
    def __init__(self,
            user_cookie=None,
            username='defalut',
            phone='defalut',
            level='l1',
            utype='guests',
            password='default',
            coins=10000,
            wechat_id=None,
            money=0,
            *args,
            **kwargs):
        self.data_list = []
        self.prediction_list = []
        database = QASETTING.client.mydatabase
        self.client = database['userinfo']
        super().__init__(
            user_cookie,
            username,
            phone,
            level,
            utype,
            password,
            coins,
            wechat_id,
            money,
            *args,
            **kwargs)



    def add_data_predict(self):
        dlist = []
        plist = []
        database = QASETTING.client[self.username]
        col_list = database.list_collection_names()

        dlist.append(i for i in col_list if "data" in i)
        plist.append(i for i in col_list if "pred" in i)
        num = 0
        for i in dlist:
            num = num + 1
            self.data_list.append({i:num})
            self.data_list = sorted(set(self.data_list), key=self.data_list.index)
        num = 0
        for i in plist:
            num = num + 1
            self.prediction_list.append({i:num})
            self.prediction_list = sorted(set(self.prediction_list), key=self.prediction_list.index)



    @property
    def message(self):
        return {
            'user_cookie': self.user_cookie,
            'username': self.username,
            'password': self.password,
            'wechat_id': self.wechat_id,
            'phone': self.phone,
            'level': self.level,
            'utype': self.utype,
            'coins': self.coins,
            'coins_history': self.coins_history,
            'money': self.money,
            'subuscribed_strategy': self._subscribed_strategy,
            'subscribed_code': self.subscribed_code,
            'portfolio_list': self.portfolio_list,
            'lastupdatetime': str(datetime.datetime.now()),
            'data_list': self.data_list,
            'prediction_list': self.prediction_list

        }


if __name__ == '__main__':

    # 测试不对
    user = TS_User(user_cookie='user_admin')
    folio = user.new_portfolio('folio_admin')
    ac1 = user.get_portfolio(folio).new_account('account_admin')

    print(user)
    print(user.get_portfolio(folio))
    print(user.get_portfolio(folio).get_account(ac1))
