
from QUANTAXIS.QAUtil import (
    QA_util_log_info,
    QA_util_random_with_topic,
    QA_util_to_json_from_pandas
)
from QUANTAXIS.QAUtil.QADate import QA_util_to_datetime
import pandas as pd
from functools import lru_cache

# todo 🛠基类名字 _quotation_base 小写是因为 不直接初始化， 建议改成抽象类


class _quotation_base():

    # 🛠todo  DataFrame 改成 df 变量名字
    def __init__(
            self,
            DataFrame,
            dtype='undefined',
    ):
        '''
        :param data: DataFrame 类型
        :param dtype: 数据

        :param marketdata_type:
        '''

        # 🛠todo 判断DataFame 对象字段的合法性，是否正确
        self.data = DataFrame.drop_duplicates()
    


        # 数据类型 可能的取值

        self.type = dtype
        self.data_id = QA_util_random_with_topic('DATA', lens=3)


        # dtype 参数 指定类 mongo 中 collection 的名字   ，
        # 🛠todo 检查 dtype 字符串是否合法， 放到抽象类中，用子类指定数据库， 后期可以支持mongodb分片集群
        # 🛠todo 子类中没有用到mongodb的数据是通过， QA_data_stock_to_fq  实现数据复权的
        # self.mongo_coll = eval('DATABASE.{}'.format(self.type))
        # self.choose_db()

    # 不能直接实例化这个类

    def __repr__(self):
        return '< ts_Base_DataStruct >' 

    def __call__(self):
        '''
        如果需要暴露 DataFrame 内部数据对象，就用() 来转换出 data （DataFrame）
        Emulating callable objects
        object.__call__(self[, args…])
        Called when the instance is “called” as a function;
        if this method is defined, x(arg1, arg2, ...) is a shorthand for x.__call__(arg1, arg2, ...).
        比如
        obj =  _quotation_base() 调用 __init__
        df = obj()  调用 __call__
        等同 df = obj.__call__()
        :return:  DataFrame类型
        '''
        return self.data

    __str__ = __repr__

    def __len__(self):
        '''
        返回记录的数目
        :return: dataframe 的index 的数量
        '''
        return len(self.index)


    def __iter__(self):
        """
        📌关于 yield 的问题
        A yield statement is semantically equivalent to a yield expression.
        yield 的作用就是把一个函数变成一个 generator，
        带有 yield 的函数不再是一个普通函数，Python 解释器会将其视为一个 generator
        for iterObj in ThisObj
        📌关于__iter__ 的问题
        可以不被 __next__ 使用
        Return an iterator object
        iter the row one by one
        :return:  class 'generator'
        """
        for i in range(len(self.index)):
            yield self.data.iloc[i]

    # 初始化的时候会重新排序
    def __reversed__(self):
        """
        If the __reversed__() method is not provided,
        the reversed() built-in will fall back to using the sequence protocol (__len__() and __getitem__()).
        Objects that support the sequence protocol should only provide __reversed__()
        if they can provide an implementation that is more efficient than the one provided by reversed().
        如果__reversed__() 方法没有提供，
        则调用内建的reversed()方法会退回到使用序列协议（ __len__条目数量 和 获取条目__getitem__ ）方法。
        对象如果支持实现序列协议应该只提供__reversed__方法，如果比上述reversed提供的方式更加有效率 （自己实现一个反向迭代)

        self.new(self.data[::-1])
        :return:
        """
        raise NotImplementedError(
            'ts_DataStruct_* CURRENT CURRENTLY NOT SUPPORT reversed ACTION'
        )

    def __add__(self, DataStruct):
        '''
        ➕合并数据，重复的数据drop
        :param DataStruct: _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        '''
        assert isinstance(DataStruct, _quotation_base)
        assert self.is_same(DataStruct)
        # 🛠todo 继承的子类  QA_DataStruct_XXXX 类型的 判断必须是同一种类型才可以操作
        return self.new(
            data=self.data.append(DataStruct.data).drop_duplicates(),
            dtype=self.type
        )

    __radd__ = __add__

    def __sub__(self, DataStruct):
        '''
        ⛔️不是提取公共数据， 去掉 DataStruct 中指定的数据
        :param DataStruct:  _quotation_base 继承的子类  QA_DataStruct_XXXX
        :return: _quotation_base 继承的子类  QA_DataStruct_XXXX
        '''
        assert isinstance(DataStruct, _quotation_base)
        assert self.is_same(DataStruct)
        # 🛠todo 继承的子类  QA_DataStruct_XXXX 类型的 判断必须是同一种类型才可以操作
        try:
            return self.new(
                data=self.data.drop(DataStruct.index),
                dtype=self.type
            )
        except Exception as e:
            print(e)

    __rsub__ = __sub__

    def __getitem__(self, key):
        '''
        # 🛠todo 进一步研究 DataFrame __getitem__ 的意义。
        DataFrame调用__getitem__调用(key)
        :param key:
        :return:
        '''
        data_to_init = self.data.__getitem__(key)
        if isinstance(data_to_init, pd.DataFrame) == True:
            # 重新构建一个 QA_DataStruct_XXXX，
            return self.new(
                data=data_to_init,
                dtype=self.type
            )
        elif isinstance(data_to_init, pd.Series) == True:
            # 返回 QA_DataStruct_XXXX DataFrame 中的一个 序列Series
            return data_to_init


    '''
    ########################################################################################################
    获取序列
    '''

    def ix(self, key):
        return self.new(
            data=self.data.ix(key),
            dtype=self.type
        )

    def iloc(self, key):
        return self.new(
            data=self.data.iloc(key),
            dtype=self.type

        )

    def loc(self, key):
        return self.new(
            data=self.data.loc(key),
            dtype=self.type

        )

    '''
    ########################################################################################################
    获取序列
    使用 LRU (least recently used) cache 
    '''


    # 交易日期

    @property
    @lru_cache()
    def datetime(self):
        '分钟线结构返回datetime 日线结构返回date'
        if 'date' in self.data.columns:
            return self.data['date']
        else:
            return self.data['datetime']




    @property
    @lru_cache()
    def ndarray(self):
        return self.to_numpy()




    @property
    @lru_cache()
    def index(self):
        '返回结构体的索引'
        return self.data.index


    @property
    @lru_cache()
    def dicts(self):
        '返回dict形式数据'
        return self.to_dict('index')

    @property
    @lru_cache()
    def len(self):
        '返回结构的长度'
        return len(self.data)




    def reset_index(self):
        return self.data.reset_index()





    def get(self, name):

        if name in self.data.__dir__():
            return eval('self.{}'.format(name))
        else:
            raise ValueError('CANNOT GET THIS PROPERTY')

    def query(self, context):
        """
        查询data
        """
        try:
            return self.data.query(context)

        except pd.core.computation.ops.UndefinedVariableError:
            print('CANNOT QUERY THIS {}'.format(context))
            pass

    def groupby(
            self,
            by=None,
            axis=0,
            level=None,
            as_index=True,
            sort=False,
            group_keys=False,
            squeeze=False,
            **kwargs
    ):
        """仿dataframe的groupby写法,但控制了by的code和datetime

        Keyword Arguments:
            by {[type]} -- [description] (default: {None})
            axis {int} -- [description] (default: {0})
            level {[type]} -- [description] (default: {None})
            as_index {bool} -- [description] (default: {True})
            sort {bool} -- [description] (default: {True})
            group_keys {bool} -- [description] (default: {True})
            squeeze {bool} -- [description] (default: {False})
            observed {bool} -- [description] (default: {False})

        Returns:
            [type] -- [description]
        """

        if by == self.index.names[1]:
            by = None
            level = 1
        elif by == self.index.names[0]:
            by = None
            level = 0
        return self.data.groupby(
            by=by,
            axis=axis,
            level=level,
            as_index=as_index,
            sort=sort,
            group_keys=group_keys,
            squeeze=squeeze
        )

    def new(self, data=None, dtype=None):
        """
        创建一个新的DataStruct
        data 默认是self.data
        🛠todo 没有这个？？ inplace 是否是对于原类的修改 ？？
        """
        data = self.data if data is None else data

        dtype = self.type if dtype is None else dtype

        temp = copy(self)
        temp.__init__(data, dtype)
        return temp

    def reverse(self):
        return self.new(self.data[::-1])



    def reindex_time(self, ind):
        if isinstance(ind, pd.DatetimeIndex):
            try:
                return self.new(self.data.loc[(ind, slice(None)), :])
            except:
                raise RuntimeError('DATASTRUCT ERROR: CANNOT REINDEX')

        else:
            raise RuntimeError(
                'DATASTRUCT ERROR: ONLY ACCEPT DATETIME-INDEX FORMAT'
            )

    def iterrows(self):
        return self.data.iterrows()

    def iteritems(self):
        return self.data.iteritems()

    def itertuples(self):
        return self.data.itertuples()

    def abs(self):
        return self.new(self.data.abs())

    def agg(self, func, axis=0, *args, **kwargs):
        return self.new(self.data.agg(func, axis=0, *args, **kwargs))

    def aggregate(self, func, axis=0, *args, **kwargs):
        return self.new(self.data.aggregate(func, axis=0, *args, **kwargs))

    def tail(self, lens):
        """返回最后Lens个值的DataStruct

        Arguments:
            lens {[type]} -- [description]

        Returns:
            [type] -- [description]
        """

        return self.new(self.data.tail(lens))

    def head(self, lens):
        """返回最前lens个值的DataStruct

        Arguments:
            lens {[type]} -- [description]

        Returns:
            [type] -- [description]
        """

        return self.new(self.data.head(lens))

    def show(self):
        """
        打印数据包的内容
        """
        return QA_util_log_info(self.data)

    def to_list(self):
        """
        转换DataStruct为list
        """
        return self.data.reset_index().values.tolist()

    def to_pd(self):
        """
        转换DataStruct为dataframe
        """
        return self.data

    def to_numpy(self):
        """
        转换DataStruct为numpy.ndarray
        """
        return self.data.reset_index().values

    def to_json(self):
        """
        转换DataStruct为json
        """

        data = self.data
        if self.type[-3:] != 'min':
            data = self.data.assign(datetime=self.datetime)
        return QA_util_to_json_from_pandas(data.reset_index())

    def to_string(self):
        return json.dumps(self.to_json())

    def to_bytes(self):
        return bytes(self.to_string(), encoding='utf-8')

    def to_csv(self, *args, **kwargs):
        """datastruct 存本地csv
        """

        self.data.to_csv(*args, **kwargs)

    def to_dict(self, orient='dict'):
        """
        转换DataStruct为dict格式
        """
        return self.data.to_dict(orient)

    def to_hdf(self, place, name):
        'IO --> hdf5'
        self.data.to_hdf(place, name)
        return place, name

    def is_same(self, DataStruct):
        """
        判断是否相同
        """
        if self.type == DataStruct.type:
            return True
        else:
            return False



    # def add_func(self, func, *arg, **kwargs):
    #     return pd.concat(list(map(lambda x: func(
    #         self.data.loc[(slice(None), x), :], *arg, **kwargs), self.code))).sort_index()

    def apply(self, func, *arg, **kwargs):
        """func(DataStruct)

        Arguments:
            func {[type]} -- [description]

        Returns:
            [type] -- [description]
        """

        return func(self, *arg, **kwargs)

    def add_func(self, func, *arg, **kwargs):
        """QADATASTRUCT的指标/函数apply入口

        Arguments:
            func {[type]} -- [description]

        Returns:
            [type] -- [description]
        """

        return self.groupby(level=1, sort=False).apply(func, *arg, **kwargs)

    # def add_func_adv(self, func, *arg, **kwargs):
    #     """QADATASTRUCT的指标/函数apply入口

    #     Arguments:
    #         func {[type]} -- [description]

    #     Returns:
    #         [type] -- [description]
    #     """
    #     return self.data.groupby(by=None, axis=0, level=1, as_index=True, sort=False, group_keys=False, squeeze=False).apply(func, *arg, **kwargs)

    def get_data(self, columns, type='ndarray', with_index=False):
        """获取不同格式的数据

        Arguments:
            columns {[type]} -- [description]

        Keyword Arguments:
            type {str} -- [description] (default: {'ndarray'})
            with_index {bool} -- [description] (default: {False})

        Returns:
            [type] -- [description]
        """

        res = self.select_columns(columns)
        if type == 'ndarray':
            if with_index:
                return res.reset_index().values
            else:
                return res.values
        elif type == 'list':
            if with_index:
                return res.reset_index().values.tolist()
            else:
                return res.values.tolist()
        elif type == 'dataframe':
            if with_index:
                return res.reset_index()
            else:
                return res



   



    def select_time(self, start, end=None):
        """
        选择起始时间
        如果end不填写,默认获取到结尾

        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """

        def _select_time(start, end):
            if end is not None:
                return self.data.loc[(slice(pd.Timestamp(start), pd.Timestamp(end)), slice(None)), :]
            else:
                return self.data.loc[(slice(pd.Timestamp(start), None), slice(None)), :]

        try:
            return self.new(_select_time(start, end), self.type)
        except:
            raise ValueError(
                'CANNOT GET THIS START {}/END{} '.format(start,
                                                            end)
            )

    def select_day(self, day):
        """选取日期(一般用于分钟线)

        Arguments:
            day {[type]} -- [description]

        Raises:
            ValueError -- [description]

        Returns:
            [type] -- [description]
        """

        def _select_day(day):
            return self.data.loc[day, slice(None)]

        try:
            return self.new(_select_day(day), self.type)
        except:
            raise ValueError('CANNOT GET THIS Day {} '.format(day))

    def select_month(self, month):
        """
        选择月份

        @2018/06/03 pandas 的索引问题导致
        https://github.com/pandas-dev/pandas/issues/21299

        因此先用set_index去重做一次index
        影响的有selects,select_time,select_month,get_bar

        @2018/06/04
        当选择的时间越界/股票不存在,raise ValueError

        @2018/06/04 pandas索引问题已经解决
        全部恢复
        """

        def _select_month(month):
            return self.data.loc[month, slice(None)]

        try:
            return self.new(_select_month(month), self.type)
        except:
            raise ValueError('CANNOT GET THIS Month {} '.format(month))



    def select_columns(self, columns):
        if isinstance(columns, list):
            columns = columns
        elif isinstance(columns, str):
            columns = [columns]
        else:
            print('wrong columns')

        try:
            return self.data.loc[:, columns]
        except:
            pass


    def select_time_with_gap(self, time, gap, method):

        if method in ['gt', '>']:

            def gt(data):
                return data.loc[(slice(pd.Timestamp(time), None), slice(None)), :].groupby(level=1, axis=0,
                                                                                           as_index=False, sort=False,
                                                                                           group_keys=False).apply(
                    lambda x: x.iloc[1:gap + 1])

            return self.new(gt(self.data), self.type)

        elif method in ['gte', '>=']:

            def gte(data):
                return data.loc[(slice(pd.Timestamp(time), None), slice(None)), :].groupby(level=1, axis=0,
                                                                                           as_index=False, sort=False,
                                                                                           group_keys=False).apply(
                    lambda x: x.iloc[0:gap])

            return self.new(gte(self.data), self.type)
        elif method in ['lt', '<']:

            def lt(data):
                return data.loc[(slice(None, pd.Timestamp(time)), slice(None)), :].groupby(level=1, axis=0,
                                                                                           as_index=False, sort=False,
                                                                                           group_keys=False).apply(
                    lambda x: x.iloc[-gap - 1:-1])

            return self.new(lt(self.data), self.type)
        elif method in ['lte', '<=']:

            def lte(data):
                return data.loc[(slice(None, pd.Timestamp(time)), slice(None)), :].groupby(level=1, axis=0,
                                                                                           as_index=False, sort=False,
                                                                                           group_keys=False).apply(
                    lambda x: x.tail(gap))

            return self.new(lte(self.data), self.type)
        elif method in ['eq', '==', '=', 'equal', 'e']:

            def eq(data):
                return data.loc[(pd.Timestamp(time), slice(None)), :]

            return self.new(eq(self.data), self.type)
        else:
            raise ValueError(
                'CURRENTLY DONOT HAVE THIS METHODS {}'.format(method)
            )

