from QUANTAXIS.TSData.ts_basedatastruct import _quotation_base
import pandas as pd
import numpy as np



class TSRawdata2(_quotation_base):

    def __init__(self, data, dtype='rawdata'):
        '''
        :param data: a DataFrame，must include column datetime(type = datetime)，column y as target
        :param dtype:  rawdata
        '''
        super().__init__(data, dtype)

        if isinstance(data, pd.DataFrame) == False:
            print("QAError data is not kind of DataFrame type !")
        
        print("processing raw data")
        #validate date
        data = data.iloc[:, [0, 1]]

        # if len(data.columns) != 2:
        #
        #     raise ValueError(
        #         'Data format not correct!'
        #     )


        # print('date')
        date_column = data.iloc[:, 0]
        # data.columns.values[0] = "date"
        data.columns = ['date', 'y']
        # print(type(data['date']))
        y_column = data.loc[1]
        if date_column.dtype == np.int64:
            data.iloc[:,0] = date_column.astype(str)
        
        print(data)
        data.dropna(axis=0, how="any", inplace=True)
        #data.reset_index(inplace=True)
        #data.fillna(0, inplace=True)
        data['date'] = pd.to_datetime(data['date'])
        print(data)
        print(data.shape)

        # manage duplicated dates
        data.drop_duplicates(subset='date', inplace=True)
        
        if data['date'].dt.tz is not None:
            raise ValueError(
                'Column date has timezone specified, which is not supported. '
                'Remove timezone.'
            )

        # if data['date'].isnull().any():
        #     raise ValueError('Found NaN in column date.')
        print('change column name date to datetime')
        data['date'] = data['date'].dt.date
        data['datetime'] = data['date']
        data = data.drop(columns='date')

        if np.isinf(data['y'].values).any():
            raise ValueError('Found infinity in column y.')


        #The first two columns will be date(0) and y(1)
        df_date = data['datetime']
        data = data.drop(labels=['datetime'], axis=1)
        data.insert(0, 'datetime', df_date)

        df_y = data['y']
        data = data.drop(labels=['y'], axis=1)
        data.insert(1, 'y', df_y)
        self.data = data


