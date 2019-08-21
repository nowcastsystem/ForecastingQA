from ts_datastruct import _quotation_base
import pandas as pd



class TSrawdata(_quotation_base):

    def __init__(self, data, dtype='rawdata'):
        '''
        :param data: a DataFrame，must include column datetime(type = datetime)，column y as target
        :param dtype:  rawdata
        '''
        super().__init__(data, dtype)

        if isinstance(data, pd.DataFrame) == False:
            print("QAError data is not kind of DataFrame type !")

        #validate date
        # when column name is date, change column name to datetime
        if 'date' in data.columns:
            if data['date'].dtype == np.int64:
                data['date'] = data['date'].astype(str)
            data['date'] = pd.to_datetime(data['date'])
            if data['date'].dt.tz is not None:
                raise ValueError(
                    'Column date has timezone specified, which is not supported. '
                    'Remove timezone.'
                )
            if data['date'].isnull().any():
                raise ValueError('Found NaN in column date.')

            data.rename(columns={'date':'datetime'}, inplace = True)
            data = data.sort_values('datetime')

        # when column name is datetime
        elif 'datetime' in data.columns:
            if data['datetime'].dtype == np.int64:
                data['datetime'] = data['datetime'].astype(str)
            data['datetime'] = pd.to_datetime(data['datetime'])
            if data['datetime'].dt.tz is not None:
                raise ValueError(
                    'Column datetime has timezone specified, which is not supported. '
                    'Remove timezone.'
                )
            if data['datetime'].isnull().any():
                raise ValueError('Found NaN in column datetime.')

            data = data.sort_values('datetime')

        else:
            raise ValueError('need column name date or datetime, type = datetime')

        #validate y
        if 'y' in df:
            df['y'] = pd.to_numeric(df['y'])
            if np.isinf(df['y'].values).any():
                raise ValueError('Found infinity in column y.')
        else:
            raise ValueError('need column y as target')


        #The first two columns will be date(0) and y(1)
        df_date = data['datetime']
        data.drop(labels=['datetime'], axis=1, inplace=True)
        df.insert(0, 'datetime', df_date)

        df_y = data['y']
        data.drop(labels=['y'], axis=1, inplace=True)
        df.insert(1, 'y', df_y)