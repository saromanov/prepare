import pandas as pd
import numpy as np
import random
import os
import json
import logging
import re
from sklearn import preprocessing

class Prepare:
    def __init__(self, data=None):
        self._data = data

    def _checkCol(self, name):
        ''' Return True is column name is exist in data frame else False in otherwise
        '''
        if self._data is None:
            logging.warning("Data is not loaded")
            return False
        if self._data.get(name) is None:
            logging.warning("Column {0} is not in data".format(name))
            return False

        return True

    def applyColumnEvent(self, name, func):
        ''' After loading of data, apply modification of some column
            name - name of the column
        '''
        if self._checkCol(name) is True:
            item = self._data[name].map(func)
            self._data[name] = item
        return Prepare(data=self._data)

    def addColumn(self, title, data):
        ''' addColumn to existing dataset
            Args:
                title - name of the column
                data - must be in the same length
        '''
        self._data[title] = pd.Series(data, index=self._data.index)
        return Prepare(data=self._data)

    def addRowEvent(self, index, func, col=None):
        ''' Apply event to the row. If col is None, apply event to all columns
            index - row index
            func - apply to value from index. Also, it can be used as value
        '''
        if type(index) is not int:
            logging.error("addRowEvent. index value must be as int type")
            return Prepare(data=self._data)

        if col is not None:
            self._data.set_value(index, col, func(self._data.get_value(index, col=col)))
            return Prepare(data=self._data)

        for col in self._data.keys():
            self._data.set_value(index, col, func(self._data.get_value(index, col=col)))

        return Prepare(data=self._data)

    def strToNum(self, colname, values):
        '''
            Replace string values to numeric
            colname - column name
            values - must be in the dict in the format {oldvalue: newvalue}
        '''
        if self._checkCol(colname) is True:
            self._data = self._data.replace({colname:values})
        return Prepare(data=self._data)

    def strToNumAll(self, except_cols=[]):
        '''
           Replace all string values to numeric from all columns
        '''

        for col in self._data.keys():
            if self._data[col].dtype != 'object' or col in except_cols:
                continue
            try:
                values = np.unique(self._data[col])
            except:
                raise Exception("Values in the column must be in the same type")
            nums = range(1, len(values)+1)
            self._data = self._data.replace({first: second for(first, second) in zip(values, nums)})

        return Prepare(data=self._data)

    def read(self, path, fields=[], replace_strings=True, drop_fields=[]):
        ''' path to data
            fields - expected fields for dataset
            replace_strings - replace strings to numeric values
        '''
        self._data = self._readInner(path, fields=fields, replace_strings=replace_strings, drop_fields=drop_fields)
        return Prepare(data=self._data)

    def _readInner(self, path, fields=[], replace_strings=True, drop_fields=[]):
        result = None
        if os.path.abspath(path).find('.csv') != -1:
            result = pd.read_csv(path)
        elif os.path.abspath(path).find('.xlsx') != -1:
            result = pd.read_excel(path)
        elif os.path.abspath(path).find('.json') != -1:
            result = pd.DataFrame(json.loads(open(path, 'r').read()))
        else:
            raise Exception("Format of file {0} is not supported yet".format(path))

        if result is None:
            raise Exception("Something went wrong with reading of data")

        if len(drop_fields) > 0:
            result = self._data.drop(drop_fields, axis=1)

        result.set_axis(1, [item.lower().replace(' ','') for item in result.keys()])
        result = result.sort_index(axis=1)
        return result


    def add_data(self, data):
        ''' Instead call 'read', set already loaded data
        '''
        return Prepare(data=self._data)

    def removeDuplicates(self):
        ''' Removing duplicate rows from dataset
        '''
        idxs = self._data.index[self._data.duplicated()]
        if len(idxs) > 0:
            self._data = self._data.drop(idxs)
        return Prepare(data=self._data)

    def preprocess(self, replace_na='mean', replace_na_string=' ', scale=True, norm=True):
        ''' replace_na - if column contains missing values, replace this fields by some rule.
              Supported:
                mean - replace by mean values of other fields
                random - replace by random value
                em - replace with expectation-maximization algorithm(planned)
                freq - Replace missing values by max frequent values
                remove - remove all lines if one(or more) columns contains NaN/NA (planned)
                none - doing nothing

            replace_na_string - replacing values in missing string items
            scale - scaling datasets
            norm - normalization datasets

        '''
        data = self._data
        for name in data.keys():
            if data[name].dtype == 'float64' or data[name].dtype == 'int64':
                if replace_na == 'mean':
                    data[name] = data[name].fillna(data[name].mean())
                if replace_na == 'random':
                    data[name] = data[name].fillna(random.random())
                if replace_na == 'predict':
                    values = data[name][data[name].notnull()].as_matrix()
                    topred = data[name][data[name].isnull()].as_matrix()
            if replace_na == 'freq':
                data[name] = data[name].fillna(data[name].value_counts().idxmax())
                maxvalue = data[name].value_counts()
                data[name] = data[name].map(lambda x: maxvalue if x == '' else x)

            if data[name].dtype == 'object':
                if replace_na_string == '':
                    continue
                data[name] = data[name].fillna(replace_na_string)

        if norm: data = self._norm(data)
        return Prepare(data=data)

    def _norm(self, df):
        ''' Data normalization
        '''
        df = [(df[key] - df[key].mean())/(df[key].max() - df[key].min()) for key in df.keys() if df[key].dtype == 'float']
        return df

    def _norm2(self, df, name):
        ''' Data normalization. Second case
        '''
        value = df[name].as_matrix()
        normalized = preprocessing.normalize(value, norm='l2')
        df[name] = pd.DataFrame(normalized)
        return df

    def _scale(self, df, name):
        value = df[name].as_matrix()
        scale = preprocessing.scale(value)
        df[name] = scale
        return df[name]

    def sample(self, n):
        ''' Sample n values from data frame
        '''
        values = self._data.index.values
        if n >= len(values):
            logging.error("Number of samples must be less then number of values")
            return

        return Prepare(data=self._data.ix[sorted(np.random.choice(values, n, replace=False))])

    def cleanFields(self, except_fields=[]):
        ''' Cleaning string data from columns from commas, dots, and so on
        '''
        func = lambda x: re.sub(r'[^\w]', ' ', x).replace(' ','')
        for col in self._data.keys():
            if self._data[col].dtype !='object':
                continue
            item = self._data[col].map(func)
            self._data[col] = item
        return Prepare(data=self._data)

    def mergeColumn(self, data=None, path=None):
        '''
           Merging columns from file
           Args:
               path - path to file
               data - column for merging
        '''

        if data is None and path is None:
            raise Exception("Data for merging is not found")
        if data is not None:
            self._data = pd.concat([self._data, data], axis=1)
        if path is not None:
            coldata = self._readInner(path)
            self._data = pd.concat([self._data, coldata], axis=1)

        return Prepare(data=self._data)



    def toMatrix(self):
        ''' Return numpy matrix representation of data
        '''
        return self._data.as_matrix()

    def toDF(self):
        ''' Return pandas DataFrame representation of data
        '''
        return self._data

    def onlyMaximum(self):
        ''' Return only columns with maximum amount of data
        '''
        return self._data.sort_index(axis=1)

