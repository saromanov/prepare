import pandas as pd
import random
import os
import json
import logging

class Prepare:
    def __init__(self, data=None):
        self.column_events = {}
        self._data = data

    def addColumnEvent(self, name, event):
        ''' This event provides modification of column after loaded
            name - column name
            event - which gets line and output new representation
        '''
        self.column_events[name] = event

    def _applyColumnEvent(self, name):
        ''' After loading of data, apply modification of some column
            name - name of the column
        '''
        if self._data is None:
            logging.warning("Data is not loaded")
            return
        if name not in self._data:
            logging.warning("Column {0} is not in data".format(name))
            return
        if name not in self.column_events:
            logging.warning("Column {0} is not in registered column events".format(name))
        item = self._data[name].map(self.column_events[name])
        self._data[name] = item


    def read(self, path, fields=[], replace_strings=True, drop_fields=[]):
        ''' path to data
            fields - expected fields for dataset
            replace_strings - replace strings to numeric values
        '''
        data = None
        if os.path.abspath(path).find('.csv') != -1:
            data = pd.read_csv(path)
            self.data = data
            #print(data.take([0,2]))
        elif os.path.abspath(path).find('.json') != -1:
            data = pd.DataFrame(json.loads(open(path, 'r').read()))
        else:
            raise Exception("Format of file {0} is not supported yet".format(path))

        if len(drop_fields) > 0:
            data = self.data.drop(drop_fields, axis=1)

        data.set_axis(1, [item.lower() for item in data.keys()])
        data.sort_index()
        return Prepare(data=data)

    def preprocess(self, replace_na='mean', replace_na_string=' '):
        ''' replace_na - if column contains missing values, replace this fields by some rule.
              Supported:
                mean - replace by mean values of other fields
                random - replace by random value
                em - replace with expectation-maximization algorithm(planned)
                remove - remove all lines if one(or more) columns contains NaN/NA (planned)
                none - doing nothing

            replace_na_string - replacing values in missing string items

        '''
        data = self._data.sort_index()
        for name in data.keys():
            if data[name].dtype == 'float':
                if replace_na == 'mean':
                    data[name] = data[name].fillna(data[name].mean())
                if replace_na == 'random':
                    data[name] = data[name].fillna(random.random())
            if data[name].dtype == 'object':
                if replace_na_string == '':
                    continue
                data[name] = data[name].fillna(replace_na_string)

        return Prepare(data=data)

    def cleanFields(self, except_fields=[]):
        ''' Cleaning string data from fields from commas, dots, and so on
        '''
        return Prepare(data=self.data)


    def toMatrix(self):
        ''' Return numpy matrix representation of data
        '''
        return self._data.as_matrix()

    def toDF(self):
        ''' Return pandas DataFrame representation of data
        '''
        return self._data

    def onlyMaximum(self):
        ''' Return only columns with maxumum amount of data
        '''
        print(self._data.sort_index(axis=1))

