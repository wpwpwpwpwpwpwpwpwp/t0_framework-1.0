# -*- coding: utf-8 -*-
'''
@Time    : 2019/7/28 8:50
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
import abc
import pandas as pd

class IndexInstruments:
    '''
    指数成分股列表
    '''
    @classmethod
    @property
    def HS300(cls):
        raise NotImplementedError

    @classmethod
    @property
    def SZ50(cls):
        raise NotImplementedError

    @classmethod
    @property
    def ZZ500(cls):
        raise NotImplementedError


class BaseReader(abc.ABC):

    @abc.abstractmethod
    def getDir(self):
        raise NotImplementedError

    @abc.abstractmethod
    def getFrequency(self):
        '''
        :return:数据必须包含周期
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def getDataShape(self):
        '''
        返回数据长度(总行数，instrument数，field数)
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def getDateRange(self):
        '''
        返回数据起止日期，start, end, 实时数据只有start，没有end
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def getRegisteredInstruments(self):
        raise NotImplementedError

    def loads(self, *args, **kwargs):
        '''
        :param args: (可选)从文件路径中一次性加载所有数据, 可以作为子函数供iter使用
        :param kwargs:
        :return:返回读取到的数据
        '''
        pass

    @abc.abstractmethod
    def getNextValues(self):
        '''
        :return:datetime, value
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def eof(self):
        raise NotImplementedError

    def run(self, stopCount=None, _print=False):
        '''
        :param stopCount: 方便测试的时候随时截断数据
        :param _print: 打印数据
        :return:
        '''
        counter = 0
        while not self.eof():
            counter += 1
            dateTime, data = self.getNextValues()
            if _print:
                print(dateTime)
            if stopCount is not None and counter > stopCount:
                break

class BaseDataReader(BaseReader):
    '''
    基础数据读取虚类
    '''

    def __init__(self, instruments, fields, start):
        '''
        按照指定instruemnts、fields, 起始时间进行读取
        fields 代表读取的数据列，OHLC等
        '''
        super().__init__()
        self.instruments = instruments
        self.fields = fields
        self.start = start

    @abc.abstractmethod
    def getFields(self):
        '''
        读取数据的列类型
        '''
        raise NotImplementedError


class BasePanelReader(BaseReader):
    '''
    存储的panel数据读取虚类, isInstrumentCol表示列名是否是codes，有可能是OHLC等数据，不同数据在加载后操作不一样
    '''

    def __init__(self, isInstrumentCol=True):
        self.isInstrumentCol = isInstrumentCol
        self.df = None #加载好的数据表格

    def from_dataframe(self, df):
        '''
        :param df: 从dataframe中直接加在过滤好的数据
        :return:
        '''
        self.df = df
        return self

    @abc.abstractmethod
    def to_static_panel(self):
        '''
        :return:直接将全量数据转换为静态的SequenceDataPanel
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def to_frame(self):
        '''
        :return:直接转换为静态的pandas DataFrame
        '''
        raise NotImplementedError


class BaseWriter(abc.ABC):

    @abc.abstractmethod
    def getDir(self):
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, filePath, mode='a'):
        raise NotImplementedError