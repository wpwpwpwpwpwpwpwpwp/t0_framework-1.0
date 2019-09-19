'''
简短的测试csv数据读写
'''

import os
import datetime
import numpy as np
import pandas as pd
from cpa.utils import logger
from cpa.io import BaseDataReader, BasePanelReader
from cpa.config import pathSelector
from cpa.config import const
from cpa.utils import bar
from cpa.utils import series



def read_csv_with_filter(path, instrument, startTime=None):
    '''
    读取csv数据并转换为DataFrame,包括open,high,low,close,volume,turnover,date,code
    :param path: 路径
    :param instrument:代码
    :param startTime: 起始时间
    :return: df, 读取csv文件
    '''
    fileName = '{}.csv'.format(instrument)
    filePath = os.path.join(path, fileName)
    df = pd.read_csv(filePath, header=None)
    df.columns = [
        'day',
        'time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'turnover']
    df['date'] = pd.to_datetime(df['day'] + ' ' + df['time'])
    df['code'] = instrument
    if startTime is not None:
        df = df[df['date'] > startTime]
    return df[['code', 'date', 'open', 'high', 'low', 'close', 'volume']]


class CSVSampleDataReader(BaseDataReader):
    '''
    从本地数据库读取股票测试分钟行情数据
    :return: 每运行一次getNextValues则输出一个时间截面所有股票数据df，行为代码，列为开高低收量信息
    '''

    logger = logger.getLogger("csvReader")

    def __init__(self, frequency=bar.Frequency.MINUTE, instruments=None, start=None):
        super().__init__(instruments, fields=const.DataField.OHLCV, start=start)

        self.frequency = frequency
        self.instruments = instruments
        self.dfs = []
        self.isEof = False
        self.valGen = None
        self.path = pathSelector.PathSelector.getDataFilePath(market=const.DataMarket.STOCK, types=const.DataType.SAMPLE, frequency=const.DataFrequency.MINUTE)

    def _iter_(self):
        return self

    def setDir(self, path):
        '''
        :param path: 设置文件读取路径
        :return:
        '''
        self.path = path

    def get_file_list(self):
        '''
        :return:获取路径下的所有csv文件
        '''
        fileLists = os.listdir(self.path)  # 读取文件夹下所有文件名
        print(self.path)
        ret = []
        for file in fileLists:
            if '.csv' in file.lower() and 'index' not in file.lower():
                ret.append(file.split('.')[0])
        return ret

    def loads(self):
        '''
        :return: 从文件路径中读取相应的csv文件
        '''
        fileLists = self.get_file_list()
        if self.instruments is None:
            self.instruments = fileLists
        removeList = list(set(self.instruments) - set(fileLists))  # 求差集
        if len(removeList) > 0:
            self.logger.info(
                "Miss Data stock list: {}".format(removeList))
        self.instruments = list(set(self.instruments) & set(fileLists))  # 求交集
        self.instruments.sort()
        self.logger.info("The stock list: {}".format(self.instruments))
        self.codeFrame = pd.DataFrame({'code': self.instruments})

        for instrument in self.instruments:
            self.logger.info('Loading Data {}'.format(instrument))
            thisData = read_csv_with_filter(self.path, instrument, self.start)
            self.dfs.append(thisData)
        self.dfs = pd.concat(self.dfs, axis=0)
        self.dfs.sort_values(['date', 'code'], ascending=True, inplace=True)
        self.allTime = np.unique(self.dfs['date'])

        self.valGen = self.valueGenerator()

    def valueGenerator(self):
        '''
        :return:从dfs中读取下一个时间的数据,返回时间和一个dataframe：行为code,列为高开低收量，{下一时刻时间:对应股票数据dataframe}
        '''
        for idx, _date in enumerate(self.allTime):
            ret = self.dfs[self.dfs['date'] == _date]
            ret = pd.merge(ret, self.codeFrame, on='code', how='outer')
            ret = ret.set_index('code')
            del ret['date']
            yield pd.to_datetime(_date), ret

            if idx == len(self.allTime) - 2:
                self.isEof = True

    def getDir(self):
        return self.path

    def getFrequency(self):
        '''
        :return:数据必须包含周期
        '''
        return self.frequency

    def getDataShape(self):
        '''
        返回数据长度(总行数，instrument数，field数)
        '''
        return self.allTime.shape[0], self.instruments.__len__(), self.fields.__len__()

    def getDateRange(self):
        '''
        返回数据起止日期，start, end, 实时数据只有start，没有end
        '''
        return self.allTime[0], self.allTime[-1]

    def getRegisteredInstruments(self):
        return self.instruments

    def getNextValues(self):
        return next(self.valGen)

    def getFields(self):
        return self.fields

    def eof(self):
        return self.isEof


class CSVPanelReader(BasePanelReader):
    '''
    csv panel 数据读取接口, 不再主动调用
    '''
    logger = logger.getLogger("CSVPanelReader")

    def __init__(
            self,
            path=None,
            fileName=None,
            frequency=bar.Frequency.MINUTE,
            isInstrmentCol=True,
            start=None,
            end=None):
        '''
        初始化
        param frequency: 数据频率
        param dir: 文件路径
        :param:isInstrumentCol代表是否列名为codes
        param startTime: 所需要获取的数据开始时间
        param endTime: 所需要获取的数据结束时间
        '''
        super().__init__()
        self.path = path
        self.fileName = fileName
        self.isInstrumentCol = isInstrmentCol
        self.frequency = frequency
        self.start = start
        self.end = end
        self.isEof = False
        self.count = 0
        self.staticPanel = None
        self.staticSeries = None
        self.iterator = None
        self.df = None

    def setDir(self, path):
        self.path = path

    def loads(self):
        '''
        获取数据，存入dataframe
        '''
        self.df = pd.read_csv(os.path.join(self.path, self.fileName), index_col=0, parse_dates=[0])
        # 若未输入所需开始时间，则取数据自身的开始时间
        self.start = pd.to_datetime(
            self.start) if self.start else self.df.index[0]
        if self.start < self.df.index[0]:
            self.logger.warning(
                "The input start date {} is before the data's start date {}".format(
                    self.start, self.df.index[0]))
        # 若未输入所需结束时间，则取数据自身的结束时间
        self.end = pd.to_datetime(self.end) if self.end else self.df.index[-1]
        if self.end > self.df.index[-1]:
            self.logger.warning("The input end date {} is after the data's end date {}".format(
                self.end, self.df.index[-1]))
        self.df = self.df.loc[self.start:self.end]
        self.logger.info("{} to {} {} data got.".format(str(self.start)[:10], str(self.end)[:10], self.fileName))
        self.getIterator()

    def getIterator(self):
        '''
        返回一个pandas迭代器
        '''
        # 判断所读取的数据为dataframe还是series，相应地生成迭代器
        self.iterator = self.df.iterrows() if len(
            self.df.shape) == 2 else self.df.iteritems()
        return self.iterator

    def getDir(self):
        '''
        返回文件路径
        '''
        return self.path

    def getFrequency(self):
        '''
        返回数据频率
        '''
        return self.frequency

    def getDataShape(self):
        '''
        返回数据长度
        '''
        return self.df.shape

    def getDateRange(self):
        '''
        返回数据起止日期，start, end, 实时数据只有start，没有end
        '''
        return (self.df.index[0], self.df.index[-1])

    def getRegisteredInstruments(self):
        '''
        返回股票代码
        '''
        if len(self.df.shape) == 2 and self.isInstrumentCol:
            instrumentList = list(self.df)
        else:
            instrumentList = None

        return instrumentList

    def getColumns(self):
        return list(self.df)

    def getNextValues(self):
        '''
        返回datetime, value
        '''
        self.count += 1
        return next(self.iterator)

    def eof(self):
        '''
        判断迭代器是否达到了dataframe最后一行并返回判断变量
        '''
        if self.count == len(self.df.index):
            self.isEof = True
        return self.isEof

    def to_static_panel(self, maxLen=None):
        '''
        直接将全量数据转换为静态的SequenceDataPanel，返回对象
        '''
        if len(self.df.shape) == 2:
            self.staticPanel = series.SequenceDataPanel(
                self.getRegisteredInstruments(), maxLen=maxLen, dtype=np.float32)
            for index, row in self.df.iterrows():
                self.staticPanel.appendWithDateTime(index, row)
            return self.staticPanel
        else:
            self.staticSeries = series.SequenceDataSeries(maxLen=maxLen)
            for index, row in self.df.iteritems():
                self.staticSeries.appendWithDateTime(index, row)
            return self.staticSeries

    def to_frame(self):
        '''
        获取的数据已为dataframe，此处无转换
        '''
        return self.df


class CSVFactorPanelReader(CSVPanelReader):
    logger = logger.getLogger('factorReader')

    def __init__(
            self,
            poolName,
            factorName,
            sectionName,
            fileName=None,
            frequency=bar.Frequency.MINUTE,
            isInstrmentCol=True,
            start=None,
            end=None):
        path = pathSelector.PathSelector.getFactorFilePath(
            poolName, factorName, sectionName)
        super(
            CSVFactorPanelReader,
            self).__init__(
            path,
            fileName,
            frequency,
            isInstrmentCol,
            start,
            end)


class CSVFutureDataReader(BaseDataReader):
    '''
    CSV feed数据读取接口。 这是用于读取期货数据的类。 使用pathSelector 统一路径
    '''
    logger = logger.getLogger('CSVFunterDataReader')

    def __init__(self, instruments, fields, startTime, endTime, frequency=bar.Frequency.MINUTE, limit=None):
        super().__init__(instruments, fields, startTime)
        self.instruments = sorted(instruments) if instruments else None
        self.frequency = frequency
        self.market = bar.Market.FUTURES
        if startTime is None:
            # 若startTime为空，则默认从最开始的数据开始输出
            self.start = pd.to_datetime('20150601')
        else:
            self.start = pd.to_datetime(startTime)  # 输入的起始时间，转换为datetime格式
        if endTime is None:
            self.end = pd.to_datetime('20200101')  # 若endTime为空，则默认输出到最后的数据
        else:
            self.end = pd.to_datetime(endTime)  # 输入的截止时间，转换为datetime格式
        self.limit = limit  # 设置输出的最多的数据条数
        if fields is None:
            self.fields = ['open', 'high', 'low', 'close', 'volume']
        else:
            pass  # 设置要读取的字段，如果没有给定输入，则默认读取开、高、低、收、量
        self.registeredInstruments = 'Apply prepareOutputData function first and then check the registered instruments.'
        self.isEof = False  # 判断是否全部输出完毕的signal
        self.valGen = None  # 储存每一次yield的数据
        self.currentInstruments = []

        self.path = pathSelector.PathSelector.getDataFilePath(
            market=const.DataMarket.FUTURES,
            types=const.DataType.OHLCV,
            frequency=const.DataFrequency.MINUTE,
            fileName=None)

        self.getAllFutureNames()

    def setDir(self, path):
        self.path = path

    def getAllFutureNames(self):
        '''
        查找本地所有期货品种的数据
        :return: 用self.allFutures储存本地所有期货品种，键是品种的简称，值是对应的文件名
                同时把要查询的品种的并且本地没有数据的剔除，初始化registeredInstruments
        '''
        fileLists = os.listdir(self.path)  # 读取文件夹下所有文件名
        self.allFutures = {
            file.split('.')[0]: file for file in fileLists}  # 取符号.之前的string
        registeredInstruments = [j for j in self.instruments if j in self.allFutures.keys()] if \
            self.instruments else sorted(list(self.allFutures.keys()))  # 只保留本地有的品种
        print("RGT:", registeredInstruments)
        self.registeredInstruments = registeredInstruments

    def readCSVFile(self, fileName):
        '''
        读取期货csv文件的函数
        :param path: 存储期货文件的路径
        :param fileName:  相应的文件名
        :return: 返回dataframe，index是datetime格式的时间，介于start和end之间，读取的列是要查询的字段和'symbol'
        '''
        df = pd.read_csv(self.path + r"\\" + fileName)
        df['datetime'] = pd.to_datetime(df['datetime'].str.slice(0, 19))
        df = df.set_index('datetime')
        # 只保留查询起始日期start之后,end之后的那些行
        df = df[(df.index >= self.start) & (df.index <= self.end)]
        df = df[self.fields + ['symbol']]
        df['symbol'] = df['symbol'].apply(lambda x: x.split(".")[0])
        return df

    def prepareOutputData(self):
        '''
        读取加工所需所有数据的函数，并设置好要读取的时间索引
        :return:
        '''
        self.futureDict = {}  # 用来存放每个需要查询的期货的dataframe
        self.firstDict = {}  # 用来存放对应dataframe的index的第一个值
        wholeIndex = []  # 用来存放所有index的并集
        removeList = []
        for future in self.registeredInstruments:  # 遍历
            df = self.readCSVFile(self.allFutures[future])
            if len(df) > 0:
                self.futureDict[future] = df
                tmpIndex = list(df.index)
                self.firstDict[future] = tmpIndex[0]
                wholeIndex = wholeIndex + tmpIndex
                del tmpIndex
                self.logger.info('{} data get'.format(future))
            else:
                # 若某品种的第一个交易时间晚于end，把它移出registeredInstruments
                removeList.append(future)
                print(future, 'data is not available before end time.')
        for j in removeList:
            self.registeredInstruments.remove(j)

        # 初始化self.wholeIndex，是一个列表，储存start和end之间所有的交易时间
        self.wholeIndex = np.unique(wholeIndex)
        del wholeIndex
        if len(self.wholeIndex) == 0:  # 若start和end之间一个品种的数据都没有，把终止信号设为TRUE
            self.isEof = True
            print('No available data within the given period.')
        else:
            if self.limit is not None:  # 如果limit不是None，那么保留wholeIndex的前limit个值
                if len(self.wholeIndex) > self.limit:
                    self.wholeIndex = self.wholeIndex[:self.limit]
            self.actualStart = self.wholeIndex[0]  # 记录真正的起、止时间
            self.actualEnd = self.wholeIndex[-1]

            removeList = []  # 把第一条数据晚于actualEnd的品种剔除registeredInstruments
            for future in self.registeredInstruments:
                if self.firstDict[future] > self.actualEnd:
                    removeList.append(future)
            for j in removeList:
                self.registeredInstruments.remove(j)
                print(
                    future,
                    'data not available within the first %d output.' %
                    self.limit)

    def prepareGenerator(self):
        '''
        初始化生成器
        :return:
        '''
        self.prepareOutputData()  # 准备数据
        self.valGen = self.valueGenerator()  # 初始化生成器

    def valueGenerator(self):
        '''
        数据生成器
        :return:  返回每一个时间的数据查询结果
        '''
        for idx, date in enumerate(self.wholeIndex):
            df = self.adjustData(date)
            print(df)
            yield date, df
            del df
            if idx == len(self.wholeIndex) - 2:  # 如果输出完了全部的数据，把终止信号设为TRUE
                self.isEof = True

    def adjustData(self, date):
        '''
        调整输出的数据格式的函数
        :param date: 要查询的数据对应的时间（datetime格式）
        :return:
        '''
        combined = pd.DataFrame()
        toAddList = []
        for future in self.registeredInstruments:  # 遍历要查询的品种
            tmp = self.futureDict[future].copy()
            tmp = tmp[tmp.index == date].reset_index()  # 取对应时间的数据
            if len(tmp) > 0:
                tmp = pd.DataFrame(tmp.iloc[0]).T  # 有些品种的数据中有些交易日的11:30:00会有两条记录！此时只取第一条
            else:
                tmp = pd.DataFrame(index=[0], columns=tmp.columns)  # 有些品种在一些时间会有数据缺失，此时用nan填充
                # tmp.loc[0,'symbol']=self.allFutures[future][:-4]
                tmp.loc[0, 'symbol'] = future
            combined = pd.concat([combined, tmp])
            del tmp
        combined = combined.drop(['datetime'], axis=1).set_index('symbol')  # 删去'datetime'列，把'symbol'设置为索引，并转置

        tmp = combined.dropna(axis=0, how='any')
        self.currentInstruments = list(tmp.index)
        del tmp

        return combined

    def getDir(self):
        '''
        返回期货的本地数据路径
        :return:
        '''
        return self.path

    def getFrequency(self):
        '''
        返回数据的频率
        :return:
        '''
        return self.frequency

    def getDataShape(self):
        '''
        return: 返回数据的维度（字典）：{'timeLength','stockNumber','fieldNumber'}
                即{本次查询的数据总长度、期货数量、字段数量}
        '''
        shape = {'timeLength': len(self.wholeIndex), 'futureNumber': len(self.registeredInstruments),
                 'fieldNumber': len(self.fields)}
        return shape

    def getDateRange(self):
        '''
        :return: 返回[数据起始时间，数据截止时间]
        '''
        if len(self.wholeIndex) == 0:
            return 'No data get in the given period.'
        else:
            return self.wholeIndex[0], self.wholeIndex[-1]

    def getRegisteredInstruments(self):
        '''
        :return: 返回实际使用的instruments
        '''
        return self.registeredInstruments

    def getCurrentInstruments(self):
        '''
        :return: 返回当前时刻，不是nan的期货品种列表
        '''
        return self.currentInstruments

    def getNextValues(self):
        '''
        :return: 获取生成器返回的下一个值
        '''
        return next(self.valGen)

    def eof(self):
        '''
        :return: 如果触发停止条件：返回True， 否则返回False
                触发条件：输出完要查询的全部的数据；或者输出的数量达到上限
        '''
        return self.isEof

    def getFields(self):
        '''
        返回要查询的字段列表
        :return:
        '''
        return self.fields


class CSVFinanceReader(BaseDataReader):
    '''
    读取财务数据三大表
    '''
    logger = logger.getLogger('CSVFinanceReader')

    class FinanceType:
        '''
        财务数据三大表文件名
        '''
        BALANCESHEET = 'ASHAREBALANCESHEET.csv'
        INCOME = 'ASHAREINCOME.csv'
        CASHFLOW = 'ASHARECASHFLOW.csv'

    def __init__(self, fileName, instruments, fields, start=None, end=None):
        '''
        初始化
        param fileName: 文件名
        param instruments: 所选股票code
        param fields: 所选字段
        param start: 起始时间
        param end: 结束时间
        '''
        super().__init__(instruments, fields, start)
        # 同时满足两种输入方法，e.g. "BALANCESHEET" or "ASHAREBALANCESHEET.csv"
        fileName = fileName if "." in fileName else getattr(self.FinanceType, fileName.upper())
        self.filePath = pathSelector.PathSelector.getDataFilePath(market=const.DataMarket.STOCK,
                                                                  types=const.DataType.FINANCE,
                                                                  frequency=const.DataFrequency.QUARTER,
                                                                  fileName=fileName)
        self.end = end
        self.isEof = False
        self.valGen = None

    def loads(self):
        '''
        读取csv数据并转换为DataFrame，并做必要的数据处理
        return: dataframe
        '''
        self.df = pd.read_csv(self.filePath)
        self.df = self.df[pd.notnull(self.df["ANN_DT"])]  # 删除公告日期为NAN的行
        self.df["ANN_DT"] = pd.to_datetime(self.df["ANN_DT"], format="%Y%m%d")
        self.df["ANN_DT"] = self.df["ANN_DT"].dt.date

        if self.start:  # 取开始时间之后的数据
            self.df = self.df[self.df["ANN_DT"] > pd.Timestamp(self.start).date()]
        if self.end:  # 取结束时间之前的数据
            self.df = self.df[self.df["ANN_DT"] < pd.Timestamp(self.end).date()]
        self.df[["WIND_CODE", "temp"]] = self.df["WIND_CODE"].str.split(".", expand=True)  # 删除股票代码后缀，保留数字
        self.df.drop(columns="temp", inplace=True)
        self.availInstruments = sorted(set(self.df["WIND_CODE"]) & set(self.instruments))  # 取股票代码交集，并排序
        self.df = self.df[self.df["WIND_CODE"].isin(self.availInstruments)]  # 删除不在股票代码集合中的行

        # 当出现同一公告日同一股票多条数据的情况时，将公告日递延一天
        self.df.sort_values(by=["WIND_CODE", "ANN_DT"], inplace=True)
        self.df.reset_index(inplace=True)
        boolDF = self.df[["WIND_CODE", "ANN_DT"]].eq(self.df[["WIND_CODE", "ANN_DT"]].shift(1)).all(axis="columns")
        duplicatedRowIndex = boolDF.index[boolDF == True].tolist()
        for idx in duplicatedRowIndex:
            datePlusOne = self.df.iloc[idx - 1]["ANN_DT"] + datetime.timedelta(days=1)
            self.df.loc[idx, "ANN_DT"] = datePlusOne
            if idx <= len(self.df) - 2:
                if self.df.loc[idx]["ANN_DT"] == self.df.loc[idx + 1]["ANN_DT"]:
                    self.df.loc[idx + 1, "ANN_DT"] = self.df.iloc[idx]["ANN_DT"] + datetime.timedelta(days=1)

        self.df.set_index(["ANN_DT", "WIND_CODE"], inplace=True)  # 设置双index，并排序
        self.df.sort_index(axis=0, inplace=True)
        self.df = self.df[self.fields]  # 取所需字段的数据，删除其他列

        self.allDate = [date for date in np.unique(self.df.index.get_level_values(level="ANN_DT"))]  # 生成一个含所有日期的列表
        self.valGen = self.valueGenerator()  # 生成generator并赋值

        return self.df

    def valueGenerator(self):
        '''
        生成器
        return: 从df中读取下一个时间的数据，返回时间和一个dataframe，行是股票代码，列是所选字段
        '''
        for idx, date in enumerate(self.allDate):
            tempDF = self.df.loc[date]
            # tempDF = tempDF[~tempDF.index.duplicated(keep="first")]  # 使用递延处理，这里注释掉只保留一行的处理
            # 生成一个列表，列表包含存在于股票交集而不在tempDF中的股票
            emptyInsList = [ins for ins in self.availInstruments if ins not in tempDF.index.values]
            emptyDF = pd.DataFrame(columns=self.fields, index=emptyInsList)  # 生成一个只包含行名和列名的空dataframe
            tempDF = tempDF.append(emptyDF)
            tempDF.sort_index(inplace=True)
            yield date, tempDF

            if idx == len(self.allDate) - 2:
                self.isEof = True

    def getDir(self):
        '''
        返回文件路径
        return: 文件路径
        '''
        return self.filePath

    def getFrequency(self):
        '''
        返回数据周期
        return: 数据周期，以秒计
        '''
        return bar.Frequency.QUARTER

    def getFields(self):
        '''
        返回所选字段
        return: 所选字段list
        '''
        return self.fields

    def getDataShape(self):
        '''
        返回未填充空值的dataframe数据长度
        return: 包含数据长度的tuple
        '''
        return self.df.shape

    def getDateRange(self):
        '''
        返回经过筛选后的数据起止日期
        return: 包含数据起止日期的tuple
        '''
        return (self.allDate[0], self.allDate[-1])

    def getRegisteredInstruments(self):
        '''
        返回用户查询的股票代码与财务数据中存在的股票代码的交集
        return: 包含交集股票代码的list
        '''
        return self.availInstruments

    def getNextValues(self):
        '''
        返回生成器返回的下一个值
        return: datetime, value
        '''
        return next(self.valGen)

    def eof(self):
        '''
        返回end of file判断变量
        return: True or False
        '''
        return self.isEof


if __name__ == '__main__':
    '''CSVSampleDataReader'''
    # instruments = None  # 要查询的股票代码
    # start = None  # 起始时间
    # path = r'D:\PythonProject\PycharmProjects\t0_frameWork\cpa\data'  # 数据路径
    #
    # csvsamplereader = CSVSampleDataReader(instruments, start)  # 生成一个类
    # csvsamplereader.setDir(path)  # 设置路径
    # csvsamplereader.loads()  # 读取数据
    # csvsamplereader.run(30, _print=True)  # 逐条输出

    '''CSVFutureDataReader'''
    # startTime = '2018/4/10 1128'  # 示例起始日期，字符串格式只要能被pandas识别并转换为datetime即可
    # endTime = None  # 查询的截止日期
    # instruments = ['IC', 'IH', 'IF']  # 要查询的期货代码列表
    # limit = 2000  # 输出查询数据的条数上限
    # # 要查询的字段名称列表，如果不输入该参数的话，自动查询高开低收量5个
    # fields = ['close', 'open', 'high', 'low', 'volume']
    #
    # csvfuturereader = CSVFutureDataReader(instruments, const.DataField.OHLCV, startTime, endTime)
    # csvfuturereader.prepareGenerator()
    # print(csvfuturereader.getDataShape())
    # print(csvfuturereader.getDateRange())
    # csvfuturereader.run(stopCount=100, _print=True)

    '''CSVPanelReader'''
    # path = r'D:\PythonProject\PycharmProjects\t0_frameWork\cpa\data'
    # fileName = 'SH000001.CSV'
    # indexReader = CSVPanelReader(path=path, fileName=fileName, frequency=bar.Frequency.MINUTE, isInstrmentCol=False, start='20050101')
    # indexReader.loads()

    '''CSVFutureDataReader + baseFeed'''
    # from cpa.feed import baseFeed
    #
    # startTime = '2019/06/23 1400'  # 示例起始日期，字符串格式只要能被pandas识别并转换为datetime即可
    # endTime = '2019/06/27 2300'  # 查询的截止日期
    # # instruments = ['A', 'JD', 'Y', 'P', 'L', 'V', 'PP', 'I', 'J', 'JM']  # 要查询的期货代码列表
    # instruments = None
    # fields = ['close', 'open', 'high', 'low', 'volume']  # 要查询的字段名称列表，如果不输入该参数的话，自动查询高开低收量5个
    #
    # csvfuturereader = CSVFutureDataReader(instruments, fields, startTime, endTime)
    # csvfuturereader.prepareGenerator()
    # panelFeed = baseFeed.PanelFeed(dataSource=csvfuturereader, instruments=csvfuturereader.getRegisteredInstruments())
    # panelFeed.run(20)

    '''csvFinanceReader'''
    from cpa.feed import baseFeed
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    startDate = "20161117"
    endDate = "20190818"
    fileName = "balancesheet"
    instruments = ["600519", "601318", "600498"]
    fields = ["REPORT_PERIOD", "MONETARY_CAP"]

    csvFinanceReader = CSVFinanceReader(fileName=fileName,
                                  instruments=instruments,
                                  fields=fields,
                                  start=startDate,
                                  end=None)
    df = csvFinanceReader.loads()
    print(df)

    print("dir:", csvFinanceReader.getDir())
    print("freq:", csvFinanceReader.getFrequency())
    print("fileds:", csvFinanceReader.getFields())
    print("shape:", csvFinanceReader.getDataShape())
    print("range:", csvFinanceReader.getDateRange())
    print("ins:", csvFinanceReader.getRegisteredInstruments())
    panelFeed = baseFeed.PanelFeed(dataSource=csvFinanceReader, instruments=csvFinanceReader.getRegisteredInstruments())
    panelFeed.run()
    print(panelFeed.extraPanel["MONETARY_CAP"].to_frame())
    print(panelFeed.extraPanel["REPORT_PERIOD"].to_frame())
