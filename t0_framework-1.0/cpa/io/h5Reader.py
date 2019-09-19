# -*- coding: utf-8 -*-
'''
@Time    : 2019/7/28 8:50
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''

import os
import pandas as pd
import numpy as np

from cpa.io import BaseDataReader, BasePanelReader
from cpa.config import pathSelector, const
from cpa.utils import bar, series
from cpa.utils import logger
from cpa.config.pathSelector import platformSectionSelector


class H5DataReader(BaseDataReader):
    '''
    h5 feed数据读取接口, 使用pathSelector 统一路径
    '''

    logger = logger.getLogger("H5DataReader")

    def __init__(
            self,
            frequency=bar.Frequency.MINUTE,
            instruments=None,
            fields=None,
            start=None,
            end=None,
            limit=-1):
        '''
        :param frequency: 数据周期
        :param instruments: 所选股票code
        :param fields: 所选字段,高开低收等
        :param startTime: 起始时间
        '''
        super().__init__(instruments, fields, start)
        self.frequency = frequency
        self.market = bar.Market.STOCK
        if start is None:
            # 若start等于None，则默认start取本地最初的数据
            self.start = pd.to_datetime('20150601 0930')
        else:
            self.start = pd.to_datetime(start)  # 输入的起始时间，转换为datetime格式
        if end is None:
            # 若end等于None，则默认end取本地最后的数据
            self.end = pd.to_datetime('20190430 1500')
        else:
            self.end = pd.to_datetime(end)  # 输入的终止时间，转换为datetime格式
        self.registeredInstruments = 'Apply prepareOutputData function first and then check the registered instruments.'
        self.appliedTimeLine = list(pd.date_range(  # 根据输入的start和end，生成日期序列，周期为月
            max(self.start.strftime('%Y%m'), '201506') + '01', min(self.end.strftime('%Y%m'), '201904') + '01', freq='MS'))
        if fields is None:
            self.fields = ['open', 'high', 'low', 'close', 'volume']
        else:
            pass  # 设置要读取的字段，如果没有给定输入，则默认读取开、高、低、收、量
        self.listedCodes = []
        self.limit = limit
        self.outputTimes = 1  # 记录输出的数据的条数
        self.totalLength = limit
        self.initialSignal = 0
        self.currentTimeLine = []
        self.isEof = False  # 判断是否全部输出完毕的signal
        self.valGen = None  # 储存每一次yield的数据
        self.actualStart = None
        self.actualEnd = None
        self.currentInstruments = []  # 储存当前返回的dataframe中的股票

        self.setFilePath()

    def setFilePath(self, path=None):
        '''
        设置文件夹路径
        :param section: 使用pathSelector，读取datapath.ini中预先配置好的本地数据路径
        :return:
        '''
        if path is not None:
            self.path = path
        else:
            self.path = pathSelector.PathSelector.getDataFilePath(
                const.DataMarket.STOCK,
                const.DataType.OHLCV,
                const.DataFrequency.freq2lable(
                    self.frequency))
        # 用self.startEnd读取存放股票的上市、退市日期
        self.startEnd = pd.read_excel(os.path.join(self.path, 'start_end_date.xlsx'), dtype={'index': str})
        self.indexConstituent = pd.read_pickle(os.path.join(self.path, 'indexconstituent.pickle'))
        self.totalLength = self.getTotalLength()

    def getTotalLength(self):
        '''
        :return: 返回本次查询所返回的总数据条数
        '''
        totalLength = 0
        totalIndex = []
        tmpTimeLine = self.appliedTimeLine.copy()
        for i in np.arange(len(tmpTimeLine)):
            df = self.fileterH5File(self.path, tmpTimeLine[i], 'trdstat')
            tmp = list([j for j in np.unique(df.index)  # 记录每个月内，交易时间介于start和end之间的数量
                        if (pd.to_datetime(j) <= self.end) & (pd.to_datetime(j) >= self.start)])
            totalIndex = totalIndex + tmp
            self.logger.debug("Checking timeline {}.".format(tmpTimeLine[i]))
            if (self.actualStart == None) & (len(tmp) > 0):
                self.actualStart = tmp[0]
            else:
                continue
            if len(tmp) > 0:
                self.actualEnd = tmp[-1]
            totalLength += len(tmp)
            del tmp

            if (self.limit is not None):
                if (totalLength >= self.limit):  # 若数量已经超过limit，从for循环中break
                    totalIndex = totalIndex[:self.limit]
                    self.actualEnd = totalIndex[-1]
                    break
        if self.instruments == None:
            self.registeredInstruments = list(df.columns)
        self.totalIndex = totalIndex

        return len(totalIndex)  # 返回 limit和totalLength中的小值

    def indexConstituentList(self, index, date):
        '''
        :param path: pickle文件的路径
        :param index:  要查询的指数 50,300,500
        :param date:   查询的日期
        :return:  成分股列表
        '''
        date = str(date)[:10]
        constituentStock = self.indexConstituent[index][date]['code'].tolist()
        constituentStockUpdate = []

        # 股票代码处理：去掉后面的字母，只保留前六位数字
        for code in constituentStock:
            constituentStockUpdate.append(code[:6])

        return constituentStockUpdate

    def fileterH5File(self, folder, date, dataName):
        '''
        :param folder:  存储股票分钟数据的总文件夹路径
        :param date:   要读取的数据的日期，数据类型为datetime
        :param dataName:  要读取的字段名称，数据类型为字符串，如'close'
        :return:   返回一个dataframe,索引为分钟级的datetime，列为需要查询的股票代码
        '''
        year = str(date)[0:4]  # 设置年份、月份，和文件路径
        month = str(int(str(date)[5:7]))
        dataname = str(dataName) + '.h5'
        path = os.path.join(folder, year, month, dataname)
        df = pd.read_hdf(path)  # 读取h5文件，只保留需要需要查询的股票
        if (self.instruments is None) or (self.instruments == 'SZ50') or (self.instruments == 'HS300') or (self.instruments == 'ZZ500'):
            cols = (df.columns)
        else:
            cols = (df.columns) & self.instruments  # 若查询特定的股票列表，则取交集
        # 重新设置索引为'datetime',格式为1min级别的datetime，删去先前的'date','time'两列

        if platformSectionSelector() == 'lixiao':
            # 百度网盘数据
            df = df[cols].reset_index()
            df['datetime'] = pd.to_datetime(
                df['date'].dt.strftime('%Y%m%d') +
                df['time'].apply(str).str.zfill(4))
            df = df.drop(['date', 'time'], axis=1)
            df = df.set_index('datetime')
        elif "xuefu" in platformSectionSelector():
            # 服务器数据
            df = df[cols]
        else:
            # 百度网盘数据
            df = df[cols].reset_index()
            df['datetime'] = pd.to_datetime(
                df['date'].dt.strftime('%Y%m%d') +
                df['time'].apply(str).str.zfill(4))
            df = df.drop(['date', 'time'], axis=1)
            df = df.set_index('datetime')

        if (dataName == 'volume'):
            return (df / 100).round(0) * 100
        elif (dataName == 'amount'):
            return df.round(0)
        else:
            return df.round(2)

    def getNeededData(self, dataName):
        '''
        根据输入的字段名称,获取对应字段数据，每次调用返回1个月的数据
        :param dataName: 要查询的字段名称，如'close'
        :return: 返回一个dataframe
        '''
        date = self.appliedTimeLine[0]  # 所要读取的日期为，appliedTimeLine的第一个值
        df = self.fileterH5File(self.path, date, dataName)
        df = df[(df.index >= self.start)]  # 只保留查询起始日期start之后的那些行
        df['field'] = dataName  # 新加一列，列名为'field',值全部填充为该字段的名字
        year = str(date)[0:4]
        month = str(int(str(date)[5:7]))
        self.logger.info("{} {} {} data got.".format(year, month, dataName))
        return df

    def getAllFields(self):
        '''
        这是获取每个所需字段数据的函数
        :return:
        '''
        df = pd.DataFrame()
        for field in self.fields:  # 遍历需要查询的字段
            tmp = self.getNeededData(field)
            df = pd.concat([df, tmp])
            del tmp
        if len(self.appliedTimeLine) > 0:  # 如果所读取的月份不是本地的最后一个月份，那么把第一个值pop掉
            self.appliedTimeLine.pop(0)
        return df

    def prepareOutputData(self):
        '''
        准备数据，存进self.dfs里面
        '''
        dfs = self.getAllFields()  # 调用getAllFields函数，得到拼接好的1个月数据
        # 将df根据'datetime'和'field'排序

        toDel = [col for col in dfs.columns if (col not in list(self.startEnd['index'])) & (col != 'field')]
        dfs = dfs.drop(toDel, axis=1)  # 把市场中没有的股票代码统一去除
        dfs = dfs.sort_values(['datetime', 'field'])
        if self.initialSignal == 0:
            if (self.instruments == 'SZ50') or (self.instruments == 'HS300') or (self.instruments == 'ZZ500'):
                self.registeredInstruments = self.setRegisteredInstruments()
            else:
                if self.instruments is not None:
                    tmp = list(dfs.columns)[:-1]
                else:
                    tmp = [j for j in (self.registeredInstruments) if j in list(self.startEnd['index'])]
                beforeDelisted = list(self.startEnd[(self.startEnd['end_date'] >= self.actualStart)]['index'])
                afterlisted = list(self.startEnd[(self.startEnd['start_date'] <= self.actualEnd)]['index'])
                self.registeredInstruments = sorted([j for j in tmp if (j in beforeDelisted) & (j in afterlisted)])
            self.initialSignal = 1
            self.currentInstruments = self.registeredInstruments

        if self.instruments == None:
            dfs = pd.DataFrame(dfs, columns=self.registeredInstruments + ['field'])
        self.dfs = dfs
        tmp = np.unique(dfs.index)  # 用tmp储存当前读取出的这个月份的数据的所有交易分钟
        self.currentTimeLine = [j for j in tmp if pd.to_datetime(j) <= self.end]  # 把这些交易时间中，不大于end的时间，用currentTimeLine储存

    def prepareGenerator(self):
        '''
            初始化生成器，并检查新prepareOutputData得到的数据是否满足继续输出的条件
            '''
        if len(self.currentTimeLine) > 0:
            self.prepareOutputData()
        else:  # 如果在start非交易日，并且当月start之后也没有交易日，那就要运行两遍prepareOutputData
            while len(self.currentTimeLine) == 0:
                self.prepareOutputData()
        if len(self.currentTimeLine) > 0:
            self.valGen = self.valueGenerator()
        else:  # 如果新prepareOutputData所得的currentTimeLine长度为0，则设置终止信号
            self.isEof = True

    def valueGenerator(self):
        '''
        生成器
        :return:从dfs中读取下一个时间的数据,返回时间和一个dataframe：行为code,列为高开低收量
        '''
        if self.limit - self.outputTimes == 0:
            self.isEof = True
        for idx, date in enumerate(
                self.currentTimeLine):  # 遍历currentTimeLine中的时间

            tmp = self.dfs[self.dfs.index == date]  # 获取每一分钟的数据，将索引设置为'field'
            ret = self.adjustData(tmp, date)  # 调用adjusatData函数，对输出数据的格式进行调整
            ret = ret.set_index('field').T  # 转置矩阵
            ret.index.names = ['code']
            if idx == len(self.currentTimeLine) - 1:  # 把数据全部逐条输出完成后，进行如下判断
                if len(self.appliedTimeLine) == 0:
                    self.isEof = True  # 若本地已经没有可查询的数据，把self.isEof设置为True
                else:
                    self.prepareGenerator()  # 否则，调用prepareGenerator函数,初始化生成器

            self.outputTimes += 1  # 输出数据，并记录输出次数
            yield pd.to_datetime(date), ret

            if (self.outputTimes == self.limit):
                self.isEof = True

    def setRegisteredInstruments(self):
        '''
        :return: 返回成分股信息，用于在regitsteredInstruments中储存当前的成分股
        '''
        if self.instruments == 'SZ50':
            cols = self.indexConstituentList(index=50, date=self.actualStart)
        elif self.instruments == 'HS300':
            cols = self.indexConstituentList(index=300, date=self.actualStart)
        elif self.instruments == 'ZZ500':
            cols = self.indexConstituentList(index=500, date=self.actualStart)
        else:
            raise '%s未记录成分股' % self.instruments
        return sorted(cols)

    def adjustData(self, df, date):
        '''
                对输出的数据进行格式加工
                :param df:  时间对应的股票数据，格式为dataframe
                :param date:  分钟级别的一个时间，格式为datetime
                :return: 返回一个处理后的股票数据dataframe
        '''
        if (self.instruments == 'SZ50') or (self.instruments == 'HS300') or (self.instruments == 'ZZ500'):
            if self.instruments == 'SZ50':
                cols = sorted(self.indexConstituentList(index=50, date=self.start)) + ['field']
            elif self.instruments == 'HS300':
                cols = sorted(self.indexConstituentList(index=300, date=self.start)) + ['field']
            elif self.instruments == 'ZZ500':
                cols = sorted(self.indexConstituentList(index=500, date=self.start)) + ['field']
            else:
                raise '%s未记录成分股' % self.instruments
            df = df[cols]
            self.currentInstruments = list(df.columns)[:-1]  # 更新当前包含的股票代码

        else:  # 如果instruments不是指数

            # 存储该时间，已经退市的股票代码

            notDelisted = list(self.startEnd[(self.startEnd['end_date'] > date)]['index'])
            df = pd.DataFrame(df, columns=notDelisted + ['field'])
            df = pd.DataFrame(df, columns=self.registeredInstruments + ['field'])

            tmp = df.dropna(axis=1, how='any')
            self.currentInstruments = list(tmp.columns).remove('field')
            del tmp

        return df

    def getRegisteredInstruments(self):
        '''
         :return: 返回实际使用的instruments
                 若查询的是指数，则返回的是查询的起始时刻，该指数的成分股；
                 若查询的是
         '''
        return self.registeredInstruments

    def getNextValues(self):
        '''
        :return: 获取生成器返回的下一个值
        '''
        return next(self.valGen)

    def eof(self):
        '''
        :return: 如果触发停止条件：返回True， 否则返回False
                触发条件：1.没有数据更新  2.输出数据条数达到预先设定值  3.start和end之间的数据全部输出完毕，
        '''
        return self.isEof

    def getDateRange(self):
        '''
        返回一个列表，[所要查询的起始日期，所要查询的截止日期]
        :return: start, end
        '''
        if len(self.totalIndex) == 0:
            return 'No data get in the given period.'
        return [self.actualStart, self.actualEnd]

    def getDataShape(self):

        shape = self.totalLength, len(
            self.registeredInstruments), len(
            self.fields)
        return shape

    def getDir(self):
        '''
        :return: 返回数据的文件夹路径
        '''
        return self.path

    def getFields(self):
        '''
        :return:返回所要查询的数据字段列表
        '''
        return self.fields

    def getFrequency(self):
        '''
        :return: 返回查询的数据频率
        '''
        return self.frequency

    def getCurrentInstruments(self):
        '''
        :return: 返回当前时刻，在市场中正常交易（已上市，未退市）的股票列表； 若查询的是指数，则返回当前该指数的成分股
        '''
        return self.currentInstruments


class H5PanelReader(BasePanelReader):
    '''
    h5 panel 数据读取接口
    '''
    logger = logger.getLogger("H5PanelReader")

    def __init__(
            self,
            dir,
            frequency=bar.Frequency.MINUTE,
            start=None,
            end=None):
        '''
        初始化
        param frequency: 数据频率
        param dir: 文件路径
        param start: 所需要获取的数据开始时间
        param end: 所需要获取的数据结束时间
        '''
        super().__init__()
        self.frequency = frequency
        self.start = start
        self.end = end
        self.dir = dir
        self.isEof = False
        self.count = 0
        self.staticPanel = None
        self.staticSeries = None
        self.iterator = None

    def retrieve(self):
        '''
        获取数据，存入dataframe
        '''
        # 读取单个文件
        self.df = pd.read_hdf(path_or_buf=self.dir)
        # 若未输入所需开始时间，则取数据自身的开始时间
        self.start = pd.Timestamp(
            self.start) if self.start else self.df.index[0]
        if self.start < self.df.index[0]:
            self.logger.warning(
                "The input start date {} is before the data's start date {}".format(
                    self.start, self.df.index[0]))
        # 若未输入所需结束时间，则取数据自身的结束时间
        self.end = pd.Timestamp(self.end) if self.end else self.df.index[-1]
        if self.end > self.df.index[-1]:
            self.logger.warning("The input end date {} is after the data's end date {}"
                                .format(self.end, self.df.index[-1]))
        self.df = self.df.loc[self.start:self.end]

        return self.df

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
        return self.dir

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
        返回数据起止日期
        '''
        return (self.df.index[0], self.df.index[-1])

    def getRegisteredInstruments(self):
        '''
        返回股票代码
        '''
        self.instrumentList = []
        if len(self.df.shape) == 2:
            self.instrumentList = self.df.columns.values.tolist()
        return self.instrumentList

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
        直接将全量数据转换为静态的SequenceDataPanel或者SequenceDataSeries，返回对象
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
        pass


class H5BatchPanelReader(BasePanelReader):
    '''
    h5 panel 数据读取接口，用于同时读取多个文件
    '''
    logger = logger.getLogger("H5PanelReader")

    def __init__(self, factorName=None, frequency=None, start=None, end=None):
        '''
        初始化
        param path: 文件夹路径
        param frequency: 数据频率
        param start: 所需要获取的数据开始时间
        param end: 所需要获取的数据结束时间
        '''
        super().__init__()
        self.frequency = frequency
        self.market = bar.Market.STOCK
        self.filePathDict = {}  # 用于存储文件路径的字典
        self.testResultDict = {}  # 用于所获得的存储数据dataframe的字典
        self.readerDict = {}  # 用于存储单因子读取H5PanelReader对象的字典
        self.nextValueDict = {}  # 用于存储所有文件下一行数据的字典
        self.staticPanelDict = {}  # 用于存储静态panel或者series的字典
        self.start = start
        self.end = end
        self.isEof = False
        self.availFctList = None
        self.factorName = factorName
        self.setFilePath()

    def setFilePath(self):
        '''
        设置文件夹路径
        :param section: 使用pathSelector，读取datapath.ini中预先配置好的本地数据路径
        :return:
        '''
        pathSelctor = pathSelector.PathSelector()
        # 生成PathSelector类，按照所输入的section，获取存放h5的路径
        self.path = pathSelctor.getFactorFilePath(factorName=self.factorName,
                                                  factorFrequency=self.frequency)


    def prepareOutputData(self):
        '''
        获取数据，生成dataframe，并将文件名及对应的路径、reader对象、和dataframe存入相应的字典
        '''

        # 判断路径下是否有子文件夹，有的话将所有子文件夹的路径存入一个list
        folderNameList = [name for name in os.listdir(self.path) if
                          os.path.isdir(os.path.join(self.path, name))]
        print(folderNameList)
        # 判断folderNameList是否有值，若有，说明包含子文件夹，则读取所有resample文件夹
        if folderNameList:
            # 将上述子文件夹生成全路径后，存入list
            folderDirList = [
                os.path.join(
                    self.path,
                    folderName) for folderName in folderNameList]
            # 遍历每个resample文件夹
            for folderDir in folderDirList:
                # 获取每个因子文件夹下的所有h5文件
                fileNameList = [name for name in os.listdir(folderDir) if ".h5" in name]
                # fileNameList = os.listdir(folderDir)
                # 将上述h5文件生成全路径后，存入list
                filePathList = [
                    os.path.join(
                        folderDir,
                        fileName) for fileName in fileNameList]
                # 遍历单个resample文件夹下的所有h5文件
                for fileName, filePath in zip(fileNameList, filePathList):
                    # 将文件名及文件全路径存入相应字典
                    self.filePathDict[fileName] = filePath
                    # 将文件名及reader对象存入相应字典
                    self.readerDict[fileName] = H5PanelReader(
                        filePath, self.frequency, self.start, self.end)
                    # 将文件名及所读取的dataframe存入相应字典
                    self.testResultDict[fileName] = self.readerDict[fileName].retrieve(
                    )

        # 若folderNameList为空，说明路径下没有子文件夹，则读取单个resample文件夹
        else:
            # 获取所需因子文件夹下的所有h5文件
            fileNameList = [name for name in os.listdir(self.path) if ".h5" in name]
            # fileNameList = os.listdir(self.path)
            # 将上述h5文件生成全路径后，存入list
            filePathList = [os.path.join(self.path, fileName)
                            for fileName in fileNameList]
            for fileName, filePath in zip(fileNameList, filePathList):
                self.filePathDict[fileName] = filePath
                self.readerDict[fileName] = H5PanelReader(
                    filePath, self.frequency, self.start, self.end)
                self.testResultDict[fileName] = self.readerDict[fileName].retrieve(
                )

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

    def getFilePath(self):
        '''
        返回存储文件路径的字典
        '''
        return self.filePathDict

    def getTestResult(self):
        '''
        返回存储检测数据dataframe的字典
        '''
        return self.testResultDict

    def getReader(self):
        '''
        返回H5PanelReader对象的字典
        '''
        return self.readerDict

    def getDataShape(self):
        '''
        返回包含数据长度的字典
        '''
        self.dataShapeDict = {}
        for key, value in self.readerDict.items():
            self.dataShapeDict[key] = value.getDataShape()
        return self.dataShapeDict

    def getDateRange(self):
        '''
        返回包含数据起止日期的字典
        '''
        self.dateRangeDict = {}
        for key, value in self.readerDict.items():
            self.dateRangeDict[key] = value.getDateRange()
        return self.dateRangeDict

    def getRegisteredInstruments(self):
        '''
        返回包含股票代码的字典
        '''
        self.instrumentDict = {}
        for key, value in self.readerDict.items():
            self.instrumentDict[key] = value.getRegisteredInstruments()
        return self.instrumentDict

    def getIterator(self):
        '''
        返回包含迭代器对象的字典
        '''
        self.iterDict = {}
        for key, value in self.readerDict.items():
            self.iterDict[key] = value.getIterator()
        return self.iterDict

    def peekNextValues(self, availFctList=None):
        '''
        通过reader字典里的reader对象调用getNextValues()，如果下一行还有值的话，
        存入nextValueDict字典中
        nextValueDict字典是一个只存储生成的单行值的字典
        当reader下一行无值时，nextValueDict会将该reader从字典中删除，从而做到只输出存在的值
        '''
        availFctList = list(
            self.readerDict.keys()) if availFctList is None else availFctList
        for factor in availFctList:
            if not self.readerDict[factor].eof():
                dateTime, value = self.readerDict[factor].getNextValues()
                self.nextValueDict[factor] = (dateTime, value)
            else:  # 若某个因子已经读取完了，则将该因子在availFctList和nextValueDict中删除
                availFctList.remove(factor)
                del self.nextValueDict[factor]
                if self.nextValueDict == {}:
                    self.isEof = True  # 若nextValueDict为空，则说明所有文件已经读取完

        self.availFctList = availFctList  # 更新availFctList

    def getNextValues(self):
        '''
        返回包含下一行数据的字典
        '''
        self.peekNextValues(self.availFctList)
        return self.nextValueDict

    def eof(self):
        '''
        返回下一行字典的判断值
        '''
        return self.isEof

    def to_static_panel(self):
        '''
        直接将全量数据转换为静态的SequenceDataPanel或者SequenceDataSeries，返回对象
        '''
        for key, value in self.readerDict.items():
            self.staticPanelDict[key] = value.to_static_panel()
        return self.staticPanelDict

    def to_frame(self):
        '''
        获取的数据已为dataframe，此处无转换
        '''
        pass


if __name__ == '__main__':
    '''H5DataReader测试用例'''
    # start = '20150101 1500'  # 查询的起始时间
    # end = '20170501 1500'  # 查询的截止时间
    # instruments = None  # ['000001', '000002']  # 要查询的股票代码列表
    # limit = 100  # 输出查询数据的条数上限
    # fields = ['close', 'open', 'high', 'low']  # 要查询的字段名称列表
    # # 生成h5Reader类
    # h5DataReader = H5DataReader(
    #     frequency=bar.Frequency.MINUTE,
    #     start=start,
    #     end=None,  # end,
    #     instruments=instruments,
    #     fields=const.DataField.OHLCV,
    #     limit=limit)
    # h5DataReader.prepareGenerator()  # 调用生成器
    # print(h5DataReader.getRegisteredInstruments())
    # print(h5DataReader.actualStart, h5DataReader.actualEnd)
    # print(h5DataReader.totalLength)
    # # 逐条返回数据，直到h5DataReader.eof为True
    # h5DataReader.run(stopCount=100, _print=True)

    '''H5PanelReader测试用例'''
    H5PanelBatchReader = H5BatchPanelReader(factorName='maPanelFactor')
    H5PanelBatchReader.prepareOutputData()
    # H5PanelBatchReader.getIterator()
    #
    # H5PanelBatchReader.run(50)

    print(H5PanelBatchReader.getFilePath())
    print(H5PanelBatchReader.getTestResult())
    print(H5PanelBatchReader.getReader())
    print(H5PanelBatchReader.getDataShape())
    print(H5PanelBatchReader.getDateRange())
    print(H5PanelBatchReader.getRegisteredInstruments())
    # staticPanelDict = H5PanelBatchReader.to_static_panel()
    # for key, value in staticPanelDict.items():
    #     if "Panel" in str(value):
    #         print(key, value.to_frame())
    #     else:
    #         print(key, value.to_series())
