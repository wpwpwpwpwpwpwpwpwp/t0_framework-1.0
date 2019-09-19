from collections import namedtuple  # 此处调用的是Python里自带的collections包
import collections
import numpy as np
from pyalgotrade import observer
from pyalgotrade import bar
from cpa.utils import logger
from cpa.utils import series
import warnings
from cpa.io import BasePanelReader, BaseDataReader

Bar = namedtuple('Bar', 'dateTime open high low close volume')


class Field:
    OHLCV = ['open', 'high', 'low', 'close', 'volume']


class BarFeed:
    '''
    输入股票代码，时间序列，对应是开、高、低、收、量slice
    默认BarFeed不再注册事件，使用主动调用的方式处理，单个Series具有事件，在指标更新时使用,
    **但不建议使用该方法，尽量使用panelIndicator 或者 dataPanel.apply处理**
    单个instrument 对应的OHLC 数据切片
    noneNaClose 为非切片
    '''
    __slots__ = (
        '__dataFeed',  # 所切片Feed
        '__instrument',  # 代码
        '__dateTimes',  # 时间序列
        '__openSlice',  # 开盘价
        '__highSlice',  # 最高价
        '__lowSlice',  # 最低价
        '__closeSlice',  # 收盘价
        '__volumeSlice',  # 交易量
        '__noneNaClose',
        '__newCloseEvent',
        '__newNoneNaCloseEvent',
        '__frequency',
        '__maxLen',
    )

    def __init__(self, dataFeed, instrument, dateTimes, _open, _high, _low, _close, volume, frequency, maxLen=None):
        """slice 长度恒定, 须按照现有数据行长度剪切, sliceSeries保留事件驱动"""

        self.__dataFeed = dataFeed
        self.__instrument = instrument
        self.__dateTimes = dateTimes
        self.__openSlice = _open
        self.__highSlice = _high
        self.__lowSlice = _low
        self.__closeSlice = _close
        self.__volumeSlice = volume
        self.__frequency = frequency
        self.__maxLen = maxLen

    def __len__(self):  # 获取时间长度
        return self.__dateTimes.__len__()

    def getInstrument(self):  # 获取股票代码
        return self.__instrument

    def getFrequency(self):
        return self.__frequency

    def getMaxLen(self):
        return self.__maxLen

    def getOpenSlice(self):
        return self.__openSlice

    def getHighSlice(self):
        return self.__highSlice

    def getLowSlice(self):
        return self.__lowSlice

    def getCloseSlice(self):
        return self.__closeSlice

    def getVolumeSlice(self):
        return self.__volumeSlice

    def getOpenSeries(self):
        '''
        :return: 返回带索引的sliceSeries，索引为dateTime
        '''
        return series.SliceDataSeries(self.__dataFeed.openPanel, self.__dateTimes, self.__openSlice, name='open')

    def getHighSeries(self):
        return series.SliceDataSeries(self.__dataFeed.highPanel, self.__dateTimes, self.__highSlice, name='high')

    def getLowSeries(self):
        return series.SliceDataSeries(self.__dataFeed.lowPanel, self.__dateTimes, self.__lowSlice, name='low')

    def getCloseSeries(self):
        return series.SliceDataSeries(self.__dataFeed.closePanel, self.__dateTimes, self.__closeSlice, name='close')

    def getVolumeSeries(self):
        return series.SliceDataSeries(self.__dataFeed.volumePanel, self.__dateTimes, self.__volumeSlice, name='volume')

    def getDateTimes(self):  # 获取所有时间序列
        return self.__dateTimes

    def getLastBar(self):  # 获取最后一个时间的值
        return Bar(dateTime=self.__dateTimes[-1],
                   open=self.__openSlice[-1],
                   high=self.__highSlice[-1],
                   low=self.__lowSlice[-1],
                   close=self.__closeSlice[-1],
                   volume=self.__volumeSlice[-1])


class PanelFeed:
    # 事件优先级, 重采样要高于计算因子
    class EventPriority:

        PREFILTER = 1000
        INDICATOR = 2000
        RESAMPLE = 3000
        FACTOR = 4000

        FACTORCOMB = 5000  # 默认优先级
        FACTORCOMBTEST = 6000
        @classmethod
        def getEventsType(cls):
            return sorted([1000, 2000, 3000, 4000, 5000,6000])

    def __init__(self, dataSource, instruments, frequency=bar.Frequency.MINUTE, maxLen=None):

        self.dataSource = dataSource
        self.instruments = sorted(instruments)
        self.maxLen = maxLen
        self.frequency = frequency
        self.__currentDatetime = np.nan
        self.fields = list(self.dataSource.fields)
        self.stopped = False

        if 'open' in self.fields:
            assert instruments == sorted(instruments), 'OHLCV columns must be sorted!'
            self.openPanel = series.SequenceDataPanel(instruments, self.maxLen, dtype=np.float32)  # 定义了一个类
            self.highPanel = series.SequenceDataPanel(instruments, self.maxLen, dtype=np.float32)
            self.lowPanel = series.SequenceDataPanel(instruments, self.maxLen, dtype=np.float32)
            self.closePanel = series.SequenceDataPanel(instruments, self.maxLen, dtype=np.float32)
            self.volumePanel = series.SequenceDataPanel(instruments, self.maxLen)

            self.barFeeds = {}
            for instrument in instruments:
                self.barFeeds[instrument] = BarFeed(self,
                                                    instrument=instrument,
                                                    dateTimes=self.closePanel.getDateTimes(),
                                                    _open=self.openPanel[instrument],
                                                    _high=self.highPanel[instrument],
                                                    _low=self.lowPanel[instrument],
                                                    _close=self.closePanel[instrument],
                                                    volume=self.volumePanel[instrument],
                                                    frequency=self.frequency,
                                                    maxLen=self.maxLen)

        self.fields = list(self.dataSource.fields)
        self.extraPanel = {}
        for field in self.fields:
            if field not in Field.OHLCV:
                self.extraPanel[field] = series.SequenceDataPanel(instruments, maxLen, dtype=np.float32)

        self.__panelEvents = collections.OrderedDict({e: observer.Event() for e in self.EventPriority.getEventsType()})

    def getCurrentDatetime(self):
        return self.__currentDatetime

    def getInstruments(self):
        return self.instruments

    def getFrequency(self):
        return self.frequency

    def getFields(self):
        return self.fields

    def getNewPanelsEvent(self, priority=None):

        assert priority in self.EventPriority.getEventsType()
        return self.__panelEvents[priority]

    def getMaxLen(self):
        return self.maxLen

    def eof(self):
        return self.dataSource.eof()

    def getOpenPanel(self):
        return self.openPanel

    def getHighPanel(self):
        return self.highPanel

    def getLowPanel(self):
        return self.lowPanel

    def getClosePanel(self):
        return self.closePanel

    def getVolumePanel(self):
        return self.volumePanel

    def dispatchNewValueEvent(self, *args, **kwargs):

        for key, evt in self.__panelEvents.items():
            evt.emit(*args, **kwargs)

    def appendNextValues(self, dateTime, df):
        '''
        更新内部数值
        '''
        self.__currentDatetime = dateTime

        assert set(self.instruments) == set(df.index.values), '成分股发生变化！'

        if 'close' in self.dataSource.fields:
            self.openPanel.appendWithDateTime(dateTime, df['open'].sort_index().values)
            self.highPanel.appendWithDateTime(dateTime, df['high'].sort_index().values)
            self.lowPanel.appendWithDateTime(dateTime, df['low'].sort_index().values)
            self.closePanel.appendWithDateTime(dateTime, df['close'].sort_index().values)
            self.volumePanel.appendWithDateTime(dateTime, df['volume'].sort_index().values)

        for extraField in self.extraPanel:
            self.extraPanel[extraField].appendWithDateTime(dateTime, df[extraField].sort_index().values)

    def getNextValues(self):

        dateTime, df = self.dataSource.getNextValues()
        self.appendNextValues(dateTime, df)
        self.dispatchNewValueEvent(self, dateTime, df)

        return dateTime, df

    def __len__(self):
        return self.closePanel.__len__()

    def getDataSource(self):
        return self.dataSource

    def getExtra(self, field=None):
        if field is None:
            return self.extraPanel
        return self.extraPanel[field]

    def run(self, stopCount=None, _print=False):
        counter = 0
        while not self.eof() and not self.stopped:
            counter += 1
            dateTime, df = self.getNextValues()
            if _print:
                print(dateTime)
            if stopCount is not None and counter > stopCount:
                break


class AdvancedFeed(PanelFeed):
    '''
    有时候feed不在一个数据源，此时为保证feed同步，将一个或多个feed合并成一个调用
    指数的lable为'base'
    '''
    logger = logger.getLogger('AdvancedFeed')

    class EmptyDataSource:
        fields = []

        @classmethod
        def getFields(cls):
            return []

    def __init__(self, feedDict=None, panelDict=None):
        '''
        :param feedDict: {lable: panelFeed}
        '''
        super().__init__(self.EmptyDataSource, [], None, None)
        self.feedDict = feedDict if feedDict is not None else {}
        self.panelDict = panelDict if panelDict is not None else {}

        self.sortedFeeds = []  # 同时包含feed和panel两种数据
        self.sortedLable = []
        self.dataSource = []
        self.instruments = None
        self.frequency = np.inf
        self.maxLen = 0

        self.synchronizedNextValue = {}  # lable : (dateTime, value)
        self.isEof = False
        self.available = None
        self.benchPanel = None
        self.baseDataLable = None  # OHLCV所在feed为baseFeed

        multiDict = {}
        if feedDict is not None:
            multiDict = dict(multiDict, **feedDict)
        if panelDict is not None:
            multiDict = dict(multiDict, **panelDict)

        for lable, value in multiDict.items():
            if lable in feedDict:
                self.attachFeed(lable, value)
            else:
                self.attachPanel(lable, value)  # 更新sequenceDataPanel

    def __attachBaseInfo(self, lable, value, source):
        '''
        :param initialize base info, panelFeed and sequence DataPanel
        :return:
        '''

        if isinstance(source, BaseDataReader) and 'close' in source.getFields():
            self.sortedFeeds.insert(0, value)
            self.sortedLable.insert(0, lable)
            self.baseDataLable = lable
        else:
            self.sortedFeeds.append(value)
            self.sortedLable.append(lable)
        self.dataSource.append(source)

        if not (isinstance(source, BasePanelReader) and source.isInstrumentCol is False):
            if self.instruments is None:
                self.instruments = source.getRegisteredInstruments()
            elif self.instruments != source.getRegisteredInstruments():
                removeList = list(set(self.instruments) - set(source.getRegisteredInstruments()))  # 求差集
                self.logger.info("Miss Data stock list:\n{}".format(removeList))
                self.instruments = list(set(self.instruments) & set(source.getRegisteredInstruments()))  # 求交集
        if source.getFrequency() < self.frequency:
            self.frequency = source.getFrequency()
        if value.getMaxLen() > self.maxLen:
            self.maxLen = value.getMaxLen()

    def hasOhlcv(self):
        '''
        :return:数据中含有高开低收数据
        '''
        return self.baseDataLable is not None

    def attachFeed(self, lable, otherFeed):
        '''
        :param panelFeed: 将两个不同数据源的feed数据合并到一个调用（如OHLC数据和财务数据分属不同数据源）
        :param lable:标签
        :return:
        '''

        otherDataSource = otherFeed.getDataSource()
        self.__attachBaseInfo(lable, otherFeed, otherDataSource)
        self.feedDict[lable] = otherFeed
        for field in otherFeed.getFields():
            assert field not in self.fields, 'duplicate field attached {}'.format(field)
            self.fields.append(field)

        if 'close' in otherDataSource.getFields():
            self.openPanel = otherFeed.openPanel
            self.highPanel = otherFeed.highPanel
            self.closePanel = otherFeed.closePanel
            self.lowPanel = otherFeed.lowPanel
            self.volumePanel = otherFeed.volumePanel
        if otherFeed.getFrequency() != self.getFrequency() and self.getFrequency() != np.inf:
            warnings.warn(u'不同周期的数据元同步,会导致数据内部的时间不同步, 进行运算时请留意')

        for extraField in otherFeed.getExtra():
            self.extraPanel[extraField] = otherFeed.getExtra(extraField)
            self.fields.append(extraField)

    def attachBenchMark(self, benchMarkPanel):
        '''
        :param benchMarkPanel:添加指数行情
        :return:
        '''
        self.attachPanel('bench', benchMarkPanel)

    def attachPanel(self, lable, otherPanel):
        '''
        :param lable:
        :param panel:dataPanel 数据
        :return:
        '''

        otherDataSource = otherPanel.getDataSource()
        self.__attachBaseInfo(lable, otherPanel, otherDataSource)
        self.panelDict[lable] = otherPanel

        if lable == 'bench':
            self.benchPanel = otherPanel
        else:
            self.extraPanel[lable] = otherPanel

        assert lable not in self.fields, 'duplicate field attached {}'.format(lable)
        self.fields.append(lable)

    def getBench(self):
        '''
        :return:返回指数
        '''
        return getattr(self, 'benchPanel', None)

    def getBase(self):
        '''
        :return: 返回价量所在feed
        '''
        assert 'close' in self.fields
        return self.sortedFeeds[0]

    def peekNextValues(self, available=None):
        '''
        :param available: 批量读取下一行数据,如果上次没有使用则不再读取
        :return:
        '''
        available = self.sortedLable if available is None else available
        for lable in available:
            lableIdx = self.sortedLable.index(lable)
            if not self.sortedFeeds[lableIdx].getDataSource().eof():
                dateTime, value = self.sortedFeeds[lableIdx].getDataSource().getNextValues()

                # concurrent day level frequency to 15:00
                if self.sortedFeeds[lableIdx].getDataSource().getFrequency() >= bar.Frequency.DAY > self.getFrequency():
                    dateTime = dateTime.replace(hour=15)

                self.synchronizedNextValue[lable] = (dateTime, value)
            else:
                del self.synchronizedNextValue[lable]
                if self.synchronizedNextValue == {}:
                    self.isEof = True

        self.available = available

    def peekNextDatetime(self, sychronizedNextValues):
        '''
        :param sychronizedNextValues:从候选列表中选择最小时间
        :return:
        '''
        return list(sorted([dateTime for dateTime, _ in sychronizedNextValues.values()]))[0]

    def getAvailable(self):
        '''
        :return:返回当前时刻有数据更新的feed或panel的lable名
        '''
        return self.available

    def getNextValues(self):
        '''
        :return:
        '''
        self.peekNextValues(self.available)
        nextTime = self.peekNextDatetime(self.synchronizedNextValue)

        if self.hasOhlcv() and self.baseDataLable not in self.synchronizedNextValue:
            self.logger.info('there is no extra ohlc values, strategy will end')
            self.stopped = True
            return None, None

        available = []
        for lable, value in self.synchronizedNextValue.items():
            dateTime, value = value
            if dateTime == nextTime:
                available.append(lable)
                if lable in self.feedDict:
                    self.feedDict[lable].appendNextValues(dateTime, value)
                else:
                    self.panelDict[lable].appendWithDateTime(dateTime, value.values)

        self.available = available

        if self.benchPanel is not None and self.synchronizedNextValue['bench'][0] > nextTime:
            self.logger.info('benchMark exists, lower than {} time is ignored, current time {}'.format(
                self.synchronizedNextValue['bench'][0], nextTime))

            return None, None
        else:
            self.dispatchNewValueEvent(self, nextTime, None)
            return nextTime, None

    def eof(self):
        '''
        :return: 全部数据为空
        '''
        return self.isEof

    def run(self, stopCount=None, _print=False):
        counter = 0
        while not self.eof() and not self.stopped:
            dateTime, df = self.getNextValues()
            if dateTime:
                counter += 1
            if _print:
                print(dateTime, self.available)
            if stopCount is not None and counter > stopCount:
                break


if __name__ == '__main__':
    from cpa.feed.feedFactory import InlineDataSet
    from cpa.io.csvReader import CSVPanelReader
    from cpa.config import const

    '''薛富调试'''
    # panelReader = CSVPanelReader(bar.Frequency.MINUTE, const.DataFile.INDEX, isInstrmentCol=False, start='20140101', end='20141231')
    # panelReader.loads()
    # dataPanel = series.SequenceDataPanel.from_reader(panelReader)

    '''李霄调试'''
    panelFeed = InlineDataSet.ZZ500_MINUTE()
    path = r"D:\PythonProject\PycharmProjects\t0_frameWork\cpa\data"
    fileName = 'IC.CCFX.csv'
    indexReader = CSVPanelReader(path=path, fileName=fileName, frequency=bar.Frequency.MINUTE, isInstrmentCol=False,
                                 start='20150601')
    indexReader.loads()
    dataPanel = series.SequenceDataPanel.from_reader(indexReader)
    feed = AdvancedFeed(feedDict={'base': panelFeed}, panelDict={'bench': dataPanel})

    feed.run(100, _print=True)
    print()
