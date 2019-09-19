from collections import namedtuple  # 此处调用的是Python里自带的collections包
import collections
import numpy as np
from pyalgotrade import observer
from cpa.utils import bar
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

        LAST = 10000

        @classmethod
        def getEventsType(cls):
            return sorted([1000, 2000, 3000, 4000, 5000, 6000, 10000])

    def __init__(self, dataSource, instruments, frequency=bar.Frequency.MINUTE, maxLen=None):

        self.dataSource = dataSource
        self.instruments = sorted(instruments)
        self.maxLen = maxLen
        self.frequency = frequency
        self.__currentDatetime = None
        self.fields = list(self.dataSource.fields)
        self.stopped = False
        self.setUseEventDateTimeInLogs(True)

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

    def getCurrentDateTime(self):
        return self.__currentDatetime

    def setUseEventDateTimeInLogs(self, useEventDateTime):
        if useEventDateTime:
            if logger.Formatter.DATETIME_HOOK is None: #使用第一次的时间（即最小级别）
                logger.Formatter.DATETIME_HOOK = self.getCurrentDateTime
        else:
            logger.Formatter.DATETIME_HOOK = None

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
        self.__currentDatetime = dateTime
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

    def run(self, stopCount=None, _print=False, callBack=None):
        '''
        :param stopCount:
        :param _print:
        :param callBack: 回调
        :return:
        '''
        counter = 0
        while not self.eof() and not self.stopped:
            counter += 1
            dateTime, df = self.getNextValues()
            if _print and callBack is None:
                print(dateTime)
            elif _print and callBack is not None:
                print(dateTime, end='\t')
                callBack()
            elif callBack is not None:
                callBack()
            if stopCount is not None and counter > stopCount:
                break


class AdvancedFeed:
    '''
    有时候feed不在一个数据源，此时为保证feed同步，将一个或多个feed合并成一个调用
    指数的lable为'base'
    '''
    logger = logger.getLogger('AdvancedFeed')

    def __init__(self, feedDict=None, panelDict=None):
        '''
        :param feedDict: {lable: panelFeed}
        '''
        self.feedDict = feedDict if feedDict is not None else {}
        self.panelDict = panelDict if panelDict is not None else {}

        self.resamples = {}  #重采样数据
        self.dataSource = {}
        self.instruments = None
        self.frequency = np.inf
        self.maxLen = 0
        self.__currentDatetime = None
        self.setUseEventDateTimeInLogs(True)

        self.synchronizedNextValue = {}  # lable : (dateTime, value)
        self.isEof = False
        self.stopped = False
        self.available = None
        self.allAvailable = None

        self.__panelEvents = collections.OrderedDict({e: observer.Event() for e in PanelFeed.EventPriority.getEventsType()})

        for lable, value in self.feedDict.items():
            self.attachFeed(lable, value)
        for lable, value in self.panelDict.items():
            self.attachPanel(lable, value)  # 更新sequenceDataPanel

    def __attachBaseInfo(self, lable, value):
        '''
        :param initialize base info, panelFeed and sequence DataPanel
        :return:
        '''

        source = value.getDataSource()

        #重采样数据没有独立数据源
        if source is None:
            self.resamples[lable] = value
            return

        self.dataSource[lable] = source
        if not (isinstance(source, BasePanelReader) and source.isInstrumentCol is False):
            if self.instruments is None:
                self.instruments = value.getInstruments()
            elif self.instruments != value.getInstruments():
                removeList = list(set(self.instruments) - set(value.getInstruments()))  # 求差集
                self.logger.info("Miss Data stock list:\n{}".format(removeList))
                self.instruments = list(set(self.instruments) & set(value.getInstruments()))  # 求交集

        if source.getFrequency() < self.frequency:
            self.frequency = source.getFrequency()
            if self.getFrequency() != np.inf:
                warnings.warn(u'不同周期的数据元同步,会导致数据内部的时间不同步, 进行运算时请留意')

        if value.getMaxLen() > self.maxLen:
            self.maxLen = value.getMaxLen()

    def getNewPanelsEvent(self, priority=None):

        assert priority in PanelFeed.EventPriority.getEventsType()
        return self.__panelEvents[priority]

    def dispatchNewValueEvent(self, *args, **kwargs):

        for key, evt in self.__panelEvents.items():
            evt.emit(*args, **kwargs)

    def hasOhlcv(self):
        '''
        :return:数据中含有高开低收数据
        '''
        return self.getBase() is not None

    def hasBench(self):
        return self.getBench() is not None

    def getFrequency(self):
        return self.frequency

    def getInstruments(self):
        return self.instruments

    def getMaxLen(self):
        return self.maxLen

    def attachFeed(self, lable, otherFeed):
        '''
        :param panelFeed: 将两个不同数据源的feed数据合并到一个调用（如OHLC数据和财务数据分属不同数据源）
        :param lable:标签
        :return:
        '''

        self.feedDict[lable] = otherFeed
        self.__attachBaseInfo(lable, otherFeed)

    def attachBaseFeed(self, baseFeed):
        self.attachFeed('base', baseFeed)

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
        self.panelDict[lable] = otherPanel
        self.__attachBaseInfo(lable, otherPanel)

    def getBench(self):
        '''
        :return:返回指数
        '''
        return self.panelDict.get('bench', None)

    def getBase(self):
        '''
        :return: 返回价量所在feed
        '''
        return self.feedDict.get('base', None)

    def getCurrentDateTime(self):
        return self.__currentDatetime

    def setUseEventDateTimeInLogs(self, useEventDateTime):
        if useEventDateTime:
            logger.Formatter.DATETIME_HOOK = self.getCurrentDateTime
        else:
            logger.Formatter.DATETIME_HOOK = None

    def peekNextValues(self, available=None):
        '''
        :param available: 批量读取下一行数据,如果上次没有使用则不再读取
        :return:
        '''
        available = self.dataSource.keys() if available is None else available
        for lable in available:
            if not self.dataSource[lable].eof():
                dateTime, value = self.dataSource[lable].getNextValues()

                # concurrent day level frequency to 15:00
                if self.dataSource[lable].getFrequency() >= bar.Frequency.DAY > self.getFrequency():
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
        return self.allAvailable


    def getNextValues(self):
        '''
        :return:
        '''
        self.peekNextValues(self.available)
        nextTime = self.peekNextDatetime(self.synchronizedNextValue)
        self.__currentDatetime = nextTime

        if self.hasOhlcv() and 'base' not in self.synchronizedNextValue:
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
                    self.feedDict[lable].dispatchNewValueEvent(self, dateTime, value)
                else:
                    self.panelDict[lable].appendWithDateTime(dateTime, value.values)

        self.available = available
        self.allAvailable = available.copy()
        for lable, feed in self.resamples.items():
            if feed.getCurrentDatetime() == nextTime:
                self.allAvailable.append(lable)

        if self.getBench() is not None and self.synchronizedNextValue['bench'][0] > nextTime:
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

    def run(self, stopCount=None, _print=False, callBack=None):
        counter = 0
        while not self.eof() and not self.stopped:
            dateTime, df = self.getNextValues()
            if dateTime:
                counter += 1
            if _print and callBack is None:
                print(dateTime, self.getAvailable())
            elif _print and callBack is not None:
                print(dateTime, self.getAvailable(), end='\t')
                callBack()
            elif callBack is not None:
                callBack()
            if stopCount is not None and counter > stopCount:
                break


if __name__ == '__main__':
    from cpa.feed.feedFactory import InlineDataSet
    from cpa.io import csvReader
    from cpa.config import const,pathSelector
    from cpa.resample import resampled

    '''薛富调试'''
    panelReader = csvReader.CSVPanelReader(pathSelector.PathSelector.getDataFilePath(types=const.DataType.INDEX,
                                                                                     frequency=const.DataFrequency.HOUR2),
                                                                                     fileName='index_2h.csv',
                                                                                     frequency=bar.Frequency.MINUTE,
                                                                                     isInstrmentCol=False,
                                                                                     start='20150601',
                                                                                     end='20151231')
    panelReader.loads()
    dataPanel = series.SequenceDataPanel.from_reader(panelReader)
    #dataPanel.run(stopCount=10, _print=True)
    panelFeed = InlineDataSet.SZ50_MINUTE()
    #panelFeed.run(stopCount=10, _print=True)
    resampleFeed = resampled.ResampledPanelFeed(panelFeed, bar.Frequency.DAY, marketType=bar.Market.STOCK, maxLen=None)

    resampleFeed.run(stopCount=10, _print=True)
    feed = AdvancedFeed(feedDict={'base': panelFeed, 'resample': resampleFeed}, panelDict={'bench': dataPanel})

    feed.run(100, _print=True)
    print()
