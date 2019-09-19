# -*- coding: utf-8 -*-
'''
1.原有的resample是在该K线结束后，下一根k线才开始得到上一轮的结果，这样的话会慢1根k线，它那么做的原因是不知道下一根k线何时
结束，此处我们重写了resampleBase，如果k线正好是15:00，则在15：00时刻就生成重采样的k线，如果数据是不连续的，即14:46一根，
9:01一根，则在15：00通过checkNow来生成重采样的K线
2.同时增加一个14:55分的事件，取消15:00事件
3.原作者所用的数据序列是9：30代表9：30分至9：31的，而国内主流数据9：30至9：31使用的标示符是9：31，因而
此处重采样也是和国内的相同，使用ending的时间来计算
4.5分钟级别的重采样不用设置nearlyMarkektEnding和updateValueEvent，即就是正常的5分钟一走。只有15分钟以上的级别才会考虑这些问题

@Time    : 2019/2/21 23:10
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''

from pyalgotrade import observer
import numpy as np

from cpa.utils import bar
from cpa.resample import resamplebase
from cpa.feed import baseFeed
from cpa.config import const
from cpa.utils import logger

class ResampleState:
    # 许多时候需要延后一个bar进行处理
    READY = 0
    LAG_ONE = 1
    UPDATE_OK = 2
    NOTHING = None


class PanelGrouper:

    def __init__(self, panelFeed):
        self.panelFeed = panelFeed
        self.counter = 0

    def addCounter(self):
        self.counter += 1

    def resetCounter(self):
        self.counter = 0

    def getGrouped(self, end=None):
        '''
        :param end: None 或 -1,, -1标示保留最后一个
        :return:
        '''
        ret = {}
        _slice = slice(-self.counter, end, None)
        end = -1 if end is None else - 1 + end

        dateTime = self.panelFeed.closePanel.getDateTimes()[end]
        ret['open'] = self.panelFeed.openPanel[-self.counter, :]
        ret['high'] = self.panelFeed.highPanel[_slice, :].max(axis=0)
        ret['low'] = self.panelFeed.lowPanel[_slice, :].min(axis=0)
        ret['close'] = self.panelFeed.closePanel[end, :]
        ret['volume'] = self.panelFeed.volumePanel[_slice, :].sum(axis=0)
        return dateTime, ret


class ResampledPanelFeed(baseFeed.PanelFeed):
    """
    market is in cpa.resamplebase.Market.STOCK or cpa.resamplebase.Market.CTP
    """
    logger = logger.getLogger('resampleFeed')
    def __init__(self,panelFeed, frequency, marketType=bar.Market.STOCK, maxLen=None):
        '''
        :param panelFeed:
        :param resampleTo: resamplebase.Frequency.(MINUT HOUR DAY WEEK MONTH)
        :param maxLen:
        '''
        if not isinstance(panelFeed, baseFeed.PanelFeed):
            raise Exception("panelFeed must be a baseFeed.panelFeed instance")

        if maxLen is None:
            maxLen = panelFeed.maxLen

        super(ResampledPanelFeed,self).__init__(panelFeed.getDataSource(), panelFeed.getInstruments(), frequency, maxLen)
        self.dataSource = None  #reset dataSource
        self.market = marketType
        self.panelFeed = panelFeed
        self.grouper = PanelGrouper(panelFeed)
        self.isResampleFeed = True
        self.range = None
        self.__currentDateTime = np.nan

        self.__needUpdateResampleBar = ResampleState.NOTHING
        self.__laggedTime = None

        self.__nearlyEndingEvent = observer.Event()
        self.__updateValuesEvent = observer.Event()

        panelFeed.getNewPanelsEvent(priority=panelFeed.EventPriority.RESAMPLE).subscribe(self.__onNewValues)

    def isResampleFeed(self):
        return self.isResampleFeed

    def __onNewValues(self, panelFeed, dateTime, df):
        '''
        :param panelFeed:
        :param dateTime:
        :param df:
        :return:
        '''
        if self.range is None:
            self.range = resamplebase.build_range(
                dateTime, self.getFrequency(), market=self.market)
            self.grouper.addCounter()

        elif self.range.belongs(dateTime):
            self.grouper.addCounter()

            if self.getFrequency() >= bar.Frequency.MINUTE * 15 and self.range.nearlyMarketEnding(dateTime):
                if self.__laggedTime is None or dateTime.day != self.__laggedTime.day:
                    self.appendValues(dateTime.replace( hour=15, minute=0), self.grouper.getGrouped())
                    self.__needUpdateResampleBar = ResampleState.LAG_ONE
                    self.__laggedTime = dateTime

        elif not self.range.belongs(dateTime):
            self.grouper.addCounter()
            if self.range.outEnding(dateTime):
                grouped = self.grouper.getGrouped(end=-1)
                self.appendValues(dateTime, grouped)
                self.grouper.resetCounter()

                self.range = resamplebase.build_range(
                    dateTime, self.getFrequency(), market=self.market)

            else:
                self.range = None
                self.appendValues(dateTime, self.grouper.getGrouped())
                self.grouper.resetCounter()

    def appendValues(self, dateTime, grouped):
        '''
        :param dateTime: 当前时间
        :param grouped: grouped里面的时间，可能和当前时间不一样
        :return:
        '''

        if self.__needUpdateResampleBar == ResampleState.LAG_ONE:
            self.updateWithDateTime(dateTime, grouped)
            self.__needUpdateResampleBar = ResampleState.READY
        else:
            self.appendWithDateTime(dateTime, grouped)

    def appendWithDateTime(self, dateTime, grouped):
        self.__currentDateTime = dateTime
        groupedTime, grouped = grouped
        self.openPanel.appendWithDateTime(groupedTime, grouped['open'])
        self.highPanel.appendWithDateTime(groupedTime, grouped['high'])
        self.lowPanel.appendWithDateTime(groupedTime, grouped['low'])
        self.closePanel.appendWithDateTime(groupedTime, grouped['close'])
        self.volumePanel.appendWithDateTime(groupedTime, grouped['volume'])
        self.dispatchNewValueEvent(self, dateTime, None)
        self.logger.debug('ResampledTime %s: %s'.format(const.DataFrequency.freq2lable(self.frequency),
                                                        groupedTime.strftime('%Y-%m-%d %H:%M:%S')))

    def updateWithDateTime(self, dateTime, grouped):
        self.__currentDateTime = dateTime
        groupedTime, grouped = grouped
        self.openPanel.updateWithDateTime(groupedTime, grouped['open'])
        self.highPanel.updateWithDateTime(groupedTime, grouped['high'])
        self.lowPanel.updateWithDateTime(groupedTime, grouped['low'])
        self.closePanel.updateWithDateTime(groupedTime, grouped['close'])
        self.volumePanel.updateWithDateTime(groupedTime, grouped['volume'])

    def getCurrentDatetime(self):
        return self.__currentDateTime

    def getNearlyEndingEvent(self):
        return self.__nearlyEndingEvent

    def eof(self):
        return self.panelFeed.eof()

    def getNextValues(self):
        dateTime, df = self.panelFeed.getNextValues()
        return dateTime, df

if __name__ == '__main__':
    from cpa.feed.feedFactory import InlineDataSet
    panelFeed = InlineDataSet.ZZ500_MINUTE()
    resampleFeed = ResampledPanelFeed(panelFeed, bar.Frequency.HOUR2)

    resampleFeed.run(600)
