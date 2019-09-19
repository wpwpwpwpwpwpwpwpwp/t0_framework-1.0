# -*- coding: utf-8 -*-
import numpy as np

from cpa.utils.series import SequenceDataPanel


class Returns(SequenceDataPanel):
    '''
    - 不可对panel赋值,若赋值须copy一份
    '''

    def __init__(self, panelFeed, lag, maxLen):
        super().__init__(panelFeed.closePanel.getColumnNames(), maxLen=maxLen)
        panelFeed.getNewPanelsEvent(priority=panelFeed.EventPriority.INDICATOR).subscribe(self.onNewValues)
        self.lag = lag

    def onNewValues(self, panelFeed, dateTime, df):
        '''
        :param ReturnValue: 存储数据
        :return:最新的一行值
        '''
        panelData = panelFeed.getClosePanel()
        if panelData.__len__() < self.lag + 1:
            self.appendWithDateTime(
                dateTime,
                np.full(
                    panelData.shape[1],
                    np.nan))  # 若数据量不足，返回nan值
        else:
            _return = panelData[-1, :] / panelData[-self.lag - 1, :] - 1
            self.appendWithDateTime(dateTime, _return)
        return self


class AbsoluteReturns(SequenceDataPanel):
    '''
    描述：计算个股相对于股指期货的超额收益（多个股，空股指期货）
        - 以收盘价到收盘价计算
    '''

    def __init__(self, advancedFeed, lag, maxLen):
        super().__init__(advancedFeed.closePanel.getColumnNames(), maxLen=maxLen)
        self.lag = lag
        self.advancedFeed = advancedFeed

        advancedFeed.getNewPanelsEvent(priority=advancedFeed.EventPriority.INDICATOR).subscribe(self.onNewValues)

    def onNewValues(self, advancedFeed, dateTime, df=None):
        if advancedFeed.closePanel.__len__() < self.lag + 1:
            self.appendWithDateTime(dateTime, np.full(advancedFeed.closePanel.shape[1], np.nan))  # 若数据量不足，返回nan值
        else:
            stockReturn = advancedFeed.closePanel[-1, :] / advancedFeed.closePanel[-self.lag - 1, :] - 1
            # 找到指数close对应的index
            benckCloseIndex = np.where(advancedFeed.benchPanel.getColumnNames() == 'close')[0][0]
            # 用股指close计算指数收益
            indexReturn = advancedFeed.benchPanel[-1, benckCloseIndex] / advancedFeed.benchPanel[-self.lag - 1, benckCloseIndex] - 1
            _return = stockReturn - indexReturn
            self.appendWithDateTime(dateTime, _return)
        return self


if __name__ == '__main__':
    from cpa.feed.feedFactory import InlineDataSet
    from cpa.io.csvReader import CSVPanelReader
    from cpa.feed.baseFeed import AdvancedFeed
    from cpa.utils import series
    from cpa.utils import bar

    '''Returns测试用例'''
    # panelFeed = InlineDataSet.SZ50_MINUTE()
    # _returns = Returns(panelFeed, lag=30, maxLen=1024)  # 以开盘价计算的向前n期收益,定义returns类
    # panelFeed.run(1000)
    # print(_returns.to_frame())

    '''AbsoluteReturns测试用例'''
    # Data panelFeed
    panelFeed = InlineDataSet.ZZ500_MINUTE()
    # Index Feed
    path = r"D:\PythonProject\PycharmProjects\t0_frameWork\cpa\data"
    fileName = 'IC.CCFX.csv'
    indexReader = CSVPanelReader(
        path=path,
        fileName=fileName,
        frequency=bar.Frequency.MINUTE,
        isInstrmentCol=False,
        start='20150601')
    indexReader.loads()
    indexFeed = series.SequenceDataPanel.from_reader(indexReader)
    # AdvanceFeed
    advanceFeed = AdvancedFeed(feedDict={'base': panelFeed}, panelDict={'bench': indexFeed})

    # 以开盘价计算的向前n期收益,定义returns类
    _returns = AbsoluteReturns(advanceFeed, lag=1, maxLen=1024)

    advanceFeed.run(1000, _print=True)
    print(_returns.to_frame())
