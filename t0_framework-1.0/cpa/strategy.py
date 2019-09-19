'''
策略入口函数，因子预处理、计算、标准化、截面计算、持久化、作图、回测等
@Time    : 2019/6/10 21:24
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
import numpy as np

from cpa.factorProcessor import feedFilter
from cpa.factorProcessor import factorTest
from cpa.factorProcessor import factorNormalizer
from cpa.indicators.panelIndicators import returns
from cpa.factorModel import factorBase


class Strategy:
    '''
    factory method
    '''
    def __init__(self):
        self.rawPanelFeed = None
        self.filterdFeed = None

        self.factorPanel = None
        self.normalizer = None

        self.returns = None
        self.sectionAnalyser = None
        self.lagBar = np.nan  #调仓（预测）频率,以bar计，计算截面、收益率等需要使用

        self.plotter = None
        self.dataSaver = None
        self.backtest = None

    def attachInputPanelFeed(self, inputFeed):
        '''
        :param inputFeed:设置输入数据流
        :return:
        '''
        assert inputFeed is not None
        self.rawPanelFeed = self.filterdFeed = inputFeed
        return self

    def attachFilter(self, filter=None):
        '''
        :param filter: feed过滤器，过滤无效的feed
        :return:
        '''
        self.filterdFeed = feedFilter.DefaultFeedFilter(self.rawPanelFeed, feedFilter.DefaultFeedFilter.FilterMode.FILTER_NOTHING)
        return self

    def attachCalculator(self, factorCalculatorCls, factorMaxLen=None):
        '''
        :param factorCalculatorCls: 设置因子计算类
        :param factorFeedMaxLen: 因子存储最大值
        :return:
        '''
        inputFeed = self.filterdFeed if self.filterdFeed is not None else self.rawPanelFeed
        assert inputFeed is not None
        self.factorPanel = factorBase.FactorPanel(inputFeed, factorCalculatorCls, factorMaxLen)
        return self

    def attachNormalizer(self, normType=factorNormalizer.NormalizedFeed.NormalizedType.RAW):
        '''
        :param factorNormalizer: 因子标准化(默认使用原值)
        :return:
        '''
        self.factorPanel.attachFactorNormalizer(normType)
        return self


    def attachSectionAnalyser(self, lagBar=1, ngroup=10, cut=0.1, maxLen=1024):
        '''
        :param lagBar: 收益序列取lag, lag至少为1, 即取上一个score同当前收益取截面, 前lag期因子值对应当期收益
        :param ngroup: 分成ngroup组，分ngroup组后的相关系数
        :param cut: 分组信息，0.1代表前10 % -后10 %
        :param maxLen:
        :return:
        '''
        self.returns = returns.Returns(self.filterdFeed, lag=lagBar, maxLen=maxLen)
        self.sectionAnalyser = factorTest.DefaultFactorTest(self.filterdFeed,
                                                            self.factorPanel,
                                                            self.returns,
                                                            indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf'],
                                                            lag=lagBar,
                                                            nGroup=ngroup,
                                                            cut=cut)  # 定义因子评价类
        return self

    def attachPlotter(self):
        '''
        :return:封装同一作图接口
        '''
        self.plotter = None
        raise NotImplementedError

    def attachDataSaver(self):
        '''
        :return:数据持久化对象
        '''
        self.dataSaver = None
        raise NotImplementedError

    def attachBackTest(self, candicate, broker):
        '''
        :return:回测模块
        '''
        raise NotImplementedError


    def run(self, stopCount=None):
        '''
        :param stopCount:
        :return:函数运行
        '''
        self.rawPanelFeed.run(stopCount)


if __name__ == '__main__':
    from cpa.feed import feedFactory
    from cpa.factorPool.factors import maPanelFactor
    f = (Strategy().attachInputPanelFeed(feedFactory.InlineDataSet.ZZ500_MINUTE())
                    .attachCalculator(maPanelFactor.Factor)
                    .attachNormalizer(factorNormalizer.NormalizedFeed.NormalizedType.ZSCORE)
                    .attachSectionAnalyser(lagBar=1)
         )

    f.run(1000)

    f.sectionAnalyser.plotAll()

