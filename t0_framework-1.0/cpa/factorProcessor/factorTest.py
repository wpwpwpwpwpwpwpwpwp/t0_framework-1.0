"""
因子检验模块
注意检验因子方法有：
    - 1 return[-1]与factor[-lag]关系
    - 2 return[-lag:]与factor[-lag]关系
    - 3 return[-lag:]与factor[-2lag:-lag]关系 （第一种特殊情况，周期不同）
此处为第一种方法。
"""
import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd

from cpa.calculator import sectionCalculator
from cpa.feed import baseFeed
from cpa.utils import series
from cpa.utils import logger
from cpa.utils import bar

warnings.filterwarnings('ignore')
#plt.style.use('ggplot')


class DefaultFactorTest:
    """
    默认因子检验模块
    """
    logger = logger.getLogger("DefaultFactorTest")

    def __init__(self, panelFeed, factorPanel, returnPanel, lag=1, indicators=None,
                 nGroup=10, cut=0.1, fee=0.003):
        '''
        :param frequency:
        :param indicators: 需要计算的指标List，indicators = ['IC', 'rankIC', 'beta', 'gpIC', 'tbdf']
        :param ngroup: 分成ngroup组
        :param cut: 分组信息，0.1代表前10 % -后10 %
        :param fee: 开仓手续费，用于计算交易成本
        '''

        self.frequency = panelFeed.frequency
        self.panelFeed = panelFeed

        self.factorPanel = factorPanel
        self.returnPanel = returnPanel
        self.indicators = indicators
        self.lag = 1
        self.nGroup = nGroup
        self.cut = cut
        self.fee=fee
        self.maxLen = panelFeed.dataSource.getDataShape()[0]

        # 需要计算的指标
        self.betaSeries = series.SequenceDataSeries(self.maxLen)
        self.gpICSeries = series.SequenceDataSeries(self.maxLen)
        self.tbdfSeries = series.SequenceDataSeries(self.maxLen)  # top组平均收益-bottom组平均收益
        self.turnSeries = series.SequenceDataSeries(self.maxLen)


        self.ICPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.rankICPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.groupRetPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.turnPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.costPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.groupNumberPanel = series.SequenceDataPanel(['Group%s' % i for i in range(1, self.nGroup + 1)], self.maxLen)
        self.groupingCodesDict = {}
        self.topCodes = {}
        self.botCodes = {}
        self.poolNum=0

        # 注册回调事件，优先级为因子计算
        self.panelFeed.getNewPanelsEvent(priority=baseFeed.PanelFeed.EventPriority.FACTOR).subscribe(
            self.updateIndicators)

        self.obervedRank = []  # 存储当期交易的股票收益

    def updateIndicators(self, panelFeed, dateTime, df):
        '''
        :param scorePanel: 分值矩阵
        :param returnPanel: 收益矩阵
        :return:因子效果评价指标，IC,rankIC,beta,gpIC,tbdf
        '''
        if len(self.returnPanel) > 2 * self.lag:  # 前期数据过短时，return值不完善，比return向前lag期因子值也不完善，因此定为2 * self.lag
            self.dateTime = dateTime

            '''对缺失值进行处理'''
            thisAllReturn = self.returnPanel[-1, :]  # 当期所有数据
            lastAllFactor = self.factorPanel[-self.lag - 1, :]  # 向前lag期因子
            returnNotNan = np.argwhere(1 - np.isnan(thisAllReturn))  # 找出非nan值的位置
            factorNotNan = np.argwhere(1 - np.isnan(lastAllFactor))
            # 股票因子数量小于2无法分层
            if factorNotNan.__len__() < 1:
                return
            notNanLocate = np.intersect1d(returnNotNan, factorNotNan)  # 求两者交集
            self.thisReturn = np.nan_to_num(thisAllReturn[notNanLocate].reshape((len(notNanLocate),)))  # 当期去除nan后的数据
            self.lastFactor = np.nan_to_num(lastAllFactor[notNanLocate].reshape((len(notNanLocate),)))


            '''指标计算'''
            if 'IC' in self.indicators:
                ICPanel = sectionCalculator.ICGrouping(self.lastFactor, self.thisReturn,self.nGroup)
                self.ICPanel.appendWithDateTime(dateTime, ICPanel)

            if 'rankIC' in self.indicators:
                rankICPanel = sectionCalculator.RankICGrouping(self.lastFactor, self.thisReturn, self.nGroup)
                self.rankICPanel.appendWithDateTime(dateTime, rankICPanel)

            if 'gpIC' in self.indicators:
                gpIC = sectionCalculator.GPIC(self.lastFactor, self.thisReturn, self.nGroup)
                if 1 - np.isnan(gpIC):
                    self.gpICSeries.appendWithDateTime(self.dateTime, gpIC)  # 分n组后的相关系数

            if 'beta' in self.indicators:
                beta = sectionCalculator.BETA(self.lastFactor, self.thisReturn)
                if 1 - np.isnan(beta):
                    self.betaSeries.appendWithDateTime(self.dateTime, beta)  # 单因子回归斜率

            if 'tbdf' in self.indicators:
                tbdf = sectionCalculator.TBDF(self.lastFactor, self.thisReturn, self.cut)
                if 1 - np.isnan(tbdf):
                    self.tbdfSeries.appendWithDateTime(self.dateTime, tbdf)  # top平均收益 - bottom平均收益

            if 'turn' in self.indicators:
                # 把分组的信息，写入groupingCodes
                totalLength=len(self.panelFeed.getInstruments())
                if self.poolNum==0:
                    self.poolNum=totalLength   #记录资产池中的总资产数量
                groupResult = sectionCalculator.Grouping(self.lastFactor, self.nGroup) #返回的groupResult中每个元素的长度，是该时间截面
                                                                                        # 非空的股票的长度
                self.groupNumberPanel.appendWithDateTime(self.dateTime, [sum(i) for i in groupResult])  # 记录每一组该时间截面的持仓数量
                #print ('groupResult:',groupResult)
                #print(np.array(self.panelFeed.getInstruments()))
                adjustedGroupResult=sectionCalculator.adjustShape(groupResult,
                                                                  len(np.array(self.panelFeed.getInstruments())),
                                                                  notNanLocate)  # 调用adjustShape函数，使每个元素的长度，和去除缺失值前一致
                #print (adjustedGroupResult)
                groupCodeSlice = [list(np.array(self.panelFeed.getInstruments())[i]) for i in adjustedGroupResult] #获取每个组对应的股票代码
                self.groupingCodesDict[self.dateTime]= groupCodeSlice  #把它写入self.groupingCodesDict这个字典

                #如果是第一个，我们先把换手率设为100%（为了正确计算交易费用，使交易费用的panel和ret的panel形状相同）
                # 在最后求平均换手率时，把第一次的100%换手率剔除
                if len(self.groupingCodesDict)==1:
                    turnGroup=[1]*self.nGroup
                    self.turnPanel.appendWithDateTime(self.dateTime, turnGroup)
                    # 根据turnover rate 计算交易成本
                    cost = list(np.array(turnGroup) * self.fee)
                    self.costPanel.appendWithDateTime(self.dateTime, cost)
                #在第二个及之后，调用TurnGrouping函数，计算换手率
                if len(self.groupingCodesDict) >= 2:
                    lastGrouping = self.groupingCodesDict[self.returnPanel.getDateTimes()[-2]]
                    thisGrouping = self.groupingCodesDict[self.dateTime]
                    turnGroup = sectionCalculator.TurnGrouping(lastGrouping, thisGrouping, self.nGroup,)
                    self.turnPanel.appendWithDateTime(self.dateTime, turnGroup)
                    #根据turnover rate 计算交易成本
                    cost=list(np.array(turnGroup)*self.fee)
                    self.costPanel.appendWithDateTime(self.dateTime,cost)


            if 'groupRet' in self.indicators:
                groupRetPanel = sectionCalculator.GroupRet(self.lastFactor, self.thisReturn, self.nGroup)
                self.groupRetPanel.appendWithDateTime(dateTime, list(np.array(groupRetPanel)-np.array(cost)))

    def getIndicators(self):
        '''获取所需要的检测指标及相应对象，存入字典中'''
        self.indicatorDict = {}
        for indicator in self.indicators:
            if indicator in ['groupRet','IC','rankIC','turn']:
                indicatorObjName = indicator+"Panel"
            else:
                indicatorObjName = indicator + "Series"
            indicatorObj = getattr(self, indicatorObjName)
            self.indicatorDict[indicator] = indicatorObj
            if indicator == 'turn':
                self.indicatorDict['cost'] = getattr(self, 'costPanel')
                self.indicatorDict['groupNumber'] = getattr(self, "groupNumberPanel")
        return self.indicatorDict


class TestReportGenerator:
    '''
    生成检测报告的图和表
    '''
    logger.getLogger("TestReportGenerator")

    def __init__(self, defaultFactorTest, h5BatchPanelReader):
        '''
        当写新的因子时，传入因子检测类直接用因子检测类中的panel作图作表
        当续写因子时，传入h5BatchPanelReader类，通过reader读取的数据作图作表
        param defaultFactorTest: 因子检测类
        param h5BatchPanelReader: h5文件读取类
        '''
        self.defaultFactorTest = defaultFactorTest
        self.h5BatchPanelReader = h5BatchPanelReader

        if self.defaultFactorTest and self.h5BatchPanelReader:
            self.logger.info("Either defaultFactorTest or h5BatchPanelReader must be None")
            return

        if self.defaultFactorTest:
            self.groupRet = self.defaultFactorTest.groupRetPanel.to_frame()
            self.IC = self.defaultFactorTest.ICPanel.to_frame()
            self.rankIC = self.defaultFactorTest.rankICPanel.to_frame()
            self.turn = self.defaultFactorTest.turnPanel.to_frame()
            self.cost = self.defaultFactorTest.costPanel.to_frame()
            self.groupNumber = self.defaultFactorTest.groupNumberPanel.to_frame()

        elif self.h5BatchPanelReader:
            for key, value in self.h5BatchPanelReader.staticPanelDict.items():
                indicator = key.split("_")[1]
                if indicator == "groupRet":
                    self.groupRet = value.to_frame()
                if indicator == "IC":
                    self.IC = value.to_frame()
                if indicator == "rankIC":
                    self.rankIC = value.to_frame()
                if indicator == "turn":
                    self.turn = value.to_frame()
                if indicator == "cost":
                    self.cost = value.to_frame()
                if indicator == "groupNumber":
                    self.groupNumber = value.to_frame()

        self.turn.index = [str(i) for i in self.turn.index.values]
        self.cost.index = [str(i) for i in self.cost.index.values]
        self.groupRet.index = [str(i) for i in self.groupRet.index.values]
        self.IC.index = [str(i) for i in self.IC.index.values]
        self.rankIC.index = [str(i) for i in self.rankIC.index.values]
        self.groupNumber.index = [str(i) for i in self.groupNumber.index.values]

    def plotGroupret(self, _show=True, path=None):
        '''绘制分层收益'''
        #设置画布大小，把画布分成4块
        plt.figure(figsize=(24, 12))

        #第一个子图画总收益，柱状图
        ax1 = plt.subplot(221)
        self.groupRet.sum().plot(kind='bar', ax=ax1, rot=20)
        plt.title('GroupRet', fontsize=20)
        plt.tight_layout()

        #第二个子图画分组的收益走势图
        ax2 = plt.subplot(223)
        self.groupRet.cumsum().plot(ax=ax2, rot=15)
        plt.legend(loc='upper left', fontsize=10)
        plt.title('CumRet', fontsize=20)
        plt.tight_layout()

        #画分组的累计IC走势
        ax3 = plt.subplot(222)
        self.IC.cumsum().plot( ax=ax3, rot=15)
        plt.title('IC_CUMSUM', fontsize=20)
        plt.legend(loc='upper left', fontsize=10)
        plt.tight_layout()

        #画分组的累计rankIC走势
        ax4 = plt.subplot(224)
        self.rankIC.cumsum().plot(ax=ax4, rot=15)
        plt.title('RankIC_CUMSUM', fontsize=20)
        plt.legend(loc='upper left', fontsize=10)
        plt.tight_layout()

        if _show:
            plt.show()
        else:
            pass

        if path!=None:
            plt.savefig(path)

    def plotGroupStat(self):

        self.groupRet.index = [str(i) for i in self.groupRet.index.values]

        ret = sectionCalculator.RET(self.groupRet)
        sharp = sectionCalculator.SHARP(self.groupRet, self.defaultFactorTest.frequency)
        maxdd = sectionCalculator.maxDrawDown(self.groupRet)

        statistic = pd.DataFrame(columns=self.groupRet.columns)
        statistic = statistic.append(ret)
        statistic = statistic.append(sharp)
        statistic = statistic.append(maxdd)
        statistic = statistic.round(3)

        print(statistic)

        plt.table(
            cellText=statistic.values,
            rowLabels=statistic.index,
            colLabels=statistic.columns,
        )
        plt.show()

    def statistic(self, path=None):
        '''
        :param path:
        :return: 把计算出的数据储存为excel
        '''
        #计算换手率和交易成本
        turn = sectionCalculator.MeanTurn(self.turn)
        cost = sectionCalculator.SumCost(self.cost)

        #分别计算ret,sharp,maxdd
        ret = sectionCalculator.RET(self.groupRet)
        sharp = sectionCalculator.SHARP(self.groupRet, self.defaultFactorTest.frequency)
        maxdd = sectionCalculator.maxDrawDown(self.groupRet)

        #提取IC和rankIC
        ic = sectionCalculator.MeanIC(self.IC)
        rankIc = sectionCalculator.MeanRankIC(self.rankIC)

        #储存每组的平均持股数量
        number = sectionCalculator.MeanNumber(self.groupNumber)

        #储存lag和费率
        lag = pd.Series([self.defaultFactorTest.lag] + [np.nan] * (len(self.groupRet.columns) - 1), index=self.groupRet.columns, name='lag')
        fee = pd.Series([self.defaultFactorTest.fee] + [np.nan] * (len(self.groupRet.columns) - 1), index=self.groupRet.columns, name='fee')
        pool = pd.Series([self.defaultFactorTest.poolNum] + [np.nan] * (len(self.groupRet.columns) - 1), index=self.groupRet.columns, name='pool')

        #写入dataframe
        statistic = pd.DataFrame(columns=self.groupRet.columns)
        appendList = [ret, sharp, maxdd, ic, rankIc, turn, cost, number, lag, fee, pool]
        for item in appendList:
            statistic = statistic.append(item)

        #存为excel
        if path != None:
            statistic.to_excel(path)

    def plotSumCurve(self, testSeries):
        testData = testSeries[:]
        allTimeList = testSeries.getDateTimes()
        fig, ax = plt.subplots(1, 1)
        ax.plot(np.array(testData).cumsum(), label='cumsum')
        plt.legend(loc='best')

        def format_date(x, pos=None):  # 改变横坐标格式
            if x < 0 or x > len(allTimeList) - 1:
                return ''
            else:
                return allTimeList[int(x)]

        ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))  # 将横坐标设置为日期
        fig.show()

    def plotProdCurve(self, testSeries):
        testData = testSeries[:]
        allTimeList = testSeries.getDateTimes()
        testData = 1 + testData
        fig, ax = plt.subplots(1, 1)
        ax.plot(np.array(testData).cumprod(), label='prod')
        plt.legend(loc='best')

        def format_date(x, pos=None):  # 改变横坐标格式
            if x < 0 or x > len(allTimeList) - 1:
                return ''
            else:
                return allTimeList[int(x)]

        ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
        fig.show()


if __name__ == '__main__':
    from cpa.factorPool.factors import maPanelFactor
    from cpa.factorModel import factorBase
    from cpa.indicators.panelIndicators import returns
    from cpa.feed.feedFactory import InlineDataSet
    from cpa.resample.resampled import ResampledPanelFeed

    '''原始1min数据回测'''
    # panelFeed = InlineDataSet.SZ50_MINUTE()
    # rawFactor = factorBase.FactorPanel(panelFeed, maPanelFactor.Factor, 1024)  # panel形式
    # F = 30  # 调仓频率
    # _return = returns.Returns(panelFeed, lag=F, maxLen=1024)  # 以开盘价计算的向前n期收益
    # factorTester = DefaultFactorTest(panelFeed, rawFactor, _return,
    #                                  indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf', 'turn'],
    #                                  lag=F,
    #                                  nGroup=10, cut=0.1)  # 定义因子评价类
    # panelFeed.run(300)
    #
    # factorTester.plotAll()
    # factorTester.plotGroupret()
    # # factorTester.plotGroupStat()

    '''resample数据回测'''
    panelFeed = InlineDataSet.SZ50_MINUTE()
    resampleFeed = ResampledPanelFeed(panelFeed, bar.Frequency.HOUR)
    rawFactor = factorBase.FactorPanel(resampleFeed, maPanelFactor.Factor, 1024)  # panel形式
    F = 1  # 调仓频率
    _return = returns.Returns(resampleFeed, lag=F, maxLen=1024)  # 以开盘价计算的向前n期收益
    factorTester = DefaultFactorTest(resampleFeed, rawFactor, _return,
                                     indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf', 'turn', 'groupRet'],
                                     lag=F,
                                     nGroup=10, cut=0.1)  # 定义因子评价类
    resampleFeed.run(3000)

    # factorTester.plotAll()
    # factorTester.plotGroupret()
    factorTester.statistic()
    # factorTester.plotGroupStat()
