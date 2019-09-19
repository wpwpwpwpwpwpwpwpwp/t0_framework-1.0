#!/usr/bin/env Python
# -*- coding:utf-8 -*-
# author: Yanggang Fang

'''
factorUpdaate.py
描述：运行更新factora下因子数据到fatoterData下
'''

import os
import importlib

import pandas as pd
import numpy as np

from cpa.io import h5Writer
from cpa.io import h5Reader
from cpa.utils import logger
from cpa.utils import bar
from cpa.config import pathSelector
from cpa.config import const
from cpa.factorModel import factorBase
from cpa.indicators.panelIndicators import returns
from cpa.factorProcessor.factorTest import DefaultFactorTest
from cpa.feed.feedFactory import DataFeedFactory
from cpa.resample.resampled import ResampledPanelFeed


class FactorUpdate:
    """因子检测数据更新"""

    logger = logger.getLogger("factorUpdate")

    def __init__(self, instruments, market=bar.Market.STOCK, start=None,
                 end=None, dataFreq=bar.Frequency.MINUTE, testFreq=None, fee=0.003):
        '''
        初始化因子检测参数并读取股票文件
        param instruments: 代码 "SZ50", "HS300", or "ZZ500"
        param market: 市场 bar.Market.STOCK, or bar.Market.FUTURES
        param frequency: 数据频率 bar.Frequency.MINUTE or bar.Frequency.HOUR
        param start: 因子检测开始时间，当为空值时将使用H5DataReader的默认开始时间
        param end: 因子检测结束时间，当为空值时将使用H5DataReader的默认结束时间
        '''
        self.instruments = instruments
        self.market = market
        self.start = start
        self.end = end
        self.newFactor = []
        self.factorDefPath = pathSelector.PathSelector.getFactorDefPath()
        self.factorDataPath = pathSelector.PathSelector.getFactorFilePath()
        self.fee = fee
        self.dataFreq = dataFreq
        self.testFreq = [bar.Frequency.MINUTE5,
                         bar.Frequency.MINUTE30,
                         bar.Frequency.HOUR,
                         bar.Frequency.HOUR2] if not testFreq else testFreq #设置要回测的时间频率，默认测试 5，30,60,120分钟的

    def getPanelFeed(self):
        '''获取一个新的panelFeed'''
        panelFeed = DataFeedFactory.getHistFeed(instruments=self.instruments,
                                                market=self.market,
                                                frequency=bar.Frequency.MINUTE,
                                                start=self.start,
                                                end=self.end)
        return panelFeed

    def newFactorList(self):
        '''获取新增的因子列表'''
        allFactors = [factor.split('.')[0] for factor in os.listdir(self.factorDefPath) \
                      if factor not in ['__init__.py', '__pycache__']]
        # self.logger.info("All factors defined: {}".format(allFactors))
        self.newFactor = sorted(list(set(allFactors) - set(os.listdir(self.factorDataPath))))

        if self.newFactor:
            self.logger.info("The new factors:{}".format(self.newFactor))
        else:
            self.logger.info("No new factors seen, the factor updating process will end soon")

    def writeNewFactor(self, F=1):
        '''
        存储数据文件
        param F: 调仓频率
        '''
        self.newFactorList()
        if self.newFactor:  # 仅在有新增因子的情况下才进行后续的因子计算、检验及存储
            for factor in self.newFactor:  # 对新增因子列表里的因子进行计算和数据存储
                if factor == 'broker':
                    continue

                self.logger.info(
                    "************************ Writing FactorData for {} ************************".format(factor))
                modulePath = "cpa.factorPool.factors.{}".format(factor)  # 因子模块路径
                module = importlib.import_module(modulePath)
                self.logger.info("The module {} has been imported successfully".format(factor))

                panelFeed = self.getPanelFeed()  # 为新的因子匹配一个新的panelFeed

                reasampleFeedDict = {}  # 几个字典，分别储存相应时间频率的变量
                _return_Dict = {}
                factorObjectDict = {}
                rawFactorDict = {}
                factorTesterDict = {}

                for freq in self.testFreq:
                    reasampleFeedDict[freq] = ResampledPanelFeed(panelFeed, freq)
                    _return_Dict[freq] = returns.Returns(reasampleFeedDict[freq], lag=F, maxLen=1024)
                    factorObjectDict[freq] = getattr(module, 'Factor')
                    rawFactorDict[freq] = factorBase.FactorPanel(reasampleFeedDict[freq], factorObjectDict[freq])
                    factorTesterDict[freq] = DefaultFactorTest(reasampleFeedDict[freq], rawFactorDict[freq], _return_Dict[freq],
                                                          indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf', 'turn',
                                                                      'groupRet'],
                                                          lag=F, cut=0.1, fee=self.fee)
                panelFeed.run(2000)

                if len(_return_Dict[self.testFreq[0]]) <= 2 * F:  # 若数据长度不符合因子检验标准，则不存储
                    self.logger.warning(
                        "The length of the return panel <= 2 * the required lag. Data will not be saved.")
                    return

                for freq in self.testFreq:
                    h5PanelWriter = h5Writer.H5PanelWriter(factorTesterDict[freq], factor)
                    h5PanelWriter.write(mode="new")

    def updateFactor(self, factor, removeOld=True, F=1):
        '''
        续写一个因子文件夹下的所有文件
        param factor: 因子名
        param removeOld: 是否删除原有文件
        param F: 调仓频率
        '''
        self.logger.info("************************Updating FactorData for {}************************".format(factor))

        factorReader = h5Reader.H5BatchPanelReader(factorName=factor, frequency=None)
        factorReader.prepareOutputData()
        dateRangeDict = factorReader.getDateRange()  # 获取存放首尾数据日期的字典
        endDateList = sorted([range[1] for range in dateRangeDict.values()])  # 取所有的数据结束日期， 并排序
        firstEndTime = endDateList[0].to_pydatetime()  # 取所有数据结束日期中最早的一个
        timeDiff = pd.tseries.offsets.BusinessDay(n=np.floor(2*F*self.dataFreq/86400) + 1)  # 将2F转换成天数后+1
        self.start = firstEndTime - timeDiff  # 计算数据读取开始的时间
        panelFeed = self.getPanelFeed()  # 以新的start获取一个新的panelFeed

        modulePath = "cpa.factorPool.factors.{}".format(factor)  # 因子模块路径
        module = importlib.import_module(modulePath)  # 导入模块
        self.logger.info("The module {} has been imported successfully".format(factor))
        factorObject = getattr(module, 'Factor')  # 获取因子对象的名称 e.g. cpa.factorPool.factors.dmaEwv.Factor

        resampleFeedDict = {}
        returnDict = {}
        rawFactorDict = {}
        factorTesterDict = {}
        dictOldResultDict = {}
        dictFilePathDict = {}
        for resample in self.testFreq:
            frequencyStr = const.DataFrequency.freq2lable(resample)
            resampleReader = h5Reader.H5BatchPanelReader(factorName=factor,
                                                         frequency=frequencyStr)  # 读取文件夹内所有文件
            resampleReader.prepareOutputData()  # 存入相应的字典中
            oldResultDict = resampleReader.getTestResult()  # 获取存放dataframe数据的字典
            filePathDict = factorReader.getFilePath()  # 获取原来H5文件的路径

            key = str(resample).split(".")[-1]
            dictOldResultDict[key] = oldResultDict
            dictFilePathDict[key] = filePathDict
            resampleFeedDict[key] = ResampledPanelFeed(panelFeed, resample)
            returnDict[key] = returns.Returns(resampleFeedDict[key], lag=F, maxLen=1024)
            rawFactorDict[key] = factorBase.FactorPanel(resampleFeedDict[key], factorObject)
            factorTesterDict[key] = DefaultFactorTest(panelFeed=resampleFeedDict[key],
                                                 factorPanel=rawFactorDict[key],
                                                 returnPanel=returnDict[key],
                                                 indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf', 'turn', 'groupRet'],
                                                 lag=F,
                                                 cut=0.1)


        # rawFactor = factorBase.FactorPanel(panelFeed, factorObject)
        # factorTester = DefaultFactorTest(panelFeed, rawFactor, _return,
        #                                  indicators=['IC', 'rankIC', 'beta', 'gpIC', 'tbdf', 'turn'],
        #                                  lag=F,
        #                                  cut=0.1)
        panelFeed.run(2000)
        for key, oldResultDict in dictOldResultDict.items():
            h5PanelWriter = h5Writer.H5PanelWriter(factorTesterDict[key], factor)
            h5PanelWriter.write(mode="append", oldResultDict=oldResultDict)        # 使用append模式写入

        # for resample in self.testFreq:
        #     frequencyStr = const.DataFrequency.freq2lable(resample)
        #     secondReader = h5Reader.H5BatchPanelReader(factorName=factor,
        #                                                  frequency=frequencyStr)  # 读取文件夹内所有文件
        #     secondReader.prepareOutputData()  # 存入相应的字典中
        #     h5PanelWriter.writeRepStat(secondReader)
            # 默认在写入新文件之后删除原来的文件
            # if removeOld:
            #     if h5PanelWriter.count == len(oldResultDict):  # 当写入的文件数等于原来的文件数时，删除原来的文件
            #         for file in dictFilePathDict[key].values():
            #             os.remove(file)
            #     else:
            #         self.logger.info(
            #             "The number of the new written files does not equal to the number of the old")

    def updateFactorPool(self, removeOld=True):
        '''
        续写factorData下所有的因子文件夹
        param removeOld: 是否删除原有文件
        '''
        factorNameList = [name for name in os.listdir(self.factorDataPath) if  # 取factorData文件下的子文件夹名
                          os.path.isdir(os.path.join(self.factorDataPath, name))]
        for factor in factorNameList:
            self.updateFactor(factor, removeOld=removeOld, F=1)


if __name__ == "__main__":
    factorUpdate = FactorUpdate(instruments="SZ50", start="20170415", end="20171115")

    # 写因子Data
    factorUpdate.writeNewFactor()

    # 续写factorData下一个因子
    # factorUpdate.updateFactor("maPanelFactor")

    # 续写factorData下所有的因子文件夹
    # factorUpdate.updateFactorPool()
