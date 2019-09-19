# -*- coding: utf-8 -*-
'''
@Time    : 2019/7/28 8:50
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
import os
import datetime

from cpa.io import BaseWriter
from cpa.config import pathSelector
from cpa.utils import logger
from cpa.config import const
from cpa.factorProcessor import factorTest


class H5PanelWriter(BaseWriter):
    '''
    因子计算及检测数据h5文件写入接口
    @Time: 2019/8/10 20:30
    @Author: Yanggang Fang
    说明：用来写因子检测数据的，有两种写新的和续写两种模式，用cpa.factorPool.factorUpdate里面的相应函数调用。
    '''

    logger = logger.getLogger("H5PanelWriter")

    def __init__(self, defaultFactorTest, factorName):
        '''
        初始化
        param defaultFactorTest: factorTest.py下的DefaultFactorTest类对象
        param factorName: 因子名
        '''
        self.defaultFactorTest = defaultFactorTest
        self.testReportGenerator = factorTest.TestReportGenerator(defaultFactorTest=self.defaultFactorTest,
                                                                  h5BatchPanelReader=None)
        # self.h5BatchPanelReader = h5BatchPanelReader

        # if self.defaultFactorTest and self.h5BatchPanelReader:
        #     self.testReportGenerator = factorTest.TestReportGenerator(defaultFactorTest=None,
        #                                                               h5BatchPanelReader=self.h5BatchPanelReader)
        # elif self.defaultFactorTest and not self.h5BatchPanelReader:
        #     self.testReportGenerator = factorTest.TestReportGenerator(defaultFactorTest=self.defaultFactorTest,
        #                                                               h5BatchPanelReader=None)

        self.frequency = defaultFactorTest.frequency
        self.factorName = factorName
        self.count = 0
        self.name = self.__class__.__name__

    def getDir(self):
        '''
        使用pathSelector生成路径，此类不需要传参文件路径
        '''
        pass

    def write(self, mode, oldResultDict=None):
        '''
        写入函数
        param mode: 写入模式， "new" or "append"
        param oldResultDict: 存储旧h5文件数据的字典，由h5PanelReader生成
        '''
        # 存储路径命名
        currentDT = datetime.datetime.now()
        factorFolderPath = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                                       factorFrequency=const.DataFrequency.freq2lable(self.frequency))  # 因子文件夹路径
        calFileName = self.factorName + "_factor_" +\
                      const.DataFrequency.freq2lable(self.frequency) +\
                      currentDT.strftime("_%Y%m%d_%H%M") + ".h5"  # 因子计算值文件名
        calFilePath = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                                  factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                                                  fileName=calFileName)

        # 写入新h5文件
        if mode == "new":
            # 因子计算数据存储
            self.testReportGenerator.defaultFactorTest.factorPanel.to_frame().to_hdf(path_or_buf=calFilePath,  # 使用pandas存储h5文件
                                                                 key=self.factorName,
                                                                 format="table",
                                                                 data_columns=True,
                                                                 mode="w")
            self.logger.info("The file {} has been saved".format(calFileName))

            # 因子检测数据存储
            indicatorDict = self.testReportGenerator.defaultFactorTest.getIndicators()  # 取包含因子检测对象的字典
            for key, value in indicatorDict.items():
                testFileName = self.factorName + "_" + key + "_" +\
                               const.DataFrequency.freq2lable(self.frequency) +\
                               currentDT.strftime("_%Y%m%d_%H%M") + ".h5"  # 因子检测数据文件名
                testFilePath = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                                           factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                                                           fileName=testFileName)  # 因子检测数据文件路径
                if indicatorDict[key].__len__():  # 当存储因子检测值的series不为空时进行存储
                    if key in ['groupRet', 'IC', 'rankIC', 'turn', 'cost', "groupNumber"]:
                        value.to_frame().to_hdf(path_or_buf=testFilePath,
                                                key=key,
                                                format="table",
                                                data_columns=True,
                                                mode="w")
                    else:
                        value.to_series().to_hdf(path_or_buf=testFilePath,
                                                 key=key,
                                                 format="table",
                                                 data_columns=True,
                                                 mode="w")
                    self.logger.info("The file {} has been saved".format(testFileName))
                else:  # 当存储因子检测值的series为空时，不进行存储，并记入日志
                    self.logger.info("The calculation of {} failed".format(key))

            # 储存分层收益图
            figName = self.factorName + '_Report_' +\
                      const.DataFrequency.freq2lable(self.frequency) + '.png'
            path = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                               factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                                               fileName=figName)
            self.testReportGenerator.plotGroupret(_show=False, path=path)
            # 储存分层统计量
            statisticFileName = self.factorName + '_Statistic_' +\
                                const.DataFrequency.freq2lable(self.frequency) + '.xls'
            self.testReportGenerator.statistic(path=pathSelector.PathSelector.getFactorFilePath(
                                             factorName=self.factorName,  # 因子计算数据的文件路径
                                             factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                             fileName=statisticFileName))

        # 续写h5文件
        elif mode == "append":
            # 因子计算数据存储
            for key in oldResultDict.keys():
                if "factor" in key:
                    oldCalFileName = key
            appendDataFrame = self.testReportGenerator.defaultFactorTest.factorPanel.to_frame()  # 新生成的因子计算值dataframe
            # 当新dataframe的最早时间晚于旧dataframe的最早时间并早于旧dataframe的最晚时间才进行拼接
            if appendDataFrame.index[0] > oldResultDict[oldCalFileName].index[0] \
                    and appendDataFrame.index[0] < oldResultDict[oldCalFileName].index[-1]:
                newDataFrame = oldResultDict[oldCalFileName].append(appendDataFrame)
                newDataFrame = newDataFrame.loc[~newDataFrame.index.duplicated(keep="first")]  # 删除时间重复的数据
                newDataFrame.to_hdf(path_or_buf=calFilePath,  # 使用pandas存储h5文件
                                    key=self.factorName,
                                    format="table",
                                    data_columns=True,
                                    mode="w")
                self.count += 1
                self.logger.info("The file {} has been saved".format(calFileName))
            else:
                self.logger.info("The earliest time of {} is before the one of {}. "
                                 "The new dataframe will not be appended.".format(calFileName, oldCalFileName))

            # 因子检测数据存储
            indicatorDict = self.testReportGenerator.defaultFactorTest.getIndicators()
            for key, value in indicatorDict.items():  # 遍历新生成的检测数据
                testFileName = self.factorName + "_" + key + "_" +\
                               const.DataFrequency.freq2lable(self.frequency) +\
                               currentDT.strftime("_%Y%m%d_%H%M") + ".h5" # 命名因子检测数据文件
                testFilePath = os.path.join(factorFolderPath, testFileName)
                if indicatorDict[key].__len__():  # 当存储因子检测值不为空时进行存储
                    appendData = value.to_frame() if key in ['groupRet', 'IC', 'rankIC', 'turn', 'cost', 'groupNumber']\
                                    else value.to_series()  # 生成新因子检测值df或者series
                    for key, value in oldResultDict.items():  # 提取存放旧h5数据的字典
                        # 检测新因子文件名是否与旧因子文件名相同
                        if "".join(testFileName.split("_")[0:2]) == "".join(key.split("_")[0:2]):
                            # 当新data的最早时间晚于旧data的最早时间并早于旧data的最晚时间才进行拼接
                            if appendData.index[0] > oldResultDict[key].index[0] \
                                    and appendData.index[0] < oldResultDict[key].index[-1]:
                                newData = oldResultDict[key].append(appendData)
                                newData = newData.loc[~newData.index.duplicated(keep="first")]
                                newData.to_hdf(path_or_buf=testFilePath,
                                                 key=key,
                                                 format="table",
                                                 data_columns=True,
                                                 mode="w")
                                self.count += 1
                                self.logger.info("The file {} has been saved".format(testFileName))
                            else:
                                self.logger.info("The earliest time of {} is before the one of {}. "
                                                 "The new series will not be appended.".format(testFileName, key))

                else:  # 当存储因子检测值的series为空时，不进行存储，并记入日志
                    self.logger.info("The calculation of {} failed".format(key))

            # 储存分层收益图
            figName = self.factorName + '_Report_' +\
                      const.DataFrequency.freq2lable(self.frequency) + '.png'
            path = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                               factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                                               fileName=figName)
            self.testReportGenerator.plotGroupret(_show=False, path=path)
            # 储存分层统计量
            statisticFileName = self.factorName + '_Statistic_' +\
                                const.DataFrequency.freq2lable(self.frequency) + '.xls'
            self.testReportGenerator.statistic(path=pathSelector.PathSelector.getFactorFilePath(
                                             factorName=self.factorName,  # 因子计算数据的文件路径
                                             factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                             fileName=statisticFileName))

        else:
            raise ValueError("An argument except 'new' or 'append' was passed into the write() function for the mode")
