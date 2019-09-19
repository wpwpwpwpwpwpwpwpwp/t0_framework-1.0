#!/usr/bin/env Python
# -*- coding:utf-8 -*-
# author: Yanggang Fang

'''
财务数据读取接口
@Time    : 2019/8/14 15:30
读取Wind生成的财务数据三大表
'''

import os
import datetime
import pandas as pd
import numpy as np
from cpa.io import BaseDataReader
from cpa.utils import logger
from cpa.utils import bar
from cpa.feed import baseFeed
from cpa.config import const, pathSelector


class FinanceReader(BaseDataReader):
    '''
    读取财务数据三大表
    '''
    logger = logger.getLogger('FinanceReader')

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
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    startDate = "20161117"
    # endDate = "20190818"
    fileName = "balancesheet"
    # fileName = "INCOME"
    instruments = ["600519", "601318", "600498"]
    fields = ["REPORT_PERIOD", "MONETARY_CAP"]
    # fields = ["REPORT_PERIOD", "OPER_REV"]

    financeReader = FinanceReader(fileName=fileName,
                                  instruments=instruments,
                                  fields=fields,
                                  start=startDate,
                                  end=None)
    df = financeReader.loads()
    print(df)

    financeReader.run(100, _print=True)
    # print("dir:", financeReader.getDir())
    # print("freq:", financeReader.getFrequency())
    # print("fileds:", financeReader.getFields())
    # print("shape:", financeReader.getDataShape())
    # print("range:", financeReader.getDateRange())
    # print("ins:", financeReader.getRegisteredInstruments())

    # print(panelFeed.extraPanel["MONETARY_CAP"].to_frame())
    # # print(panelFeed.extraPanel["OPER_REV"].to_frame())
    # print(panelFeed.extraPanel["REPORT_PERIOD"].to_frame())
