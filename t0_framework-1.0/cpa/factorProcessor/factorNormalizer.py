# -*- coding: utf-8 -*-
'''
原始因子标准化操作
@Time    : 2019/7/21 8:42
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
from cpa.utils.series import SequenceDataPanel
import numpy as np


class NormalizedFeed(SequenceDataPanel):
    class NormalizedType:
        '''
        因子规范化类型
        '''
        RAW = 0
        RANK = 1
        ZSCORE = 2

    def __init__(self, factorPanel, normType=NormalizedType.RAW, outliersOut=True, inplace=True):
        '''
        :param factorPanel:
        :param normType:
        :param maxLen:
        :param inplace: 直接对factor的原值进行替代，如果同时需要rank和zsocre，则不可对factor原值替代
        :param outliersOut:去极值
        :return:
        '''
        super(NormalizedFeed, self).__init__(factorPanel.getColumnNames(), maxLen=factorPanel.getMaxLen())
        self.normType = normType
        self.inplace = inplace
        self.outliersOut = outliersOut
        factorPanel.getNewValuesEvent().subscribe(self.onNewValues)

    def onNewValues(self, factorPanel, dateTime, values):
        '''
        :param factorPanel:标准化数据，直接对factorPanel的原值进行修改
        :param dateTime:
        :param values:
        :return:
        '''
        if self.normType == self.NormalizedType.RAW:
            return

        if not self.inplace:
            values = values.copy()

        noneNalocation = np.argwhere(~np.isnan(values))
        noneNaValue = values[noneNalocation[:, 0]]

        if self.normType == self.NormalizedType.RANK:
            rvalue = np.argsort(np.argsort(noneNaValue))
            values[noneNalocation[:, 0]] = rvalue

        elif self.normType == self.NormalizedType.ZSCORE:
            # 中位数去极值处理
            if self.outliersOut:
                c = 2  # 1.483 ~ 3sigma, 采用稍大尽量少过滤一些极端值
                mu = np.median(noneNaValue)
                mad = np.median(np.abs(noneNaValue - mu))
                outlier = (noneNaValue > mu + c * mad) | (noneNaValue < mu - c * mad)

                naIdex = noneNalocation[outlier]
                values[naIdex] = np.nan
                noneNalocation = np.argwhere(~np.isnan(values))
                noneNaValue = values[noneNalocation[:, 0]]

            mu = np.median(noneNaValue)
            std = np.std(noneNaValue)
            zvalue = (noneNaValue - mu / std)
            values[noneNalocation[:, 0]] = zvalue

        if not self.inplace:
            self.appendWithDateTime(dateTime, values)


if __name__ == '__main__':
    from cpa.feed import feedFactory

    a = feedFactory.DataFeedFactory.getHistFeed(instruments=['SH000060'])
    n = NormalizedFeed(a.closePanel, normType=NormalizedFeed.NormalizedType.ZSCORE)
    a.closePanel.getNewValuesEvent().unsubscribe(n.onNewValues)

    testCase = np.array(
        [[1, np.nan, 8, 2, 2, np.nan],
         [2, 2, np.nan, 3, 10, 1]]
    )
    for i in [0, 1]:
        n.onNewValues(None, None, testCase[i])
