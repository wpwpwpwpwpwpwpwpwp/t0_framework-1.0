#!/usr/bin/env Python
# -*- coding:utf-8 -*-
# author: Yanggang Fang

'''
Renko因子计算
@Time    : 2019/8/17 23:58
'''
from cpa.indicators.seriesIndicators import renkoGenerator
from cpa.factorModel import BaseBarFeedCalculator


class Factor(BaseBarFeedCalculator):
    '''
    '''

    def __init__(self, factorManager, barFeed):
        self.factorManager = factorManager
        self.barFeed = barFeed
        self.instrument = self.barFeed.getInstrument()
        self.frequency = self.barFeed.getFrequency()
        self.maxLen = self.barFeed.getMaxLen()

        self.dataMergeFeed = renkoGenerator.RenkoGenerator(
            barFeed=self.barFeed,
            brickLength=None,
            rolling=True,
            preN=100,
            discount=0.0001,
            decimals=2)

    def calScore(self, barFeed, dateTime, bar):
        self.dataMergeFeed.KLineToRenko(dateTime, bar)
