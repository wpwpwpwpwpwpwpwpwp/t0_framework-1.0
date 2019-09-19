# -*- coding: utf-8 -*-
'''
@Time    : 2019/6/10 21:36
@Author  : msi
@Email   : sdu.xuefu@gmail.com
因子打分计算基类
'''
import abc
from cpa.utils import logger


class BaseBarFeedCalculator:

    '''
    基于单个instrument的因子打分计算基类

    '''

    __metaclass__ = abc.ABCMeta
    __slots__ = ('factorManager', 'feed', 'outputScore', 'name')
    logger = logger.getLogger("BaseBarFeedCalculator")

    def __init__(self, factorManager, barFeed):
        '''
        :param factorManagerCls: 可能会调用从factorManagerCls中加载面板计算的指标
        :param barFeed: barFeed操作方法与cta策略类似
        :param maxLen:
        '''

        self.factorManager = factorManager
        self.feed = barFeed
        self.name = self.__class__.__name__
        self.logger.debug("The factor manager: {}".format(factorManager))
        self.logger.debug("The feed type: {}".format(self.feed))

    @abc.abstractmethod
    def calScore(self, barFeed, dateTime, bar):
        '''
        当新时刻的数据到达时,计算最新的因子打分值并返回
        :param barFeed:
        :param dateTime:
        :param bar:
        :return: 无结果填充np.nan
        '''
        raise NotImplementedError


class BasePanelCalculator:
    '''
    矩阵化处理计算因子,某些因子比较简单,使用这种方法速度更快
    '''
    __metaclass__ = abc.ABCMeta
    __slots__ = ('factorManager', 'feed', 'outputScore', 'name')

    logger = logger.getLogger("BasePanelCalculator")

    def __init__(self, factorManager, panelFeed):
        # 可能会调用从factorManager中加载面板计算的指标
        self.factorManager = factorManager
        self.feed = panelFeed
        self.name = self.__class__.__name__
        self.logger.debug("The factor manager used: {}".format(factorManager))
        self.logger.debug("The feed type: {}".format(self.feed))

    @abc.abstractmethod
    def calSore(self, panelFeed, dateTime, df):
        raise NotImplementedError



