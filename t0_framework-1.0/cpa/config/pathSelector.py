# -*- coding: utf-8 -*-
'''
简单的配置管理类
@Time    : 2019/7/28 21:49
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
import configparser as cg
import os
import platform
from cpa.utils import bar
from cpa.config import const


def platformSectionSelector():
    '''
    :return:数据路径选择
    '''
    if platform.system() == "Linux":
        return r'F:\jianghai\data\data\stock\ohlcv\1min\2015'
    elif platform.node() == 'msi-PC':
        return r'F:\jianghai\data\data\stock\ohlcv\1min\2015'
    elif platform.node() == 'DESKTOP-L1IOU60':
        return r'F:\jianghai\data\data\stock\ohlcv\1min\2015'
    else:
        return r'F:\jianghai\data\data\stock\ohlcv\1min\2015'


def dirChecker(func):
    # 不存在该目录则自动创建
    def checkDir(*args, **kwargs):
        path, file = func(*args, **kwargs)
        if not os.path.exists(path):
            os.makedirs(path)
        if file is not None:
            path = os.path.join(path, file)
        return path

    return checkDir


class PathSelector:
    '''
    维护config里面的目录结构
    '''

    cfg = cg.ConfigParser()
    modulePath = os.path.dirname(os.path.abspath(__file__))
    cfg.read(os.path.join(modulePath, 'dataPath.ini'))
    DEFAULT_SECTION = platformSectionSelector()

    @classmethod
    def getConfig(cls):
        return cls.cfg

    @classmethod
    def getDefaultSection(cls):
        return cls.DEFAULT_SECTION

    @classmethod
    def setConfigPath(cls, section=None):
        '''
        :param path:自定义配置文件路径
        :param section:
        :return:
        '''
        if section is not None:
            cls.DEFAULT_SECTION = section

    @classmethod
    @dirChecker
    def getDataFilePath(cls, market=const.DataMarket.STOCK, types=const.DataType.OHLCV,
                        frequency=const.DataFrequency.MINUTE, fileName=None):
        '''
        基础数据目录结构  data - dataMarket - dataFields - fileName - columns(字段名)
        :param dataMarket: DataCatogory
        :param types: DataType
        :param frequency:
        :param  fileName, None 则返回到所在目录
        :return:
        '''
        folder = cls.cfg.get(cls.DEFAULT_SECTION, 'data')
        dataPath = os.path.join(folder, market, types, frequency)
        return dataPath, fileName

    @classmethod
    @dirChecker
    def getFactorFilePath(cls, poolName='base', factorName=None, factorFrequency=const.DataFrequency.MINUTE5,
                          fileName=None):
        '''
        因子数据目录结构 factors - PoolName - factorName - factorSection - fileName
        :param pool: 获取因子存储路径，含因子计算完成的相关变量，采用四级目录结构
        :param factor:因子名
        :param section: 因子下子目录名
        :param file: None 则返回路径名
        :return:
        '''
        folder = cls.cfg.get(cls.DEFAULT_SECTION, 'factorData')
        if factorName is None:
            path = os.path.join(folder, poolName), fileName
        elif factorName is not None and factorFrequency is None:
            path = os.path.join(folder, poolName, factorName), fileName
        else:
            path = os.path.join(folder, poolName, factorName, factorFrequency), fileName
        return path

    @classmethod
    @dirChecker
    def getFactorDefPath(cls):
        '''
        返回因子定义文件夹factors
        '''
        folder = cls.cfg.get(cls.DEFAULT_SECTION, "factorDefinition")
        path = folder, None
        return path

    @classmethod
    @dirChecker
    def getFactorReportPath(cls):
        '''
        返回因子报告文件夹factorReports
        '''
        folder = cls.cfg.get(cls.DEFAULT_SECTION, "factorReports")
        path = folder, None
        return path


if __name__ == '__main__':
    from cpa.config import const

    print(PathSelector.getFactorFilePath(factorName='sss',fileName='111'))
