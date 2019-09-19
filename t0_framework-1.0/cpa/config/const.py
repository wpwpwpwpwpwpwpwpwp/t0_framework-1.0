# coding=utf8
from cpa.utils import bar


# 基础数据目录结构  data - dataMarket - dataType - fileName - fields(字段名)
class DataMarket:
    # 保存数据分类
    STOCK = 'stock'
    FUTURES = 'futures'

    @classmethod
    def market2lable(cls, market):
        if market == bar.Market.STOCK:
            return cls.STOCK
        elif market == bar.Market.FUTURES:
            return cls.FUTURES

        return cls.STOCK

class DataType:

    OHLCV = 'ohlcv'
    FINANCE = 'finance'
    SAMPLE = 'sample'  #测试样本
    INDEX = 'index'

class DataFrequency:
    # key标记
    MINUTE = '1min'
    MINUTE5 = '5min'
    MINUTE30 = '30min'
    HOUR = '1h'
    HOUR2 = '2h'
    DAY = '1d'

    WEEK = 'week'
    MONTH = 'month'
    QUARTER = 'quarter'

    @classmethod
    def freq2lable(cls, frequency):
        if frequency == bar.Frequency.MINUTE:
            return cls.MINUTE
        elif frequency < bar.Frequency.HOUR and frequency / bar.Frequency.MINUTE == 5:
            return cls.MINUTE5
        elif frequency < bar.Frequency.HOUR and frequency / bar.Frequency.MINUTE == 30:
            return cls.MINUTE30
        elif frequency == bar.Frequency.HOUR:
            return cls.HOUR
        elif frequency < bar.Frequency.DAY and frequency / bar.Frequency.HOUR == 2:
            return cls.HOUR2
        elif frequency == bar.Frequency.DAY:
            return cls.DAY
        elif frequency == bar.Frequency.WEEK:
            return cls.WEEK
        elif frequency == bar.Frequency.MONTH:
            return cls.MONTH
        return cls.QUARTER


class DataInstrument:
    HS300 = None
    SZ50 = None
    ALL = 'ALL'

class DataField:
    OHLCV = ['open', 'high', 'low', 'close', 'volume']


class DataFile:
    # 数据文件名标记
    ZZ500 = 'zz500_stock20180601to20190601.h5'
    INDEX = '000001_index.csv'


# 因子数据目录结构 factor - PoolName - factorName - factorSection - fileName
class FactorSection:
    EXPOSURE = 'risk_exposure'
    SCORE = 'score'
    SECTION = 'section_indicators'
