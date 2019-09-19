# -*- coding: utf-8 -*-
'''
@Time    : 2019/7/20 22:25
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''

from cpa.utils import logger
from cpa.feed.baseFeed import PanelFeed
from cpa.utils import bar
from cpa.config import const
from cpa.io import h5Reader
from cpa.io import csvReader
from cpa.resample.resampled import ResampledPanelFeed

class InlineDataSet:
    '''
    内部提供的测试集
    '''

    @classmethod
    def SZ50_MINUTE(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='SZ50', frequency=bar.Frequency.MINUTE)

    @classmethod
    def SZ50_DAY(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='SZ50', frequency=bar.Frequency.DAY)

    @classmethod
    def HS300_MINUTE(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='HS300', frequency=bar.Frequency.MINUTE)

    @classmethod
    def HS300_DAY(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='HS300', frequency=bar.Frequency.DAY)

    @classmethod
    def ZZ500_MINUTE(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='ZZ500', frequency=bar.Frequency.MINUTE)

    @classmethod
    def ZZ500_DAY(cls):
        '''
        中证500分钟频测试数据，3个月，样本内数据2015（6,7,8月）
        '''
        return DataFeedFactory.getHistFeed(
            instruments='ZZ500', frequency=bar.Frequency.DAY)

    @classmethod
    def IC_MINUTES(cls):
        return DataFeedFactory.getHistFeed(instruments=['IC'], market=bar.Market.FUTURES,
                                           frequency=bar.Frequency.MINUTE)

    @classmethod
    def IF_MINUTES(cls):
        return DataFeedFactory.getHistFeed(instruments=['IF'], market=bar.Market.FUTURES,
                                           frequency=bar.Frequency.MINUTE)

    @classmethod
    def IH_MINUTES(cls):
        return DataFeedFactory.getHistFeed(instruments=['IH'], market=bar.Market.FUTURES,
                                           frequency=bar.Frequency.MINUTE)

    @classmethod
    def CSV_TEST_DATA(cls):
        '''
        常用的功能测试数据
        '''
        return DataFeedFactory.getHistFeed(
            types=const.DataType.SAMPLE,
            frequency=bar.Frequency.MINUTE)


class DataFeedFactory:
    '''
    数据集中接口
    '''
    logger = logger.getLogger('feedFactory')

    @classmethod
    def getHistFeed(
            cls,
            market=bar.Market.STOCK,
            frequency=bar.Frequency.MINUTE,
            instruments=None,
            field=const.DataField.OHLCV,
            start=None,
            end=None,
            types=const.DataType.OHLCV,
            maxLen=1024):
        '''
        :param market: 市场类型
        :param frequency: 周期
        :param instruments: instrumets列表
        :param field: 数据列（OHLCV）
        :param start: 起止时间
        :param end:
        :param types: 数据类别，默认是价量数据，也可以是财务数据
        :param maxLen: dataFeed缓存长度
        :return:
        '''

        if field is None:
            field = const.DataField.OHLCV
        if types is None:
            types = const.DataType.OHLCV

        # 股票市场数据
        if market == bar.Market.STOCK:
            if types == const.DataType.OHLCV:
                dataReader = h5Reader.H5DataReader(frequency=frequency,
                                                   instruments=instruments,
                                                   fields=field,
                                                   start=start,
                                                   end=end)
                dataReader.prepareGenerator()  # 调用生成器
            elif types == const.DataType.SAMPLE:
                assert frequency == bar.Frequency.MINUTE, '样本数据只包含1分钟频率'
                dataReader = csvReader.CSVSampleDataReader(frequency, instruments=None)
                dataReader.loads()

            elif types == const.DataType.FINANCE:
                assert frequency == bar.Frequency.QUARTER, '财务数据为季度'
                dataReader = csvReader.CSVFinanceReader.FinanceReader(
                    fileName=None, instruments=None, fields=None, start=None)
            else:
                dataReader = None

            panelFeed = PanelFeed(
                dataReader,
                dataReader.getRegisteredInstruments(),
                maxLen=maxLen)
            return panelFeed

        # 期货市场数据
        elif market == bar.Market.FUTURES:

            if frequency == bar.Frequency.MINUTE:
                dataReader = csvReader.CSVFutureDataReader(instruments, const.DataField.OHLCV, start, end)
                dataReader.prepareGenerator()
                panelFeed = PanelFeed(
                    dataReader,
                    dataReader.getRegisteredInstruments(),
                    maxLen=maxLen)
                return panelFeed

            elif frequency != bar.Frequency.MINUTE and {instruments} <= {['IH', 'IF', 'IC']}:
                dataReader = csvReader.CSVFutureDataReader(instruments, const.DataField.OHLCV, start, end)
                dataReader.prepareGenerator()
                panelFeed = PanelFeed(dataReader, dataReader.getRegisteredInstruments(), maxLen=maxLen)
                resampleFeed = ResampledPanelFeed(panelFeed, frequency=frequency)
                return resampleFeed

            else:
                assert frequency == bar.Frequency.MINUTE, '商品期货数据仅支持1分钟频率，后续请李霄完善'

    @classmethod
    def getLiveFeed(
            cls,
            source='tq',
            instruments=None,
            start=None,
            end=None,
            maxLen=1024):
        '''
        :param source: 实时数据流
        :param instruments:
        :param start:
        :param end:
        :param maxLen:
        :return:
        '''
        pass


if __name__ == '__main__':
    # panelFeed = InlineDataSet.CSV_TEST_DATA()
    # panelFeed.run(stopCount=10, _print=True)

    # 调用股票数据
    # panelFeed = DataFeedFactory.getHistFeed(market=bar.Market.STOCK, frequency=bar.Frequency.HOUR * 2, instruments=None)

    # 调用股指期货数据
    panelFeed = DataFeedFactory.getHistFeed(instruments=['IC'], market=bar.Market.STOCK,
                                            frequency=bar.Frequency.MINUTE30)

    panelFeed.run(stopCount=1000, _print=True)
