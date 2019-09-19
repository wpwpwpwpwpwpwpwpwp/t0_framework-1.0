# -*- coding: utf-8 -*-
'''
1.redefine the meaning of resample to fit the chinese stock and ctp market.
especialling the ctp market,we'll swith the market time and the real time ,and get the real time ending time without 1 bar lag.
for eg: the 15:00 is the market end and the then the resample bar is send.
2.called from cnx.strategy, when directly called from the raw strategy module, we should add monkey patch
@Time    : 2019/1/10 18:03
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''

# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import abc
import datetime

import six

from pyalgotrade.utils import dt
from cpa.utils import bar
from pandas.tseries.offsets import BDay


@six.add_metaclass(abc.ABCMeta)
class TimeRange(object):
    @abc.abstractmethod
    def belongs(self, dateTime):
        raise NotImplementedError()

    @abc.abstractmethod
    def getBeginning(self):
        raise NotImplementedError()

    # 1 past the end
    @abc.abstractmethod
    def getEnding(self):
        raise NotImplementedError()


class IntraDayRange(TimeRange):
    def __init__(self, dateTime, frequency):
        super(IntraDayRange, self).__init__()
        assert isinstance(frequency, int)
        assert frequency > 1
        assert frequency < bar.Frequency.DAY

        ts = int(dt.datetime_to_timestamp(dateTime))
        # slot = ts / frequency # 从31分开始计算
        # slotTs = slot * frequency
        self.__begin = dt.timestamp_to_datetime(
            ts, not dt.datetime_is_naive(dateTime))
        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
        self.__end = self.__begin + \
            datetime.timedelta(seconds=(frequency - bar.Frequency.MINUTE))  # 获取最后一个bar数据时截止
        self.isFirtstCheckingNearly = True

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def nearlyMarketEnding(self, dateTime):  # 收盘前,且只发送一次
        if dateTime.hour == 14 and dateTime.minute >= 55 and self.isFirtstCheckingNearly:
            self.isFirtstCheckingNearly = False
            return True
        return False

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class DayRange(TimeRange):
    def __init__(self, dateTime):
        super(DayRange, self).__init__()
        self.__begin = datetime.datetime(
            dateTime.year, dateTime.month, dateTime.day)
        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
        self.__end = self.__begin + datetime.timedelta(days=1)

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class CtpDayRange(TimeRange):
    def __init__(self, dateTime):
        super(CtpDayRange, self).__init__()
        self.__initBegin(dateTime)

        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
        self.__end = self.__begin.replace(hour=15, minute=0) + BDay()
        self.isFirtstCheckingNearly = True

    def __initBegin(self, dateTime):
        # 初始化起始日期，dataTime 为真实数据时间，begin也是用的真实时间，即1月8日 21:00的数据，此处即为1月8日21:00
        hour = dateTime.hour
        self.__begin = datetime.datetime(
            dateTime.year, dateTime.month, dateTime.day, hour=21)
        if hour < 15:
            self.__begin = self.__begin - BDay()

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def nearlyMarketEnding(self, dateTime):  # 收盘前,且只发送一次,夜盘暂未考虑
        if dateTime.hour == 14 and dateTime.minute >= 55 and self.isFirtstCheckingNearly:
            self.isFirtstCheckingNearly = False
            return True
        return False

    def getTradingDate(self):
        # 返回交易（重采样后的交易时间），像1月8日21:00，实际上日线重采样归属于1月9日
        return self.getEnding().replace(hour=0)

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class StockDayRange(TimeRange):
    def __init__(self, dateTime):
        super(StockDayRange, self).__init__()
        self.__begin = datetime.datetime(
            dateTime.year,
            dateTime.month,
            dateTime.day,
            hour=9,
            minute=31)

        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
        self.__end = self.__begin.replace(hour=15, minute=0)
        self.isFirtstCheckingNearly = True

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def nearlyMarketEnding(self, dateTime):  # 收盘前,且只发送一次
        if dateTime.hour == 14 and dateTime.minute >= 55 and self.isFirtstCheckingNearly:
            self.isFirtstCheckingNearly = False
            return True
        return False

    def getTradingDate(self):
        return self.getEnding().replace(hour=0)

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class StockWeekRange(TimeRange):
    def __init__(self, dateTime):
        super(StockWeekRange, self).__init__()
        self.__begin = dateTime.replace(
            hour=9, minute=0) - datetime.timedelta(days=dateTime.weekday())
        self.__end = self.__begin.replace(hour=15) + datetime.timedelta(days=6)
        self.isFirtstCheckingNearly = True
        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
            self.__end = dt.localize(self.__end, dateTime.tzinfo)

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def nearlyMarketEnding(self, dateTime):  # 收盘前,且只发送一次
        if dateTime.hour == 14 and dateTime.minute >= 55 and self.isFirtstCheckingNearly:
            self.isFirtstCheckingNearly = False
            return True
        return False

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class CtpWeekRange(TimeRange):
    def __init__(self, dateTime):
        super(CtpWeekRange, self).__init__()
        self.__begin = dateTime - datetime.timedelta(days=dateTime.weekday())
        self.__initBegin(self.__begin)
        self.__end = self.__begin.replace(hour=15) + datetime.timedelta(days=6)
        self.isFirtstCheckingNearly = True
        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
            self.__end = dt.localize(self.__end, dateTime.tzinfo)

    def __initBegin(self, dateTime):
        # 初始化起始日期，dataTime 为真实数据时间，begin也是用的真实时间，即1月8日 21:00的数据，此处即为1月8日21:00
        hour = dateTime.hour
        self.__begin = datetime.datetime(
            dateTime.year, dateTime.month, dateTime.day, hour=21)
        if hour < 15:
            self.__begin = self.__begin - BDay()

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def nearlyMarketEnding(self, dateTime):  # 收盘前,且只发送一次
        if dateTime.hour == 14 and dateTime.minute >= 55 and self.isFirtstCheckingNearly:
            self.isFirtstCheckingNearly = False
            return True
        return False

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


class MonthRange(TimeRange):
    def __init__(self, dateTime):
        super(MonthRange, self).__init__()
        self.__begin = datetime.datetime(dateTime.year, dateTime.month, 1)

        # Calculate the ending date.
        if dateTime.month == 12:
            self.__end = datetime.datetime(dateTime.year + 1, 1, 1)
        else:
            self.__end = datetime.datetime(
                dateTime.year, dateTime.month + 1, 1)

        if not dt.datetime_is_naive(dateTime):
            self.__begin = dt.localize(self.__begin, dateTime.tzinfo)
            self.__end = dt.localize(self.__end, dateTime.tzinfo)

    def belongs(self, dateTime):
        return dateTime >= self.__begin and dateTime < self.__end

    def equalEnding(self, dataTime):
        return dataTime == self.__end

    def outEnding(self, dateTime):
        return dateTime > self.__end

    def getBeginning(self):
        return self.__begin

    def getEnding(self):
        return self.__end


def is_valid_frequency(frequency):
    assert (isinstance(frequency, int))
    assert (frequency > 1)

    if frequency < bar.Frequency.DAY:
        ret = True
    elif frequency == bar.Frequency.DAY:
        ret = True
    elif frequency == bar.Frequency.MONTH:
        ret = True
    else:
        ret = False
    return ret


def build_range(dateTime, frequency, market=None):
    assert (isinstance(frequency, int))
    assert (frequency > 1)

    if frequency < bar.Frequency.DAY:
        ret = IntraDayRange(dateTime, frequency)
    elif frequency == bar.Frequency.DAY:
        if market == bar.Market.STOCK:
            ret = StockDayRange(dateTime)
        elif market == bar.Market.CTP:
            return CtpDayRange(dateTime)
        else:
            return DayRange(dateTime)
    elif frequency == bar.Frequency.WEEK:
        if market == bar.Market.STOCK:
            ret = StockWeekRange(dateTime)
        elif market == bar.Market.CTP:
            return CtpWeekRange(dateTime)
        else:
            return DayRange(dateTime)

    elif frequency == bar.Frequency.MONTH:
        ret = MonthRange(dateTime)
    else:
        raise Exception("Unsupported frequency")
    return ret


@six.add_metaclass(abc.ABCMeta)
class Grouper(object):
    def __init__(self, groupDateTime):
        self.__groupDateTime = groupDateTime

    def getDateTime(self):
        return self.__groupDateTime

    @abc.abstractmethod
    def addValue(self, value):
        """Add a value to the group."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getGrouped(self):
        """Return the grouped value."""
        raise NotImplementedError()

    def setDateTime(self, dateTime):
        self.__groupDateTime = dateTime
