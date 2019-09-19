# -*- coding: utf-8 -*-
# tushare live feed 需要打补丁，保证14：55分的数据，在dataSeries上去除该时间点的数据
# 具体包括数据方面和技术指标方面
# 股票市场比较特殊，增加一个event，当出现15：00的数据的时候，调用的不是新增一条bar，而是去刷新14：55分的所有数据（包括dataseries里的以及技术指标里的）
# #同时不再调用onbar Event
# 技术指标上要添加onLastBarEvent

#***************************************************
#第90行左右， self._EventBasedFilter__dataSeries.getUpdateValueEvent().subscribe(self.onUpdateValue)
#给输入进来的dataseries添加监听器的时候，会给本身（继承自sequence dataseries）的event也添加一个listener，而这个listener还会
#指向自身，导致死循环,根源在于第63行
#该语法错误，会使得updateValueEvent变成global变量，而非私有的变量，相当于实例化了一个对象，然后让该类的所有的instance都指向它
#dataseries.SequenceDataSeries.updateValueEvent = observer.Event()
#*************************************************

from pyalgotrade.utils import collections
from pyalgotrade import dataseries
from pyalgotrade import technical
from pyalgotrade import feed
from pyalgotrade import observer
from pyalgotrade.dataseries import bards
import six
def _patch_listdeque():
    '''add removeLast function'''
    def updateLast(cls, value):
        if cls._ListDeque__values.__len__() == 0:
            cls._ListDeque__values.append(value)
        else:
            cls._ListDeque__values[-1] = value

    collections.ListDeque.removeLast = lambda cls: cls._ListDeque__values.pop(-1)
    collections.ListDeque.updateLast = updateLast

def _patch_numpydeque():

    def update(self, value):
        if self._NumPyDeque__nextPos == 0:
            self._NumPyDeque__values[self._NumPyDeque__nextPos] = value
            self._NumPyDeque__nextPos += 1
        else:
            self._NumPyDeque__values[self._NumPyDeque__nextPos - 1] = value

    def removeLast(self):
        if self._NumPyDeque__nextPos > 0:
            self._NumPyDeque__nextPos -= 1
    collections.NumPyDeque.update = update
    collections.NumPyDeque.removeLast = removeLast

def _patch_dataseries():

    def updateWithDateTime(cls, dateTime, value):
        assert(len(cls._SequenceDataSeries__values) == len(cls._SequenceDataSeries__dateTimes))
        cls._SequenceDataSeries__dateTimes.updateLast(dateTime)
        cls._SequenceDataSeries__values.updateLast(value)
        if isinstance(value, float):
        # send to technical indicators if the series is from ohlc
            cls.getUpdateValueEvent().emit(cls, dateTime, value)

    def update(cls, value):
        cls._SequenceDataSeries__values.updateLast(value)
        if isinstance(value, float):
            # send to technical indicators if the series is from ohlc
            cls.getUpdateValueEvent().emit(cls, cls._SequenceDataSeries__dateTimes[-1], value)


    def getUpdateValueEvent(cls):
        return cls.updateValueEvent

    def new_init(self, maxLen=None):
        super(dataseries.SequenceDataSeries, self).__init__()
        maxLen = dataseries.get_checked_max_len(maxLen)
        self._SequenceDataSeries__newValueEvent = observer.Event()
        self._SequenceDataSeries__values = collections.ListDeque(maxLen)
        self._SequenceDataSeries__dateTimes = collections.ListDeque(maxLen)
        self.updateValueEvent = observer.Event()

    #该语法错误，会使得updateValueEvent变成global变量，而非私有的变量，相当于实例化了一个对象，然后让该类的所有的instance都指向它
    #dataseries.SequenceDataSeries.updateValueEvent = observer.Event()
    dataseries.SequenceDataSeries.__init__ = new_init
    dataseries.SequenceDataSeries.getUpdateValueEvent = getUpdateValueEvent
    dataseries.SequenceDataSeries.updateWithDateTime = updateWithDateTime
    dataseries.SequenceDataSeries.update = update

def _patch_bards():

    def updateWithDateTime(self, dateTime, bar):
        assert (dateTime is not None)
        assert (bar is not None)

        super(bards.BarDataSeries, self).updateWithDateTime(dateTime, bar)

        self._BarDataSeries__openDS.updateWithDateTime(dateTime, bar.getOpen())
        self._BarDataSeries__closeDS.updateWithDateTime(dateTime, bar.getClose())
        self._BarDataSeries__highDS.updateWithDateTime(dateTime, bar.getHigh())
        self._BarDataSeries__lowDS.updateWithDateTime(dateTime, bar.getLow())
        self._BarDataSeries__volumeDS.updateWithDateTime(dateTime, bar.getVolume())
        self._BarDataSeries__adjCloseDS.updateWithDateTime(dateTime, bar.getAdjClose())
        # Process extra columns.
        for name, value in six.iteritems(bar.getExtraColumns()):
            extraDS = self._BarDataSeries__getOrCreateExtraDS(name)
            extraDS.updateWithDateTime(dateTime, value)

    bards.BarDataSeries.updateWithDateTime = updateWithDateTime

def _patch_eventWindow():
    def onUpdateValue(self, dateTime, value):
        if value is not None or not self._EventWindow__skipNone:
            self._EventWindow__values.removeLast()  #called buy technical, calling numpydeque
            self.onNewValue(dateTime, value)

    technical.EventWindow.onUpdateValue = onUpdateValue

def _patch_technical():

    def onUpdateValue(self, dataSeries, dateTime, value):
        self._EventBasedFilter__eventWindow.onUpdateValue(dateTime, value)
        newValue = self._EventBasedFilter__eventWindow.getValue()
        self.updateWithDateTime(dateTime, newValue)

    def new_init(self, dataSeries, eventWindow, maxLen=None):
        super(technical.EventBasedFilter, self).__init__(maxLen)
        self._EventBasedFilter__dataSeries = dataSeries
        self._EventBasedFilter__dataSeries.getNewValueEvent().subscribe(self._EventBasedFilter__onNewValue)
        self._EventBasedFilter__dataSeries.getUpdateValueEvent().subscribe(self.onUpdateValue)
        self._EventBasedFilter__eventWindow = eventWindow

    technical.EventBasedFilter.onUpdateValue = onUpdateValue
    technical.EventBasedFilter.__init__ = new_init


def _patch_feed():

    '''reload the dispatch function of the base feed'''

    def dispatch(self):
        dateTime, values = self.getNextValuesAndUpdateDS()
        # 15:00的不再emit strategy 的onbars,backtest.broker的onbars, 处于idle status
        if dateTime is not None:
            self._BaseFeed__event.emit(dateTime, values)
        return dateTime is not None

    feed.BaseFeed.dispatch = dispatch

def patchAll():
    _patch_listdeque()
    _patch_numpydeque()
    _patch_dataseries()
    _patch_bards()
    _patch_eventWindow()
    _patch_technical()
    _patch_feed()

def test_case():

    _patch_listdeque()
    a = collections.ListDeque(maxLen=3)
    a.append(3)
    a.append(4)
    a.updateLast(5)
    print(a.data())

if __name__ == '__main__':
    #test_case()
    patchAll()

