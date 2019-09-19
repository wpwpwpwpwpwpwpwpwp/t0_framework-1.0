from cpa.utils.series import SequenceDataSeries


class MA(SequenceDataSeries):
    '''
    **不可对sliceSeries赋值,若赋值须copy一份**
     不推荐使用seriesIndicator, 尽量使用panelIndicator或者
    '''

    def __init__(self, sliceSeries, n, maxLen=None):
        super(MA, self).__init__(maxLen=maxLen)  # 继承的子类
        sliceSeries.getNewValueEvent().subscribe(self.onNewValue)
        self.n = n

    def onNewValue(self, sliceSeries, dateTime, value):
        '''
        :return:最新的一行值
        '''
        value = sliceSeries[-self.n: ].mean(axis=0)  # 取最新一期值
        self.appendWithDateTime(dateTime, value)


if __name__ == '__main__':
    from cpa.feed.feedFactory import InlineDataSet

    panelFeed = InlineDataSet.SZ50_MINUTE()
    maClass = MA(panelFeed.barFeeds[panelFeed.getInstruments()[0]].getCloseSeries(), n=20, maxLen=1024)
    panelFeed.run(1000)
