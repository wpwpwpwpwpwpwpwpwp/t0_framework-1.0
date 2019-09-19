from cpa.utils.series import SequenceDataPanel


class MA(SequenceDataPanel):
    '''
    **不可对panel赋值,若赋值须copy一份**
    panel行为时间,列为codes,值为数据矩阵  
    '''

    def __init__(self, dataPanel, n, maxLen):
        super(MA, self).__init__(dataPanel.getColumnNames(), maxLen=maxLen)  # 继承的子类
        dataPanel.getNewValuesEvent().subscribe(self.onNewValues)
        self.n = n

    def onNewValues(self, dataPanel, dateTime, values):
        '''
        :return:最新的一行值
        '''

        values = dataPanel[-self.n:, :].mean(axis=0)  # 取最新一期值
        self.appendWithDateTime(dateTime, values)


if __name__ == '__main__':
    from cpa.feed import baseFeed
    from cpa.feed.feedFactory import InlineDataSet
    panelFeed = InlineDataSet.HS300_MINUTE()
    maPanel = MA(panelFeed.closePanel, n=20, maxLen=1024)  # 以开盘价计算的向前n期收益,定义returns类

    panelFeed.run(500)

    # 数据展示
    print(maPanel.to_frame())