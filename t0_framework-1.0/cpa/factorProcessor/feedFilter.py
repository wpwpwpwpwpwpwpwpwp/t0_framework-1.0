# -*- coding: utf-8 -*-
# 输入数据过滤模块，在加载完原始数据后第一时间进行预过滤，过滤掉的instrument所在列数据清空
'''
@Time    : 2019/7/20 23:24
@Author  : msi
@Email   : sdu.xuefu@gmail.com
'''
from cpa.feed import baseFeed


class DefaultFeedFilter:
    '''
    默认数据过滤器
    '''

    class FilterMode:
        FILTER_NOTHING = 0  # 不过滤

    def __init__(self, panelFeed, filterMode=FilterMode.FILTER_NOTHING):
        '''
        :param panelFeed:
        :param filterMode:过滤模式
        :return:
        '''
        self.rawPanelFeed = panelFeed
        self.filterMode = filterMode

        if self.filterMode == self.FilterMode.FILTER_NOTHING or filterMode is None:
            self.values = panelFeed
        else:
            self.values = baseFeed.PanelFeed(None, panelFeed.getInstruments(), panelFeed.getFrequency(), panelFeed.maxLen)
            self.rawPanelFeed.getNewPanelsEvent(priority=baseFeed.PanelFeed.EventPriority.PREFILTER).subscribe(self.onNewValues)

    def onNewValues(self, panelFeed, dateTime, df):
        pass

    def __getattr__(self, attr):
        return getattr(self.values, attr)


if __name__ == '__main__':
    from cpa.feed.feedFactory import DataFeedFactory

    feed = DataFeedFactory.getHistFeed(instruments=['SZ399974'])
    d = DefaultFeedFilter(feed)
    print(d.getClosePanel())
