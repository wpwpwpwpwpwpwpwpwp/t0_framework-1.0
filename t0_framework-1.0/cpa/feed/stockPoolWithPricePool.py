from jqdatasdk import *
from datetime import datetime, date, timedelta
auth('18582326396', 'cdbjwp1995wpwp')
######涨停板统计股票池
def StockPoolWithPricePool():
       yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")####获取前一天的日期
       data = get_billboard_list(stock_list=None, end_date=yesterday, count=250)####获取最近一年的龙虎榜数据
       data = data.drop_duplicates(['code', 'day'])
       return(data['code'].value_counts())
if __name__ == '__main__':
    data=StockPoolWithPricePool()
    for i in range(50):
       print(data[i])
