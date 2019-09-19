"""
截面收益计算
输入x和y均为一列的数组形式, 且已处理过nan值
"""
import numpy as np
import pandas as pd
from cpa.utils import bar


def IC(x, y):
    """Pearson 相关系数"""
    x_ = x - x.mean(axis=0)
    y_ = y - y.mean(axis=0)
    xy = x_ * y_
    x2 = x_ ** 2
    y2 = y_ ** 2
    return xy.sum(axis=0) / np.sqrt(x2.sum(axis=0) * y2.sum(axis=0))


def RKIC(x, y):
    """Spearman 相关系数"""
    xNew = np.argsort(np.argsort(x))
    yNew = np.argsort(np.argsort(y))
    x_ = xNew - xNew.mean(axis=0)
    y_ = yNew - yNew.mean(axis=0)
    xy = x_ * y_
    x2 = x_ ** 2
    y2 = y_ ** 2
    return xy.sum(axis=0) / np.sqrt(x2.sum(axis=0) * y2.sum(axis=0))


def BETA(x, y):
    """单因子回归斜率"""
    x_ = x - x.mean(axis=0)
    y_ = y - y.mean(axis=0)
    xy = x_ * y_
    x2 = x_ ** 2
    return xy.sum(axis=0) / x2.sum(axis=0)


#暂时不用GPIC这个函数
def GPIC(x, y, groupNum):
    """分n组后的相关系数"""
    xRank = np.argsort(np.argsort(x))  # 计算序号:值越大序号越大
    xQuantile = xRank / len(x)  # 计算分位数
    groupIDs = np.array(range(1, groupNum + 1))  # 设置1-groupNum的组号
    groupMeanY = np.zeros([groupNum])  # 设置groupNum大小的空间存放每组平均收益
    xGroupRank = np.ceil(xQuantile * groupNum)  # 确定每个x值分别属于哪一组
    for dumi in range(1, groupNum + 1):
        groupIdx = xGroupRank == dumi  # 提取dumi组信息
        groupMeanY[dumi - 1] = np.sum(groupIdx * y, axis=0) / np.sum(groupIdx, axis=0)  # 计算该组平均收益率
    return IC(groupIDs, groupMeanY)


def TBDF(lastFactor, thisReturn, cut):
    """top平均收益 - bottom平均收益"""
    lastFactorRank = np.argsort(np.argsort(lastFactor))  # 计算序号:值越大序号越大
    lastFactorQuantile = lastFactorRank / len(lastFactor)  # 计算分位数
    topIdx = lastFactorQuantile >= 1 - cut  # 筛选出top组
    topRet = (thisReturn * topIdx).mean(axis=0) / topIdx.mean(axis=0)  # 计算top组平均收益
    botIdx = lastFactorQuantile < cut  # 筛选出bottom组
    botRet = (thisReturn * botIdx).mean(axis=0) / botIdx.mean(axis=0)  # 计算bottom组平均收益
    tbdf = topRet - botRet
    return tbdf

#暂时不用TURN这个函数
def TURN(lastTop, thisTop):
    turn = 1 - len(set(lastTop) & set(thisTop)) / len(lastTop)
    return turn

def ICGrouping(lastFactor, thisReturn, nGroup):
    '''
    :param lastFactor: 上期因子值
    :param thisReturn: 本期收益
    :param nGroup: 分组数
    :return:  分组计算各组的IC值
    '''
    lastFactorRank = np.argsort(np.argsort(lastFactor))  # 计算序号:值越大序号越大
    lastFactorQuantile = lastFactorRank / len(lastFactor)  # 计算分位数
    colCount = lastFactor.__len__()
    amountOfGroup = colCount // nGroup  # 计算每组股票数
    GroupingIC=[]
    lastFactor=pd.Series(lastFactor)
    thisReturn=pd.Series(thisReturn)
    for i in range(nGroup):
        groupIdx = ((colCount - i * amountOfGroup) > lastFactorRank) & (
                lastFactorRank >= (colCount - (i + 1) * amountOfGroup))  # 先筛选出相应的分组的股票，列表的值为TRUE和FALSE

        segFactor=lastFactor.iloc[groupIdx]
        segReturn=thisReturn.iloc[groupIdx]     #分别选出该组的 前一天因子值 和 当前的收益
        GroupingIC.append(IC(segFactor,segReturn))      #调用IC函数，计算该组的IC值
    return GroupingIC

def RankICGrouping(lastFactor, thisReturn, nGroup):
    '''
    :param lastFactor: 上期因子值
    :param thisReturn: 本期收益
    :param nGroup: 分组数
    :return: 分别计算各组的RankIC值
    '''
    lastFactorRank = np.argsort(np.argsort(lastFactor))  # 计算序号:值越大序号越大
    lastFactorQuantile = lastFactorRank / len(lastFactor)  # 计算分位数
    colCount = lastFactor.__len__()
    amountOfGroup = colCount // nGroup  # 计算每组股票数
    GroupingRankIC=[]
    lastFactor=pd.Series(lastFactor)
    thisReturn=pd.Series(thisReturn)
    for i in range(nGroup):
        groupIdx = ((colCount - i * amountOfGroup) > lastFactorRank) & (
                lastFactorRank >= (colCount - (i + 1) * amountOfGroup))  # 先筛选出相应的分组的股票，列表的值为TRUE和FALSE
        segFactor=lastFactor.iloc[groupIdx]
        segReturn=thisReturn.iloc[groupIdx]           #分别选出该组的 前一天因子值 和 当前的收益
        GroupingRankIC.append(RKIC(segFactor,segReturn))    #调用 RKIC函数，计算该组的RankIC
    return GroupingRankIC

def TurnGrouping(lastGrouping,thisGrouping, nGroup):
    '''
    :param lastGrouping: 之前持仓的股票
    :param thisGrouping: 目前根据因子计算，需要持仓的股票
    :param nGroup: 分组数量
    :return: 返回一个列表，元素是各组的当期换手率，第0个元素是top组
    '''
    turn=[]
    for i in range(nGroup):
        last = lastGrouping[i]
        this = thisGrouping[i]
        #换手率计算分两部分：
        #   1.上期持有，这期继续持有的票， 但是可能会增加持仓比例
        #   2.上期没有，这期要买入的票
        turnTmp = len(set(last) & set(this))*max((1/len(this)-1/len(last)),0) + \
                  1 - len(set(last) & set(this)) / len(last)
        turn.append(turnTmp)
    return turn

def Grouping(lastFactor, nGroup):
    '''
    :param lastFactor: 上期的因子值
    :param nGroup: 分组数
    :return: 根据传入的因子打分结果，按照得分高低，把股票分组，返回分组列表。列表的每个元素也是一个列表，对应该组别的True false
            返回的TRUE FALSE 列表的长度为，去除了空值后的长度
    '''
    lastFactorRank = np.argsort(np.argsort(lastFactor))  # 计算序号:值越大序号越大
    lastFactorQuantile = lastFactorRank / len(lastFactor)  # 计算分位数
    colCount = lastFactor.__len__()
    amountOfGroup = colCount // nGroup  # 计算每组股票数
    GroupingResult = []
    lastFactor = pd.Series(lastFactor)
    for i in range(nGroup):
        groupIdx = ((colCount - i * amountOfGroup) > lastFactorRank) & (
                lastFactorRank >= (colCount - (i + 1) * amountOfGroup))  # 先筛选出相应的分组的股票，列表的值为TRUE和FALSE
                                                                            # 分别选出该组的 前一天因子值 和 当前的收益
        GroupingResult.append(groupIdx)  # 把每一组的选出的股票记录（TRUE FALSE 列表）
    return GroupingResult

def adjustShape(GroupingResult,length,notNanLocate):
    '''
    :param GroupingResult: 由Grouping函数返回的列表
    :param length:  需要填充到的最终长度
    :param notNanLocate: 非空元素对应的位置
    :return: 返回一个列表，元素为子列表，每个子列表的长度填充为length
    '''
    adjusted=[]
    for i in range(len(GroupingResult)):
        wholeList=[False]*length
        for j in range(len(notNanLocate)):
            wholeList[notNanLocate[j]]=GroupingResult[i][j] #对于每个子列表，把GroupingResult中的TRUE、FALSE 按照notNanLocate填充进去
        adjusted.append(wholeList)
    return adjusted


def GroupRet(lastFactor, thisReturn, nGroup):
    '''
    :param lastFactor:
    :param thisReturn:
    :param nGroup:
    :return: 每组收益的list。从左数第一组为top组
    '''
    lastFactorRank = np.argsort(np.argsort(lastFactor))  # 计算序号:值越大序号越大
    lastFactorQuantile = lastFactorRank / len(lastFactor)  # 计算分位数
    colCount = lastFactor.__len__()
    amountOfGroup = colCount // nGroup  # 计算每组股票数
    retRaw = []
    for i in range(nGroup):
        groupIdx = ((colCount - i * amountOfGroup) > lastFactorRank) & (
                    lastFactorRank >= (colCount - (i + 1) * amountOfGroup))  # 先筛选出该组的股票
        groupRet = (thisReturn * groupIdx).sum(axis=0) / groupIdx.sum(axis=0)  # 再计算平均收益平均收益
        retRaw.append(groupRet)
    return retRaw

"""下面6个函数是对计算好的panel进行简单运算的函数"""
def RET(df):
    '''
    :param df:
    :return: 返回各组累计收益
    '''
    ret = df.sum(axis=0)
    ret.name = 'Ret'
    return ret

def MeanIC(df):
    '''
    :param df:
    :return: 返回各组平均IC
    '''
    IC_=df.mean(axis=0)
    IC_.name='IC'
    return IC_

def MeanRankIC(df):
    '''
    :param df:
    :return:返回各组平均RankIC
    '''
    IC_=df.mean(axis=0)
    IC_.name='RankIC'
    return IC_

def MeanTurn(df):
    '''
    :param df:
    :return: 返回每组平均换手率
    '''
    Turn_=df.iloc[1:].mean(axis=0)   #去除第一行，因为之前为了保持panel形状一致，在0时刻的换手率设为100%。 因此求平均时去除。
    Turn_.name='Turn'
    return Turn_

def SumCost(df):
    '''
    :param df:
    :return: 返回每组的累计交易成本
    '''
    Cost_ = df.sum(axis=0)
    Cost_.name = 'Cost'
    return Cost_

def MeanNumber(df):
    '''
    :param df:
    :return: 返回每组平均的持仓资产数量
    '''
    number_=df.mean(axis=0)
    number_.name='Num'
    return number_


def SHARP(df, frequency):
    if frequency == bar.Frequency.MINUTE:
        period = 252 * 4 * 60
    elif frequency == bar.Frequency.MINUTE5:
        period = 252 * 4 * 12
    elif frequency == bar.Frequency.MINUTE30:
        period = 252 * 4 * 2
    elif frequency == bar.Frequency.HOUR:
        period = 252 * 4
    elif frequency == bar.Frequency.HOUR2:
        period = 252 * 2
    elif frequency == bar.Frequency.DAY:
        period = 252
    else:
        raise ValueError('必须输入计算频率')

    # 注意是否要乘波动率
    excessEarning = df + int(1) - 1.035 ** (1 / period)
    sharpRaw = excessEarning.mean() / excessEarning.std()
    sharpRaw = sharpRaw * period ** 0.5
    sharpRaw.name = 'Sharp'
    return sharpRaw


def maxDrawDown(df):
    tempData1 = (df + 1).cumprod().expanding().max()  # 从开始到当期净值最高点
    tempData2 = (df + 1).cumprod() / tempData1  # 从开始到现在每一期的最大回撤
    maxdd = tempData2.min() - 1
    maxdd.name = 'MaxDD'
    return maxdd
