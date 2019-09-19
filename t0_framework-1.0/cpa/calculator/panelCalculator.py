# coding=utf8
'''
    panelCalculator.py
    描述：对数据最后一段2D numpy矩阵进行运算
    注意：
    （1）计算统计量时采用无偏估计还是有效估计
        - std
        - var
    （2）是否需要考虑缺失值
        - decay对缺失值赋予零值，这个做法是否合理
        - 其余函数未对缺失值做处理，其中相关系数函数和线性回归函数对缺失值是否采取对应值全部删除,排序函数缺失值是当做最大、最小或者删去，
        - 权重函数缺失值是将缺失值的权重略过还是顺延到下一个有值处
        对无缺失值的情况已完成校验
    使用方法：
    1.input: SequenceDataPanel，SequenceDataPanel是二维数组，以numpy为底层，numpy的函数都可以用
    2.output: 一维数组，对应传入的SequenceDataPanel最新的计算结果，每调用一次计算函数输出一行（一维数组）
'''

import numpy as np


def Power(x):
    raw = x[-1, :]
    return np.power(raw, 2)


def cmpMin(x, y):
    '''
    对传入的SequenceDataPanel的最新一行进行比对
    '''
    xraw = x[-1, :]
    yraw = y[-1, :]
    return xraw * (xraw <= yraw) + yraw * (yraw < xraw)


def cmpMax(x, y):
    xraw = x[-1, :]
    yraw = y[-1, :]
    return xraw * (xraw >= yraw) + yraw * (yraw > xraw)


def Delay(x, num, fillNan=None):
    raw = x[:-num, :]
    cols = raw.shape[1]  # 判断数组是否为空
    if raw.size == 0:
        raw = np.zeros(cols)  # 填充空数组
        return raw
    else:
        raw = raw[-1, :]
    if fillNan is not None:
        raw[raw.isnull()] = fillNan
    return raw


def Diff(x, num, fillNan=None):
    raw = x - Delay(x=x, num=num, fillNan=fillNan)
    raw = raw[-1, :]
    return raw


def Max(x, num, minobs=0):
    raw = x[-num:, :].max(axis=0)
    return raw


def Min(x, num, minobs=0):
    raw = x[-num:, :].min(axis=0)
    return raw


def Median(x, num, minobs=0):
    raw = x[-num:, :]
    rowlen = raw.shape[0]
    rawSort = np.sort(raw, axis=0)
    if rowlen % 2 == 0:
        row1 = rowlen / 2 - 1
        row2 = rowlen / 2
        raw = (rawSort[row1, :] + rawSort[row2, :]) / 2
    else:
        row = int((rowlen + 1) / 2 - 1)
        raw = rawSort[row, :]
    return raw


def Mean(x, num, minobs=0):
    raw = x[-num:, :].mean(axis=0)
    return raw


def Sum(x, num, minobs=0):
    raw = num * Mean(x=x, num=num, minobs=minobs)
    return raw


def Std(x, num, ddof=1, minobs=0):
    '''
    采取有效估计
    采用无偏估计时需修改函数
    '''
    raw = x[-num:, :].std(axis=0)
    return raw


def Var(x, num, ddof=1, minobs=0):
    '''
    采取有效估计
    采用无偏估计时需修改函数
    '''
    mean = Mean(x=x, num=num)
    raw = x[-num:]
    raw = Mean((raw - mean) ** 2, num=num)
    return raw


def Skew(x, num, minobs=0):
    '''
    计算偏度
    三阶中心距除以标准差的三次方
    '''
    std3 = Std(x=x, num=num, minobs=minobs) ** 3  # 计算标准差的三次方
    mean = Mean(x=x, num=num, minobs=minobs)  # 计算期望
    cd3 = Mean((x - mean) ** 3, num)  # 计算三阶中心距
    raw = np.divide(cd3, std3)
    return raw


def Kurt(x, num, minobs=0):
    '''
    计算峰度
    四阶中心距与标准差四次方的比值减去3
    '''
    std4 = Std(x=x, num=num, minobs=minobs) ** 4  # 计算标准差的四次方
    mean = Mean(x=x, num=num, minobs=minobs)  # 计算期望
    cd4 = Mean((x - mean) ** 4, num)  # 计算四阶中心距
    raw = np.divide(cd4, std4) - 3
    return raw


def Countif(condition, num, minobs=0):
    raw = Sum(x=condition, num=num, minobs=minobs)
    return raw


def Sumif(x, condition, num, minobs=0):
    raw = Sum(x * condition, num)
    return raw


def Sma(x, n, m):
    '''
    加权移动平均数,x为输入的属性，n为计算所需数据最大长度，m为权值，n-m+1为实际所需数据长度
    Sma(x(t),n,m)=m/n*x+(n-m)/n*Sma(x(t-1),n-1,m)
    计算思路，先计算各值对应的权值alpha，然后用权值乘对应的属性值
    '''
    assert n >= m
    num = n - m + 1
    x = x[-num:, :]
    alpha1 = np.zeros(shape=num)
    alpha2 = np.zeros(shape=num)
    alpha1[0] = 1
    for i in range(1, num):
        alpha1[i] = alpha1[i - 1] * (n - (i - 1) - m) / (n - (i - 1))  # 权值的前部
    for i in range(num):
        alpha2[i] = m / (n - i)  # 权值的后部
    alpha = alpha1 * alpha2
    xT = x.T
    rawT = xT * alpha
    raw = rawT.T
    raw = raw.sum(axis=0)
    return raw


def Wma(x, num, pct, weightType=False, minobs=0):
    """
    计算前 num期样本加权平均值
    :param x:
    :param num:
    :param pct:
    :param weightType:  权重方式
    :param minobs:
    :return:
    """
    if weightType == 'exp':
        weights = pct ** np.array((range(num - 1, -1, -1)))
        toWeight = (1 - pct ** num) / (1 - pct)
    elif weightType == 'halflife':
        rate = -np.log(2) / pct
        weights = np.exp(rate * np.array(range(num - 1, -1, -1)))
        toWeight = np.sum(weights)
    else:
        raise NotImplementedError
    x = x[-num:, :]
    x = x.T
    rawT = x * weights
    raw = rawT.T
    raw = raw.sum(axis=0)
    return raw


def Decaylinear(x, n):
    """
    对 A 序列计算移动平均加权
    权重对应 d,d-1,…,1/ sum(1-d)（权重和为 1）
    """
    x = x[-n:, :]
    dsum = sum(np.array(range(1, n + 1)))
    alpha = np.zeros(shape=n)
    for i in range(1, n + 1):
        alpha[i - 1] = i / dsum
    xT = x.T
    rawT = xT * alpha
    raw = rawT.T
    raw = raw.sum(axis=0)
    return raw


def TsToMin(x, num, minobs=0):
    """
    计算输入的x前num行每列数据中最小值所处的位置,出现相同值时，取间隔小的
    """
    x = x[-num:, :]
    collen = x.shape[1]
    raw = np.zeros(shape=collen)
    for i in range(collen):
        colmin = x[:, i].min()
        minIdx = np.where(x[:, i] == colmin)
        minIdx = minIdx[0][-1]
        raw[i] = raw.shape[0] - minIdx
    return raw


def TsToMax(x, num, minobs=0):
    """
    计算 当前值 距离窗口期内最大值之间的间隔数, 当前值本身也算一位
    当出现相同值时，取间隔小的
    """
    x = x[-num:, :]
    collen = x.shape[1]
    raw = np.zeros(shape=collen)
    for i in range(collen):
        colmin = x[:, i].max()
        maxIdx = np.where(x[:, i] == colmin)
        maxIdx = maxIdx[0][-1]
        raw[i] = raw.shape[0] - maxIdx
    return raw


def FindRank(x, num, minobs=0, pct=False):
    """
    计算当前值在过去n天的顺序排位，最小值排名为1
    """
    xNow = x[-1:, :]
    x = x[-num:, :]
    x = np.sort(x, axis=0)  # 对x进行排序并生成新的二维数组
    col = xNow.shape[1]
    raw = np.zeros(shape=col)
    for i in range(col):  # 在排序后的列表中寻找当前值的序列
        xi = xNow[0, i]
        xrank = np.where(x[:, i] == xi)[0][0]  # 取出序列值
        raw[i] = xrank + 1
    return raw


def Rank(x):
    """
    不应再时间序列排序中使用该函数，应用于截面排序（升序）。对x进行排序获得新数组，再取得值在新数组的排列次序
    """
    x = x[-1, :]
    xSort = np.sort(x)
    col = len(x)
    rank = np.zeros(shape=col)
    for i in range(col):
        rank[i] = np.where(xSort == x[i])[0][0] + 1
    raw = rank
    return raw


def Corr(x, y, num, minobs=2):
    denominator = Std(x, num, minobs) * Std(y, num, minobs)
    xMean = Mean(x, num)
    yMean = Mean(y, num)
    numerator = Mean(((x - xMean) * (y - yMean)), num)
    return numerator / denominator


def AlphaBetaSigma(x, y, num, minobs=2):
    xraw = x[-num:, :]
    yraw = y[-num:, :]
    numerator = Mean(xraw * yraw, num, minobs) - Mean(xraw, num, minobs) * \
                Mean(yraw, num, minobs)
    denominitor = Var(xraw, num)
    beta = numerator / denominitor  # 计算斜率
    xMean = Mean(xraw, num, minobs)
    yMean = Mean(yraw, num, minobs)
    alpha = yMean - xMean * beta  # 计算截距
    ssX = Mean(xraw ** 2, num, minobs)
    ssY = Mean(yraw ** 2, num, minobs)
    sXY = Mean(xraw * yraw, num, minobs)
    sigma = ssY + ssX * beta ** 2 + alpha ** 2 - 2 * beta * sXY - 2 * alpha * yMean + 2 * alpha * beta * xMean
    return alpha, beta, sigma


def RegBeta(x, y, num):
    alpha, beta, sigma = AlphaBetaSigma(x=x, y=y, num=num)
    return beta


def RegAlpha(x, y, num):
    alpha, beta, sigma = AlphaBetaSigma(x=x, y=y, num=num)
    return alpha


def RegSi(x, y, num):
    alpha, beta, sigma = AlphaBetaSigma(x=x, y=y, num=num)
    return sigma
