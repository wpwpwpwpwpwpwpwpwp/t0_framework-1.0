# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 15:57:15 2019

@author: sjhself
"""
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt


# 只需更改需要检验的风格因子的名称和自己的因子名称，如sjh_001和FC
factorData=pd.read_hdf( r'..\FactorPool\factors_wangjp_data\sjh_001\factor_scores_zscore.h5')
IndexData=pd.read_hdf(r'..\FactorPool\factors_wangjp_data\FC\factor_scores_zscore.h5')

Corr=[]
for i in range(0,len(factorData.index),1):
    X=factorData.iloc[i]
    Y=IndexData.iloc[i]
    Corr.append(X.corr(Y,method="pearson"))

Corr_Series = pd.Series(Corr,index=factorData.index)
Corr_Series.index = pd.to_datetime(Corr_Series.index)
Corr_Month = Corr_Series.resample('M',kind='period').mean()
print(Corr_Month)
plt.show(Corr_Month.plot())