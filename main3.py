import datetime as dt
import pandas as pd
import numpy as np
from WindPy import *
from fangfa5 import * #备注：这里引入fangfa4好像没有用，在database7.py改了才行
from database8 import *
from strategies import *
import matplotlib.pyplot as plt

#数据库
################################################################################################################################################################
cb = cb_data()

#指标计算函数 避免陷入循环，用矩阵计算
################################################################################################################################################################
# 求均价，非异常样本
(cb.matNormal * cb.Close).apply(np.mean, axis=1) #axis=1表示逐行读取，=0则表示逐列读取。 map是针对单列，apply是针对单列或者多列，applymap是针对全部元素
# 求平价90~110元转债平均溢价率 
(cb.matNormal * cb.ConvV.applymap(lambda x: 1 if 90 <= x < 110 else np.nan) * cb.ConvPrem).apply(np.mean, axis=1) #1.筛选非异常转债 2.平价90~110元转债 3.挑出以上转债的溢价率 最后求均值
# 求10日平均隐含波动率
(cb.matNormal * cb.ImpliedVol).apply(np.mean, axis=1).rolling(10).mean()

#单策略
################################################################################################################################################################
easyBall(cb, codes = cb.codes, date = "2022/9/26", tempCodes = cb.codes) #原文easyBall参数列表与现在很不一样，具体参数怎么传的还要复习一下
测算结果 = frameStrategy(cb, "2022/8/1", selMethod = easyBall, defineMethod = 'default', weightMethod = 'fakeEv', roundMethod = 'daily')
测算结果.plot(figsize=(12, 5))

# ROE测算 = frameStrategy(cb, selMethod = ROE策略)
# plt.show()

#合成策略
################################################################################################################################################################
合成策略 = synthesis(cb, [easyBall, 动量250日]).func
合成策略测算 = frameStrategy(cb, selMethod=合成策略, roundMethod=42)
合成策略测算.plot(figsize=(10,5))
plt.show()
