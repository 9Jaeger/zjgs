import datetime as dt
import pandas as pd
import numpy as np
from WindPy import *

class synthesis(object): 
   def __init__(self, obj, lstStrategy, kind="Serial"):
        '''
        obj : 数据库变量，cb_data().
        lstStrategy : list, tuple, OrderedDict 策略列表，函数名即可. [策略1, 策略2]
        kind : str, optional 可以是Serial 或者Intersection. The default 默认参数 is "Serial". 
        '''
        obj.strategys = lstStrategy        
        if kind == "Serial":           
            def myFunc(data, codes, date, tempCodes): 
                for s in obj.strategys:
                    codes = s(data, codes, date, tempCodes)
                    if len(codes) == 0:
                        return []                
                return codes     
        elif kind == "Intersection":         
            def myFunc(data, codes, date, tempCodes):
                ret = set(codes)
                for s in obj.strategys:
                    t = s(data, codes, date, tempCodes)
                    ret = ret.intersection(t)       
                return list(ret)     
        else:
            raise ValueError("kind must be one of Serial or Intersection")     
        self.func = myFunc  

def lowPrice(obj, codes, date, tempCodes):
    '''
    下面以低价策略举例，如果我们希望在调仓时买入所有价格低于均价的品种，则可以写下面这个函数，并把lowprice作为selMethod传入上面的函数： 原文是_lowPrice，不知为啥，去掉小杠
    '''
    avgPrice = obj.DB['Close'].loc[date][tempCodes].mean() 
    return obj.DB['Close'].loc[date, codes] <= avgPrice 

def lowPrem(obj, codes, date, tempCodes):
    '''
    比如上面的案例可以作为低价品种等权指数，稍作改动就可以变成“高价指数”。再如，将下面的函数作为selMethod传入框架，可以得到低溢价率品种指数
    '''
    avgPrem = obj.DB['ConvPrem'].loc[date][tempCodes].mean() 
    return obj.DB['ConvPrem'].loc[date, codes] <= avgPrem

def easyBall(obj, codes, date, tempCodes): 
    '''
    EasyBall策略。仅data（数据库变量）、date（假设的日期）、codes（当日可用的转债代码）常用，allCodes（全部历史转债代码）和dfAsset（账户情况）相对不常用
    '''
    avgPrice = obj.DB['Close'].loc[date][tempCodes].mean()
    TR1 = obj.DB['Close'].loc[date, codes] <= avgPrice
    avgPrem = obj.DB['ConvPrem'].loc[date][tempCodes].mean()
    TR2 = obj.DB['ConvPrem'].loc[date, codes] <= avgPrem
    TR = TR1&TR2
    return TR.index


def ROE策略(obj, codes, date, tempCodes): 
    srsROE = factor(codes, 'fa_roenp_ttm', date) 
    return srsROE[srsROE > 10].index

def factor(codes, fields, date, other=''):
    '''
    得到单个因子或者因子组，基于w.wss
    '''
    if len(codes) == 0:
        return pd.Series()
    if other:
        other = ';' + other
    _, dfUnderlying = w.wss(','.join(codes), 'underlyingcode', usedf = True) 
    uniStock = list(set(dfUnderlying.UNDERLYINGCODE))
    date = pd.to_datetime(date).strftime('%Y%m%d')
    _, dfRaw = w.wss(','.join(uniStock), fields, 'tradeDate='+date+other, usedf = True)
    return dfUnderlying.merge(dfRaw, left_on = 'UNDERLYINGCODE', right_index = True).iloc[:,1]

#合成策略
def 动量250日(data, ac, date, codes, ass):
    srs = factor(codes, "TECH_REVS250", date)
    return srs[srs > srs.quantile(0.7)].index

