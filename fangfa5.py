import datetime as dt
import pandas as pd
import numpy as np
from WindPy import *
from database8 import *


def frameStrategy(obj, start = '2022/8/1', defineMethod = 'default', selMethod = None, weightMethod = 'average', roundMethod = 'daily'):
    '''
    主函数
    默认初始设置：因为输入参数绝不止obj和起始时间start。结合上面几个小函数的讨论，至少这几个参数是可以留给投资者自设的（当然沿用默认设置也没问题）：1、defineCodes中的method，用来调整择券范围；2、selectCode中的method，用来调整核心策略；3、getWeight中的method，用来调整加权方法；4、调仓周期的参数。
    '''
    intStart = getStartLoc(obj, start)  
    dfRet = pd.DataFrame(index = obj.DB['Amt'].index[intStart:], columns = ['NAV','LOG:SEL','LOG:WEIGHT']) 
    dfAssetBook = pd.DataFrame(index = ['Nothing'], columns = ['costPrice','w'])  
    cash = 100.0 
    codes = defineCodes(obj, defineMethod) 
    isAdjustDate = roundOfAdjust(obj, start, roundMethod)

    for date in dfRet.index:  
        checkBook(obj, dfRet, dfAssetBook, cash, date)  
        if date in isAdjustDate: 
            sel = selectCodes(obj, codes, date, selMethod) 
            if sel:
                w = getWeight(obj, sel, date, weightMethod) 
            else:
                sel = ['Nothing']
                w = 0.0
            dfAssetBook = pd.DataFrame(index = sel, columns = ['costPrice', 'w'])
            dfAssetBook['costPrice'] = 100.0
            dfAssetBook['w'] = w

    dfRet['LOG:SEL'][date] = ','.join(list(dfAssetBook.index))
    dfRet['LOG:WEIGHT'][date] = ','.join([str(t) for t in list(dfAssetBook['w'])]) 
    return dfRet

def getStartLoc(obj, date):
    '''
    取日期index所在的位置
    pd.DataFrame的index有一个get_loc的方法也能得到这个结果，但早期的版本没考虑过万一要找的变量不在index中怎么办。而后来的版本中，虽然给予了一定容忍度，但也基本没考虑过当index本身是不可比变量时的处理。所以此时我们要进行简单的改造
    '''
    if date in obj.DB['Amt'].index:    
        i = obj.DB['Amt'].index.get_loc(date) 
    else:
        fakeIndex = obj.DB['Amt'].index.map(str2dt) 
        i = fakeIndex.get_loc(str2dt(date),method = 'ffill')
    return i

def str2dt(strTime):
    time = dt.datetime.strptime(strTime,'%Y-%m-%d') 
    return time

def defineCodes(obj, defineMethod = 'default'):
    '''
    定义个券大致范围
    一般要剔除因股改而退市的那些转债，有时候我们也希望剔除EB。投资者也可以设定其他的规则
    '''
    if defineMethod == 'default':
        return obj.excludeSpecial() 
    elif defineMethod =='nonEB':
        return obj.excludeSpecial(hasEB=0)
    elif hasattr(defineMethod,'__call__'):
        return defineMethod(obj) 

def excludeSpecial(obj, hasEB = 1): 
    IsSpecial = []  
    columns = set(list(obj.DB['Amt'].columns)) 
    columns -= set(IsSpecial) 
    columns = list(columns)
    if not hasEB: 
        for code in columns:
            if code[:3] == '132' or code[:3] == '120': 
                columns.remove(code)
    return columns

def selectCodes(obj, codes, date, selMethod=None):
    '''
    择券的代码，也是对策略决定意义最大的函数。在调仓日期会调用这个函数。同样，为了给予投资者外部接口，这里也要保留传入函数的可能性。
    '''
    i = getStartLoc(obj, date)
    n = min([i, 5]) 
    condition = (obj.DB['Amt'].iloc[i-n:i][codes].fillna(0).min()>100000.0)&(obj.DB['Outstanding'].iloc[i][codes] > 30000000.0) 
    if selMethod: 
        tempCodes = list(condition[condition].index)
        moreCon = selMethod(obj, codes, date, tempCodes)
        condition &= moreCon  
    retCodes = list(condition[condition].index)
    if not retCodes:    
        print('its a empty selection, when date: ',date)
    return retCodes

def getWeight(obj, codes, date, method = 'average'):
    if method == 'average':
        ret = pd.Series(np.ones(len(codes))/float(len(codes)), index = codes) 
        return ret
    elif method == 'fakeEv':
        srsIssue = get_issueamount(codes)  
        srsFakeEv =  obj.DB['Close'].loc[date,codes] * srsIssue
        return srsFakeEv / srsFakeEv.sum()
    elif method == 'Ev':# 市值加权
        srsOutstanding = obj.DB['Outstanding'].loc[date,codes]
        srsEv = obj.DB['Close'].loc[date, codes] *srsOutstanding
        return srsEv / srsEv.sum()
    elif hasattr(method,'__call__'):
        return method(obj, codes, date) 

def get_issueamount(codes):
    '''
    codes：list型，代码列表
    输出srsIssue
    '''
    #if not w.isconnected(): w.start()
    strCode=','.join(codes)
    data=w.wss(strCode,'issue_amountact').Data[0]
    srsIssue = pd.Series(data, index = codes)
    return srsIssue

def roundOfAdjust(obj, start, method = 'daily'):
    '''
    调仓周期函数：这里只留了两种形式，一种是每日调仓（但这个其实没有想象中那么实用），另一种是每隔固定交易日调仓一次。
    '''
    i = getStartLoc(obj, start)  
    if method == 'daily':
        return obj.DB['Amt'].index[i:]
    elif isinstance(method, int):
        return obj.DB['Amt'].index[i:][::method] 

def checkBook(obj, dfRet, dfAssetBook, cash, date, cashRate = 0.03):
    '''
    对于账簿的每日处理函数
    '''
    if date == dfRet.index[0]:
        dfRet.loc[date]['NAV'] = 100 
    else:
        i = dfRet.index.get_loc(date)         
        j = obj.DB['Close'].index.get_loc(date) 
        if len(dfAssetBook.index) == 1 and dfAssetBook.index[0] == 'Nothing':
            dfRet.iloc[i]['NAV'] = dfRet.iloc[i-1]['NAV'] * (1 + cashRate / 252.0)
            cash *= 1 + cashRate / 252.0
        else:
            codes = list(dfAssetBook.index)
            srsPct = obj.DB['Close'].iloc[j-1:j+1][codes].pct_change().iloc[-1] + 1.0
            cashW = 1 - dfAssetBook['w'].sum() 

            t1 = (srsPct * dfAssetBook['costPrice'] * dfAssetBook['w']).sum() + cash * cashW * (1 + cashRate) 
            t0 = (dfAssetBook['costPrice'] * dfAssetBook['w']).sum() + cash * cashW 
            dfRet.iloc[i]['NAV'] = dfRet.iloc[i-1]['NAV'] * t1 / t0 
            cash *= 1 + cashRate 




def checkCall(database, dfRet, dfAssetBook, trade_dt, reBalanced):
    '''
    明确我们要做的工作后，我们实际的程序逻辑很直白——先获取每日更新的转债赎回情况列表，对照我们当日持仓组合，将其中在前一交易日公告赎回的转债进行剔除。再根据现有的转债列表，我们再进行仓位的平衡或留空。
    :param database: 存储转债标化数据的数据库
    :param dfRet: 存储策略净值表现的dataframe
    :param dfAssetBook: 存储当期策略选择的券及权重
    :param trade_dt: 交易日
    :param reBalanced: 是否对于仓位进行再平衡
    :return: 策略新选择的券与对应权重
    '''
    # 获取赎回公告，index为各已公告赎回的转债代码，announceDate列表示赎回公告发布时间
    if not hasattr(database, 'callTable'): 
        database.callTable = getCallTable(database.tickers)  
    if trade_dt == dfRet.index[0]:
        return dfAssetBook
    else:
        tradeInt = int(pd.to_datetime(trade_dt).strftime('%Y%m%d'))
        called = database.callTable[database.callTable['announceDate'] < tradeInt].index  
        reSel = [ticker for ticker in dfAssetBook.index if ticker not in called]       
        reSel = []
        for ticker in dfAssetBook.index:
            if ticker not in called:
                reSel.append(ticker)
        if reBalanced & (len(dfAssetBook) != len(reSel)):     #备注：人为设置reBalanced对照，再平衡和不再平衡对照 =1再平衡 =0直接卖
            reWgt = getWeight(database, reSel, trade_dt, weightMethod)
            dfAssetBook = pd.DataFrame(index=reSel, columns=['costPrice', 'w', 'betas'])
            dfAssetBook['costPrice'] = 100. 
            dfAssetBook['w'] = reWgt
        else
            dfAssetBook = dfAssetBook.loc[reSel]     
    return dfAssetBook