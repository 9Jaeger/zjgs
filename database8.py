import pandas as pd
from WindPy import *
import numpy as np
from warnings import simplefilter
from fangfa5 import *
simplefilter(action='ignore', category=FutureWarning)

class cb_data(object):

    def __init__(self):
        if not w.isconnected(): w.start()
        self.DB = {} 
        self.loadBaseData(create = False, update =False) 

    def loadBaseData(self, create = False, update = True):
        if create:
            self.dailyData()
            self.PanelData()
        else:
            if update:
                self.update(2022/9/26) 
            else:
                self.readData() 

#下载动态基础数据库
####################################################################################################################################################################################################################
    def dailyData(self, period = False):
        para = pd.read_excel("参数.xlsx",index_col=0)
        for k,v in para.iterrows():
            if period:
                self.DB[k] = readTable(getCodeList(),v["字段(Wind)"],"2022/8/1", "2022/9/8")
            else:
                self.DB[k] = readTable(getCodeActive(),v["字段(Wind)"],"2022/8/1", "2022/9/8") 
            self.DB[k].to_csv(v["文件名"],encoding="gbk")   

#下载静态基础数据库
####################################################################################################################################################################################################################
    def PanelData(self, codes = None): 
        '''
        下载静态数据。 
        '''
        date = pd.to_datetime(self.date).strftime("%Y%m%d") 
        if codes is None: codes = self.codes     
        jingtai = pd.read_excel("静态参数.xlsx", index_col=0)
        _, self.panel = w.wss(codes, ",".join(jingtai["字段(Wind)"]), f"tradedate={date}", usedf=True) 
        self.panel.columns = list(jingtai.index) 
        self.panel.to_csv("静态数据.csv", encoding="gbk", mode = 'a')
        return self.panel

#后续更新之前读入基础数据库
####################################################################################################################################################################################################################
    def readData(self):
        self.dfParams = pd.read_excel('参数.xlsx', index_col = 0) 
        self.readDailyData()
        self.readPanelData() 

    def readDailyData(self):
        '''
        读入动态数据基础数据库
        '''
        for k, v in self.dfParams.iterrows(): 
            self.DB[k] = pd.read_csv(v['文件名'], index_col = 0)
            self.DB[k].index = pd.to_datetime(self.DB[k].index, format='%Y/%m/%d')

    def readPanelData(self):
        '''
        读入静态数据基础数据库
        '''
        self.panel = pd.read_csv("静态数据.csv", encoding="gbk")

#后续更新
####################################################################################################################################################################################################################
    def update(self, day): 
        self.updatetheActive(self, day)
        self.updatePanelData()
        self.updateSyn() 

    def updatetheActive(self, day = '2022/9/26', method='wind-api'):
        '''
        更新某日正在上市交易交易的品种的数据，默认直接插入新券，从上次最后一日开始更新
        '''
        new_codes=getCodeActive(day) 
        for key,value in self.DB.items(): 
            field = self.dfParams.loc[key, '字段(Wind)']
            dates = w.tdays(self.DB[key].index[-1], day).Data[0] 
            start=dates[1]
            #if method == 'wind-api':
            kwargs = "rfIndex=1" if field == "impliedvol" else None 
            df = readTable(new_codes, field, start, day, kwargs)   
            # elif method == "sql":        
            #     df = readFromSQL(new_codes, field, start, day)
            df.index=pd.to_datetime(df.index)
            value = pd.concat([value, df], axis=0, join='outer')
            self.DB[key] = value
            print(f'{key} 更新已完成')

    def updatePanelData(self, new_codes = None): 
        '''
        更新面板数据2 静态数据也可以用dfParmas
        '''
        if new_codes is None: new_codes = self.codes 
        diff = list(set(new_codes) - set(self.panel.index)) 
        if diff:
            dfNew = self.PanelData(diff)
            self.panel = self.panel.append(dfNew)

#输出动态数据文件到本地。 
####################################################################################################################################################################################################################
    def updateSyn(self):
        for k, v in self.dfParams.iterrows():
            self.DB[k].index = pd.to_datetime(self.DB[k].index, format='%Y/%m/%d')
            self.DB[k].to_csv(v["文件名"], encoding="gbk")
        self.panel.to_csv("静态数据.csv", encoding="gbk", mode = 'a')   #备注教程：pandas to_csv追加更新

#其他设置
####################################################################################################################################################################################################################
    def __getitem__(self, key): 
        return self.DB[key] if key in self.DB.keys() else None
    def __getattr__(self, key):
        return self.DB[key] if key in self.DB.keys() else None
    
    @property 
    def date(self):
        return self.DB["Amt"].index[-1] 
    @property 
    def start_date(self):
        return self.DB["Amt"].index[0] 
    
    #调用codes
    @property
    def codes(self):
        return list(self.DB["Amt"].columns)
    @property
    def codes_active(self):
        srs = self.DB["Amt"].loc[self.date, self.codes] 
        return list(srs[srs > 0].index)

    #样本选择相关的函数 
    @property
    def matTrading(self):  
        return self['Amt'].applymap(lambda x:1 if x > 0 else np.nan)  
    @property
    def matNormal(self):  
        matTurn = self.DB['Amt'] * 10000.0 / self.DB['Outstanding'] / self.DB['Close'] 
        matEx = (matTurn.applymap(lambda x: 1 if x > 100 else np.nan) * \
                self.DB["Close"].applymap(lambda x: 1 if x > 135 else np.nan) * \
                self.DB["ConvPrem"].applymap(lambda x: 1 if x >35 else np.nan)
                ).applymap(lambda x: 1 if x != 1 else np.nan)  
        return self.matTrading * matEx        

####################################################################################################################################################################################################################
def readTable(codes, field, start, end, *others):  
    '''
    用于下载动态数据，在初次下载、后续下载中均用到
    '''
    _, df =w.wsd(','.join(codes), field, start, end, *others, usedf=True) 
    return df

def getCodeList(start = "2022/8/1", end = "2022/9/8"):  
    '''
    给readTable函数传递代码列表，此处传递的是于start-end期间新上市的转债
    '''
    if not w.isconnected():
        w.start()
    _, dfIssue = w.wset("cbissue", "startdate=" + start + ";enddate=" + end, usedf=True)
    dfIssue = dfIssue.loc[dfIssue["issue_type"]!="私募"] 
    dfIssue.to_csv("issue.csv", encoding="gbk") 
    return list(dfIssue["bond_code"])

def getCodeActive(day = '2022/9/8'):  
    '''
    提取特定日在交易的CB和EB，wind路径是wset→板块成分→债券分类（证监会）→可转债行情(沪深公募)/→公开发行公司债→可交换债
    '''
    if not w.isconnected(): w.start()
    _, dfActCB = w.wset("sectorconstituent", "date=2022/9/8; sectorid=1000047016000000", usedf=True) 
    return list(dfActCB['wind_code'])

if __name__ == '__main__':
    cb = cb_data() #动态、静态基础数据库