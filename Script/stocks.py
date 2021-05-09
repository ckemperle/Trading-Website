#%% Import Libraries
# for scraping and main application
import time
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import yahoofinance
#%% Main Class for web scraping and data saving#################################
class _StockDownload():

    stockList = 'Nasdaq 100 as nasdaq,\nSP 500 as sp,\nStoxx 600 as stoxx,\nNikkei 225 as nikkei\n'

    def __init__(self, name):
        self._name = name
        self._indexNames = list()
        self._nameSymbol = [[], []]

        self._date = None
        self._endDate = None

        self._week = False
        self._us = True

        self.df = None
        self.companies = None

    ########################################################################################
    # -----StockDonwload function (WORKING FOR NASDAQ and SP500 [Every US Index])-----------#
    ########################################################################################
    def _stockNames(self):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}
        website = requests.get(f'https://www.slickcharts.com/{self._name}', headers=headers).text
        soup = BeautifulSoup(website, 'html.parser')
        scrapper = soup.find('table').find_all('a')

        i = 0
        while True:
            try:
                self._nameSymbol[0].append(scrapper[i].text)
                self._nameSymbol[1].append(scrapper[i + 1].text)
                i += 2
            except:
                break
        self.companies =  pd.DataFrame(np.array(self._nameSymbol).T, columns=['Name', 's'])

    def _YahooDataFrame(self):

        def GetYahoo(comp, beginDate, endDate):
            try:
                df = yahoofinance.HistoricalPrices(comp, beginDate,
                                                   endDate).to_dfs()  # get historical data from beginData to endDate
                df = list(df.values())[0].reset_index()  # convert to pd.DataFrame
                df.Date = pd.to_datetime(df.Date)  # make Date a datetime
                df['s'] = comp  # insert column symbol
                return df
            except Exception as e:
                print(e)

        def ToDataFrame(comp, parallelList):
            def ParallelAppend(company, DataFrame, dateVar):
                s, Name = DataFrame[DataFrame['s'] == company]['s'].iloc[0], \
                          DataFrame[DataFrame['s'] == company]['Name'].iloc[0]  # Get Name and Symbol from Company
                df = pd.DataFrame(dateVar).merge(DataFrame[DataFrame['s'] == company], on='Date', how='outer')  # Append
                df['s'], df['Name'] = s, Name
                return df

            companiesDf = comp.merge(pd.concat(parallelList), on='s')  # flatten list of DataFrames to DataFrame
            maxVal = max(companiesDf.groupby('s').size())
            maxComp = companiesDf.groupby('s').size()[companiesDf.groupby('s').size() == maxVal].index[0]  # Get symbol of maximal entries
            dateVar = companiesDf[companiesDf.s == maxComp]['Date']  # Get Date
            shortComp = companiesDf.groupby('s').size()[companiesDf.groupby('s').size() != maxVal].index  # get list of too short companies
            if len(shortComp) > 0:  # if not every company is same size
                shortDf = Parallel(n_jobs=1)(delayed(ParallelAppend)(company=company, DataFrame=companiesDf, dateVar=dateVar) for company in shortComp)
                self.df = companiesDf.merge(pd.concat(shortDf), how='outer')
            else:  # if every company has same size
                self.df = companiesDf


        self._endDate = time.strftime('%Y-%m-%d')

        self.df = Parallel(n_jobs=1)(delayed(GetYahoo)(comp=comp,
                                                beginDate=self._date,
                                                endDate=self._endDate) for comp in self.companies['s'])
        ToDataFrame(comp=self.companies, parallelList=self.df)

        columnsToKeep = ['Close', 's', 'Date', 'Name']  # keep only these values
        self.df = self.df.loc[:, columnsToKeep]
        self.df.columns = ['kurs', 's', 'date', 'Name']  # rename

    def download(self, week=False, day=False, scrapDate=False):
        #check if it first call of download => if not initialize all as None
        if self.df != None:
            self._indexNames = list()
            self._nameSymbol = [[], []]
            self._date = None
            self._endDate = None
            self.df = None
            self.companies = None

        self._week = week

        if self._name == 'nasdaq':
            self._name = 'nasdaq100'
            self._indexNames = ['Nasdaq100', '%5EIXIC']
        elif self._name == 'sp':
            self._name = 'sp500'
            self._indexNames = ['Sp500', '%5EGSPC']
        elif self._name == 'stoxx':
            self._name = 'STOXX-EUROPE-600-7477'
            self._indexNames = ['Stoxx600', '%5ESTOXX']
        elif self._name == 'nikkei':
            self._name = 'NIKKEI-225-4987'
            self._indexNames = ['Nikkei225', '%5EN225']
        else:
            raise Exception(f'\nStockname `{self._name}` is not implemented in the current version!\nStocks that are implemented:\n{_StockDownload.stockList}')

        #set date
        if self._week:
            self._date = (datetime.today() - timedelta(days=datetime.today().weekday())).strftime("%Y-%m-%d")  # timedelta(days = 5) if starting this on saturday => timedelta(days = 4) if starting this on friday evening
        elif day:
            self._date=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif not isinstance(scrapDate, bool):
            self._date=scrapDate
        else:
            self._date = input('Enter date (f.e 2010-01-31 [Year-Month-Day] empty if you want 1 year back) : ')

            # check if date is given, if not set to 1 year past
            if len(self._date) > 0:
                # try if date is really datetime
                try:
                    datetime.strptime(self._date, "%Y-%m-%d")
                except:
                    raise Exception(f'{self._date} is not in predefined format! (see f.e 2010-01-31 [Year-Month-Day])')
            else:
                self._date = (datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d')  # 1 year

        #scrap company names
        if self._us:
            self._stockNames()
        else:
            pass

        #scrap data from yahoo api
        self._YahooDataFrame()

    #dunder Methods
    def __call__(self):
        return self.df

    def __repr__(self):
        print(f'Index: {self._name},\nDate: {self._date} - {self._endDate}\nCompanies:')
        print(*sorted(self._nameSymbol[0]), sep=', \n', end='\n')
        print(f'DataFrame:\n{repr(self.df.head())}')
        return('')

#%% Main class to call from outside
class Stock(_StockDownload):

    def __init__(self, name):
        super().__init__(name)
        pass

# TODO: Implement download for none US Stocks
# TODO: Other method for R code? (not depending on importing os and setting paths)
# TODO: connect to pythonanywhere database (need premium account) and scrap data from there
# TODO: other website than https://www.slickcharts.com for _stockNames function
