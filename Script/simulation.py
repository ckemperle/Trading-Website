from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import math
import numpy as np
from joblib import Parallel, delayed
import plotly.express as px
import plotly


class Simulation():
    def __init__():
        pass

    def simulation(begin_date, end_date, all_comps, budget, z_i=0, z_ii=1, z_ii_a=0.8,  z_ii_b=None, stck = 200, r_bord = 200, p_mitn = 40, plotlyPlt=True):

        def multiProcessComp(u, all_comps, begin_date):
            df_use=all_comps[(all_comps['date'] >= (begin_date - relativedelta(years=1) + relativedelta(weeks=u))) & (all_comps['date'] <= (begin_date + relativedelta(weeks=(u))))]
            groups=df_use.groupby('name')
            allStocks = pd.DataFrame([], columns=['price_close', 'var_ges', 'var_gew', 'mittel', 'median', 'min', 'max', 'price_b_median', 'price_b_mittel', 'i_week', 'ii_week','iii_week','iv_week','v_week','vi_week','vii_week','viii_week','i_week_rise','lastDay'], index=list(groups.groups.keys()))

            price_close=[]
            date=[]
            for comp in list(groups):
                price_close.append(comp[1].iloc[:,0])
                date.append(comp[1].loc[:,'date'])
            price_close=list(zip(*price_close))
            date=list(zip(*date))

            allStocks.iloc[:,0]=np.array(price_close[0])
            allStocks.iloc[:,1]=groups.var().values
            allStocks.iloc[:,2]=math.sqrt(groups.var().values[0])/groups.mean().values
            allStocks.iloc[:,3]=groups.mean().values
            allStocks.iloc[:,4]=groups.median().values
            allStocks.iloc[:,5]=groups.min().price.values
            allStocks.iloc[:,6]=groups.max().price.values
            allStocks.iloc[:,7]=price_close[0]<groups.median().values.reshape(groups.median().values.shape[0],)
            allStocks.iloc[:,8]=price_close[0]<groups.mean().values.reshape(groups.mean().values.shape[0],)
            allStocks.iloc[:,9]=[(price1-price2)/price2 for price1,price2 in zip(price_close[0],price_close[5])]
            allStocks.iloc[:,10]=[(price1-price2)/price2 for price1,price2 in zip(price_close[5],price_close[10])]
            allStocks.iloc[:,11]=[(price1-price2)/price2 for price1,price2 in zip(price_close[10],price_close[15])]
            allStocks.iloc[:,12]=[(price1-price2)/price2 for price1,price2 in zip(price_close[15],price_close[20])]
            allStocks.iloc[:,13]=[(price1-price2)/price2 for price1,price2 in zip(price_close[20],price_close[25])]
            allStocks.iloc[:,14]=[(price1-price2)/price2 for price1,price2 in zip(price_close[25],price_close[30])]
            allStocks.iloc[:,15]=[(price1-price2)/price2 for price1,price2 in zip(price_close[30],price_close[35])]
            allStocks.iloc[:,16]=[(price1-price2)/price2 for price1,price2 in zip(price_close[35],price_close[40])]
            allStocks.iloc[:,17]=[(price1-price2)>0 for price1,price2 in zip(price_close[0],price_close[5])]
            allStocks.iloc[:,18]=date[0]

            rat_var= [1+value/maximum*9 for value,maximum in zip(allStocks.loc[:,'var_gew'].values, [max(allStocks.loc[:,'var_gew'].values)]*len(allStocks.loc[:,'var_gew'].values))] #rescale values to 1-10
            rat_per=(allStocks.i_week * 0.40157 + allStocks.ii_week * 0.20078+ allStocks.iii_week * 0.10039+ allStocks.iv_week * 0.05020+ allStocks.v_week * 0.02510+ allStocks.vi_week * 0.01255+ allStocks.vii_week * 0.00627+ allStocks.viii_week * 0.00314)/8 *100 * z_ii_a + z_ii_b * allStocks.price_b_median
            rat_per= [1+value/maximum*9 for value,maximum in zip(rat_per.values, [max(rat_per.values)]*len(allStocks.iloc[:,0].values))] #rescale values to 1-10
            allStocks['rating']=z_i*rat_var+z_ii*rat_per
            allStocks['rank']=allStocks.rating.rank(ascending=False)
            return allStocks

        def run(weeksDiff, portfolio, allStocksList, budget, stck, r_bord, p_mitn):
            for u in range(weeksDiff):
                allStocks=allStocksList[u]

                if u==0:
                    work_p=portfolio.copy()
                    allStocks.reset_index(inplace=True)
                    allStocks=allStocks.rename({'index': 'name1'},axis=1)
                    good=allStocks.sort_values(by='rating', ascending=False)
                    t=0
                    while budget>=stck and t<len(allStocks.index):
                        budget-=stck
                        buy_date=good.lastDay[t]
                        portfolio=portfolio.append({'name1': good.name1[t], 'name2': good.name1[t], 'buy_price': good.price_close[t], 'buy_date': buy_date}, ignore_index=True)
                        t+=1
                    portfolio=portfolio[1:len(portfolio.name1)]

                else:
                    allStocks.reset_index(inplace=True)
                    allStocks=allStocks.rename({'index': 'name1'},axis=1)
                    if (len(portfolio.columns) ==  len(allStocks.columns)) and (portfolio.columns == allStocks.columns):
                        portfolio=allStocks.copy()
                    else:
                        portfolio=pd.merge(portfolio, allStocks, on='name1', how='left', suffixes=('_no',''))
                        portfolio=portfolio.loc[:,~ portfolio.columns.str.endswith('_no')]
                    portfolio['profit']=[stck*((priceClose-buyPrice)/buyPrice) for priceClose,buyPrice in zip(portfolio.price_close, portfolio.buy_price)]
                    work_p=portfolio.copy()
                    good=allStocks.sort_values(by='rating', ascending=False)
                    sell=work_p[(work_p['rank']>r_bord) | (work_p.profit>p_mitn)]
                    if len(sell) != 0:
                        for close,buy in zip(sell.price_close, sell.buy_price):
                            profit=stck*((close-buy)/buy)
                            budget+=stck+profit
                    portfolio=work_p.iloc[np.where(portfolio.name1 == [name for name in portfolio.name1 if name not in sell.name1])][:5]
                    t=0
                    while budget>=stck and t<len(allStocks.index):
                        if np.any(good.name1[t]==portfolio.name1) == False:
                            budget-=stck
                            buy_date=good.lastDay[t]
                            portfolio=portfolio.append({'name1': good.name1[t], 'name2': good.name1[t], 'buy_price': good.price_close[t], 'buy_date': buy_date}, ignore_index=True)
                            t+=1
                        else:
                            t+=1
                if u==weeksDiff-1:
                    yield portfolio, allStocks, int(budget), allStocks.lastDay[0]
                else:
                    yield int(budget), allStocks.lastDay[0]

        if z_ii_b == None:
            z_ii_b=1-z_ii_a

        begin_date=datetime.strptime(begin_date, '%Y-%m-%d')
        end_date=datetime.strptime(end_date, '%Y-%m-%d')
        all_comps['date']=pd.to_datetime(all_comps['date'])

        weeksDiff = ((end_date-begin_date).days//7)+1
        portfolio = pd.DataFrame([], columns=['name1', 'name2', 'buy_price', 'buy_date', 'profit'])

        try:
            allStocksList=Parallel(n_jobs=-2)(delayed(multiProcessComp)(u, all_comps=all_comps, begin_date=begin_date) for u in range(weeksDiff))
        except:
            allStocksList=Parallel(n_jobs=1)(delayed(multiProcessComp)(u, all_comps=all_comps, begin_date=begin_date) for u in range(weeksDiff))

        result=list(run(weeksDiff=weeksDiff, portfolio=portfolio, allStocksList=allStocksList, budget=budget, stck=stck, r_bord=r_bord, p_mitn=p_mitn))
        portfolio, allStocks, budget = result[-1][0], result[-1][1], result[-1][2]

        if len(portfolio.columns) == len(allStocks.columns) and portfolio.columns == allStocks.columns:
            sell=allStocks.copy()
        else:
            portfolio=pd.merge(portfolio, allStocks, on='name1', how='left', suffixes=('_no',''))
            portfolio=portfolio.loc[:,~ portfolio.columns.str.endswith('_no')]
            sell=portfolio.copy()

        for buy,close in zip(sell.buy_price, sell.price_close):
            profit=stck*((close-buy)/buy)
            budget+=stck+profit


        if plotlyPlt:
            resultPlot=result[:-1].copy()
            resultPlot.append(result[-1][2:4])
            resultPlot=list(zip(*resultPlot))

            print(resultPlot[1])
            fig = px.line(x=resultPlot[1],y=resultPlot[0], labels={'x':'Date', 'y':'Revenue €'})
            fig.update_traces(mode='markers+lines')
            fig.update_layout(
                    autosize=False,
                    width=1000,
                    height=410)

            plotCode = plotly.offline.plot(fig, include_plotlyjs='cdn', output_type='div')
            plotCode=plotCode.replace('"', "'")

        return budget, plotCode
