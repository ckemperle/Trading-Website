from flask import Flask
from flask_restful import Resource, Api, reqparse
import mysql.connector
import plotly.express as px
import plotly
from datetime import datetime
import pandas as pd
import sys

paths =['...']
for path in paths:
    if path not in sys.path:
        sys.path.insert(0, path) #path to Stocks.py

with open('...', 'r') as reader:
    dbVars= reader.read().split('\n')
from simulation import Simulation as Sim

#%%############################################################################
app = Flask(__name__)
api = Api(app)
#%%############################################################################
class DBconn():
    def makeConn(self):
        conn = mysql.connector.connect(
        host=f'{dbVars[0]}',
        user=f'{dbVars[1]}',
        passwd=f'{dbVars[2]}',
        database=f'{dbVars[3]}')
        self.conn = conn
################################################################################
class PortfolioSimulationBridge(DBconn):
        def __init__(self):
            super().__init__()
            self.df=None

        def executeConn(self):
            with self.conn.cursor() as cursor:
                ex = 'select * from stocks where '
                first=True #if first do sql without OR
                for stock in self.stock_list:
                    print(stock) #logging
                    if first:
                        ex += f'name like "{stock}%"'
                        first=False
                    else:
                        ex += f' OR name like "{stock}%"'
                ex+=' order by date asc' #order by date
                cursor.execute(ex)
                self.df = cursor.fetchall()

        def parserInit(self):
            self.parser = reqparse.RequestParser()
            self.parser.add_argument('companies')

        def parserExecute(self):
            self.args = self.parser.parse_args()
            try:
                self.stock_list=self.args['companies'].split(', ')
                if len(self.stock_list) <= 1:
                    sys.exit()
            except:
                self.stock_list=self.args['companies'].split(',')

        def dataframeManipulation(self):
            self.df = pd.DataFrame(self.df, columns=['index', 'price', 'symbol', 'date', 'name']).iloc[:,1:]
            self.df['weekday'] = pd.to_datetime(self.df['date']).dt.strftime('%A')
            self.df = self.df.loc[:, ['price', 'symbol', 'date', 'weekday', 'name']]
#%%############################################################################
class Plot(Resource, DBconn):
    def __init__(self):
        super().__init__()
        self.date=list()
        self.value=list()

    def executeConn(self, stock):
        with self.conn.cursor() as cursor:
            cursor.execute('select date, price from stocks where name like "' + f'{stock}' + '%" order by date asc')
            result = cursor.fetchall()

        if len(result) > 0:
            for date,value in result:
                self.date.append(date.strftime('%Y-%m-%d'))
                self.value.append(value)
        else:
            return 'No Data'

    def get(self, stock_name):
        self.makeConn()
        error=self.executeConn(str(stock_name))

        if len(self.value) > 0:
            fig = px.line(x=self.date, y=self.value, title=f'Stock prices {stock_name}', labels={'x': 'Date', 'y': 'Price €'})
            fig.update_layout(
                autosize=False,
                width=1000,
                height=410)

            plotCode = plotly.offline.plot(fig, include_plotlyjs='cdn', output_type='div')
            plotCode=plotCode.replace('"', "'")
            return plotCode, 200
        else:
            return error, 400
api.add_resource(Plot, '/plot/<stock_name>')
#%%############################################################################
class Portfolio(Resource, PortfolioSimulationBridge):
    def __init__(self):
        super().__init__()

    def get(self):
        self.parserInit() #parse arguments
        self.parserExecute() #get companies
        self.makeConn() #make db connection
        self.executeConn() #execute db query
        self.dataframeManipulation() #manipulate dataframe

        dfCode = self.df.to_json()
        dfCode=dfCode.replace('"', "'")

        return dfCode, 202
api.add_resource(Portfolio, '/portfolio')
#%%############################################################################
class StockNames(Resource, DBconn):
    def __init__(self):
        super().__init__()
        pass

    def get(self):
        self.makeConn()
        with self.conn.cursor() as cursor:
            cursor.execute('select name from stocks group by name')
            result = cursor.fetchall()

        if len(result) > 0:
            return result, 200
        else:
            return 'Error', 400

api.add_resource(StockNames, '/names')
#%%############################################################################
class Simulation(Resource, PortfolioSimulationBridge):
    def __init__(self):
        super().__init__()
        self._begin_date=None
        self._end_date=None

    def simulation(self, begin_date='2013-04-05', end_date='2018-04-27'):
        if self._begin_date != 'null' and self._begin_date is not None:
            self._begin_date=self._begin_date.replace('/','-') #make date right formate
            try:
                datetime.strptime(self._begin_date, '%Y-%m-%d')
            except:
                self._begin_date=begin_date
        else:
            self._begin_date=begin_date
        if self._end_date != 'null' and self._end_date is not None:
            self._end_date=self._end_date.replace('/','-') #make date right formate
            try:
                datetime.strptime(self._end_date, '%Y-%m-%d')
            except:
                self._end_date=end_date
        else:
            self._end_date=end_date

        if self._budget == 'null' and self._budget is not None: #check if budget is right type and not empty
            self._budget=2000
        try:
            self._budget=float(self._budget)
        except:
            self._budget=2000

        self.budget, self.plotCode = Sim.simulation(begin_date=self._begin_date, end_date=self._end_date, all_comps=self.df, budget=self._budget)


    def get(self):
        self.parserInit() #initialize parser
        self.parser.add_argument('begin')
        self.parser.add_argument('end')
        self.parser.add_argument('budget')

        self.parserExecute()
        self._begin_date=self.args['begin']
        self._end_date=self.args['end']
        self._budget=self.args['budget']

        self.makeConn() #make db connection
        self.executeConn() #execute db query
        self.dataframeManipulation() #manipulate dataframe
        self.simulation()

        return {'budget': self.budget, 'plot': self.plotCode}, 200
api.add_resource(Simulation, '/simulation')
##%%############################################################################
class Home(Resource):
    def get(self):
        home="{'Examples': [{'Adress': '.../plot/<stock_name>', 'Method': '[GET]' }, {'Adress': '.../portfolio/?companies=<stock_name1>,<stock_name2>,...', 'Method': '[GET]'}, {'Adress': '.../simulation/?id=<stock_name1>,<stock_name2>,...,&begin=2020-01-01&end=2020-12-31', 'Method': '[GET]'}, {'Adress': '.../names', 'Method': '[GET]'} ]} "
        return home, 202
api.add_resource(Home, '/')
#%%#############################################################################
if __name__ == "__main__":
    app.run()
