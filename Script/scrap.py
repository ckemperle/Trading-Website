import mysql.connector

import numpy as np
from datetime import datetime
import sys

paths =['...']
for path in paths:
    if path not in sys.path:
        sys.path.insert(0, path) #path to Stocks.py
from stocks import Stock

with open('...', 'r') as reader:
    dbVars= reader.read().split('\n')

class DBconn():
    def makeConn(self):
        conn = mysql.connector.connect(
            host=f'{dbVars[0]}',
            user=f'{dbVars[1]}',
            passwd=f'{dbVars[2]}',
            database=f'{dbVars[3]}')
        self.conn = conn

class ScrapPost(DBconn):
    def __init__(self):
        super().__init__()
        self.message=list()
        self.error=False
        self.date=None

    def dataBase(self,stockValues):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO stocks VALUES (NULL, {np.round(stockValues[0],3)}, '{stockValues[1]}', '{stockValues[2].strftime('%Y-%m-%d')}', '{stockValues[3]}')")
                self.conn.commit()
                yield
        except Exception:
            self.error=True
            yield

    #scrap all stocks (currently only nasdaq and sp500)
    def scrap(self, date=None, stock=None):
        self.date=date

        self.makeConn()
        if stock is None:
            stock=['nasdaq', 'sp']
        for name in stock:
            download = Stock(name)
            if self.date is None:
                download.download(day=True)
            elif self.date == 'week':
                download.download(week=True)
            else:
                try:
                    self.date = datetime.strptime(self.date, '%Y%m%d').strftime('%Y-%m-%d')
                except:
                    pass
                download.download(scrapDate=self.date)
            for row in download().values:
                for r in self.dataBase(row):
                    print(r) #logging


if __name__ == "__main__":
    try:
        date = str(sys.argv[1])
        stock = str(sys.argv[2])
    except:
        try:
            date=str(sys.argv[1])
            stock=None
        except:
            date=None
            stock=None
    scrap=ScrapPost()
    scrap.scrap(date=date, stock=stock)
    print("Done")
