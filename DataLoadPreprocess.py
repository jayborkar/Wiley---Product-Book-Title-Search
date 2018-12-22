# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 10:51:25 2018

@author: jborkar
"""

import pandas as pd
import re
import string
import pymysql
import sys

user = sys.argv[2]#'root'
passw = sys.argv[3]#'' 
host =  sys.argv[4]#'127.0.0.1'
port = 3306
database = 'producttitle'

db_connection = pymysql.connect(host=host,
                       port=port,
                       user=user, 
                       passwd=passw,  
                       db=database)

#db_connection = pymysql.connect(host='127.0.0.1', database='titles', user='root', password='')

df1 = pd.read_sql('SELECT * FROM GBPM', con=db_connection)
df2 = pd.read_sql('SELECT * FROM CORE', con=db_connection)
df3 = pd.read_sql('SELECT * FROM Coltrane', con=db_connection)

db_connection.close()

exclude = set(string.punctuation)

def handle_strings(x):
    """
    Helper function to make string all caps and remove punctuation.
    
    x: any string
    """
    x = str(x).upper()
    x = ''.join(ch for ch in x if ch not in exclude)
    return x


df1 = df1[df1['Full Title'].notnull()]
df2 = df2[df2['Full Title'].notnull()]
df3 = df3[df3['Full Title'].notnull()]

df1['Full Title'] = df1['Full Title'].apply(handle_strings)
df1['Full Title'] = df1['Full Title'].map(lambda x: x.strip())
df1['Full Title'] = df1['Full Title'].replace('\s+', ' ', regex=True)
GBPM_title=df1[['ISBN13','Full Title']]
GBPM_title.insert(2, "System",  'GBPM')

df2['Full Title'] = df2['Full Title'].apply(handle_strings)
df2['Full Title'] = df2['Full Title'].map(lambda x: x.strip())
df2['Full Title'] = df2['Full Title'].replace('\s+', ' ', regex=True)
CORE_title=df2[['ISBN13','Full Title']]
CORE_title.insert(2, "System",  'CORE')

df3['Full Title'] = df3['Full Title'].apply(handle_strings)
df3['Full Title'] = df3['Full Title'].map(lambda x: x.strip())
df3['Full Title'] = df3['Full Title'].replace('\s+', ' ', regex=True)
Coltrane_title=df3[['ISBN13','Full Title']]
Coltrane_title.insert(2, "System",  'Coltrane')

frames = [GBPM_title, CORE_title, Coltrane_title]
result = pd.concat(frames)
result.rename(columns={'Full Title': 'FullTitle'}, inplace=True)

filter = result["FullTitle"] != ""
result = result[filter]

from sqlalchemy import create_engine
'''
user = sys.argv[2]#'root'
passw = sys.argv[3] #''
host = sys.argv[4] #'127.0.0.1'  # either localhost or ip e.g. '172.17.0.2' or hostname address 
port = 3306 
database = 'producttitle'
'''
engine = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + 
                     host + ':' + str(port) + '/' + database , echo=False)

#engine = create_engine("mysql+pymysql://root:@127.0.0.1/titles")
con=engine.connect()
result.to_sql(name='result',con=con,if_exists='replace') 
con.close()

