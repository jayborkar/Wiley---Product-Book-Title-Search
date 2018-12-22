# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 14:06:45 2018

@author: jborkar
"""
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
import string
import pymysql
import sys

exclude = set(string.punctuation)

def handle_strings(x):
    """
    Helper function to make string all caps and remove punctuation.
    
    x: any string
    """
    x = str(x).upper()
    x = ''.join(ch for ch in x if ch not in exclude)
    return x

def fuzMatch(query):

    #choices = result['FullTitle'].tolist()
    choices = list(result[['FullTitle', 'System','ISBN13']].itertuples(index=False, name=None)) 
    choices=list(dict.fromkeys(choices))
    
    prc=process.extractBests(query,choices,score_cutoff=60, limit=10)
    prc=list(dict.fromkeys(prc))
    lim=len(prc)
    if lim>0:
        labels = ['FullTitles', 'Score']
        df = pd.DataFrame.from_records(prc, columns=labels)
        df1=pd.DataFrame(df['FullTitles'].values.tolist(), index=df.index)
        df = pd.concat([df1, df['Score']], axis=1, sort=False)
        df.rename(columns={0: 'FullTitle',1:'System',2:'ISBN13'}, inplace=True)
    else:
        df = pd.DataFrame(columns=['FullTitle','System','Score','ISBN13'])
    #for i in range(0,lim):
       # prcc=result[result['FullTitle'].str.match((prc[i][0]))]
       # df = df.append(prcc,sort=True)
    '''
    lent=len(df)
    for j in range(0,lent):
        kk= df['FullTitle'].values[j]
        score= fuzz.WRatio(query,kk)
        df['Score'].values[j]=score
    
    df=df.drop_duplicates(['FullTitle','System'],keep= 'last')    
    '''
    #df.reset_index(drop = True, inplace = True)
    return df

def DupliFuzzyMatch(query):
    
    #choices = list(result[['FullTitle'].itertuples(index=False, name=None)) 
    #choices=list(dict.fromkeys(choices))
    choices = result['FullTitle'].tolist()
    prc=process.extractBests(query,choices,score_cutoff=60, limit=10)
    prc=list(dict.fromkeys(prc))
    '''
    labels = ['FullTitles', 'Score']
    df = pd.DataFrame.from_records(prc, columns=labels)
    df1=pd.DataFrame(df['FullTitles'].values.tolist(), index=df.index)
    df = pd.concat([df1, df['Score']], axis=1, sort=False)
    df.rename(columns={0: 'FullTitle',1:'System'}, inplace=True)
    '''
    lim=len(prc)
    if lim>0: 
        df = pd.DataFrame([])
        for i in range(0,lim):
            prcc=Dup_Count[Dup_Count['FullTitle'].str.match((prc[i][0]))]
            df = df.append(prcc)
        df=df.drop_duplicates(['Titles'],keep= 'last')  
        df=df[['FullTitle','System', 'Count']]  
        df=df.sort_values(['Count'],ascending=False)
        df.reset_index(drop = True, inplace = True)
        
    else:
        df = pd.DataFrame(columns=['FullTitle','System','ISBN13'])  

    return df

user = sys.argv[2]#'root'
passw = sys.argv[3]#'' 
host =  sys.argv[4]#'127.0.0.1'
port = 3306
database ='producttitle'

db_connection = pymysql.connect(host=host,
                       port=port,
                       user=user, 
                       passwd=passw,  
                       db=database)

result = pd.read_sql('SELECT * FROM result', con=db_connection)
db_connection.close()

Result_dupl=result[result.duplicated(['FullTitle','System'], keep=False)]
Result_dupl=Result_dupl.sort_values(['FullTitle','System'])
Result_dupl.reset_index(drop = True, inplace = True)

Dup_Count=Result_dupl.groupby(["FullTitle", "System"]).size()
Dup_Count=Dup_Count.sort_values(0, ascending=False)
Dup_Count= Dup_Count.to_frame()
Dup_Count.columns = ['Count']
Dup_Count['Titles'] = Dup_Count.index
Dup_Count.reset_index(drop = True, inplace = True)
Dup_Count[['FullTitle', 'System']] = Dup_Count['Titles'].apply(pd.Series)


#sentence = input("Enter the Book Title : ")
sentence=sys.argv[1]
sentence = re.sub(r"\s+", "", sentence, flags=re.UNICODE)
query=handle_strings(sentence)
matches=fuzMatch(query)
dupli=DupliFuzzyMatch(query)

from sqlalchemy import create_engine
'''
user = sys.argv[2] #'root'
passw = sys.argv[3] #''
host =  sys.argv[4] #'127.0.0.1'  # either localhost or ip e.g. '172.17.0.2' or hostname address 
port = 3306 
database = 'producttitle'
'''
engine = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + 
                     host + ':' + str(port) + '/' + database , echo=False)

#engine = create_engine("mysql+pymysql://root:@127.0.0.1/titles")
con=engine.connect()
matches.to_sql(name='matches',con=con,if_exists='replace') 
dupli.to_sql(name='duplicates',con=con,if_exists='replace')
con.close()

