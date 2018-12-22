# -*- coding: utf-8 -*-
"""
Created on Thu Jul 19 15:20:51 2018

@author: jborkar
"""
import time
start_time = time.time()
import pymysql
import sys
#db = pymysql.connect(host='127.0.0.1',user='root', passwd="", db="titles") 
user = sys.argv[2] #'root'
passw = sys.argv[3]# '' 
host = sys.argv[4]#'127.0.0.1'
port = 3306
database = 'producttitle'

db = pymysql.connect(host=host,
                       port=port,
                       user=user, 
                       passwd=passw,  
                       db=database)
dbcur = db.cursor()
dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'result' 
        """)#and table_schema = 'producttitle'
sentence=sys.argv[1]

if dbcur.fetchone()[0] != 1:
    dbcur.close()
    import DataLoadPreprocess
    import FuzzyMatch

else:
    import FuzzyMatch
    
print("--- %s seconds ---" % (time.time() - start_time))