# -*- coding: utf-8 -*-
"""
Created on Sun Sep 25 17:19:59 2022

@author: TMudastu
"""
import pyodbc
import pandas as pd
import datetime as dt
import numpy as np
import time


conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=systechtraining.database.windows.net;'
                      'Database=Bootcamp;'
                      'UID=B32022_TMudastu;'
                      'PWD=n2g9JxqKT3pYr;'
                      'Trusted_Connection=no;')


titledf= pd.read_sql_query('SELECT * FROM BCMPPBS.titles', conn)
title_authordf=pd.read_sql('select *from BCMPPBS.titleauthor',conn)
authordf=pd.read_sql('select * from BCMPPBS.authors',conn)


m1=titledf.merge(title_authordf, how='left', on='title_id') 
m2=m1.merge(authordf,how='left',on='au_id')

tgt_df=pd.DataFrame()


tgt_df['title_id']=m2['title_id'].fillna('N/A').apply(lambda x:'T-'+x+'-001').astype('str')
tgt_df['Title_Desc']=m2['title'].fillna('N/A').apply(lambda x: x+m2['type']).astype('str')





