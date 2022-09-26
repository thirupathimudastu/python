# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 17:08:20 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re

logger=utils.setlogger(logfile='DIM_CUST_ACCT_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_ACCT

    '''
    
    
    
    cust_acct_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    
    cust_acct_df[ 'ACCT_ID']=cust_acct_df[ 'ACCT_ID'].replace('NULL',101).astype('int64')
    cust_acct_df[ 'CUST_ID']=cust_acct_df[ 'CUST_ID'].replace('NULL',101).astype('int64')
    cust_acct_df[ 'TENANT_ORG_ID']=cust_acct_df[ 'TENANT_ORG_ID'].replace('NULL',101).astype('int64')
    cust_acct_df[ 'ACCT_STS_ID']=cust_acct_df[ 'ACCT_STS_ID'].replace('NULL',101).astype('int64')
    cust_acct_df[ 'ACCT_TYPE_ID']=cust_acct_df[ 'ACCT_TYPE_ID'].replace('NULL',101).astype('int64')
    
    
    cust_acct_df[ 'EMAIL']=cust_acct_df[ 'EMAIL'].replace(['NULL','?'],'N/A').apply(lambda x: x if '@' else 'NA').astype('str')
    cust_acct_df[ 'VALID_CUST_IND']=cust_acct_df[ 'VALID_CUST_IND'].replace('NULL',101).astype('int64')
    
    cust_acct_df['CRE_DT']=pd.to_datetime(cust_acct_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date
    cust_acct_df[ 'CRE_USER']=cust_acct_df[ 'CRE_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    cust_acct_df[ 'UPD_TS']=pd.to_datetime(cust_acct_df['UPD_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True).dt.date
    
    cust_acct_df[ 'UPD_USER']=cust_acct_df[ 'UPD_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')

    cust_acct_df[ 'DELTD_YN']=cust_acct_df[ 'DELTD_YN'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    
    
    
   
    cleaned_df=cust_acct_df
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_CUST_ACCT_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
 
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into IN1560.STG_DIM_CUST_ACCT_PYTHON_IN1560
         values (?,?,?,?,?,?,?,?,?,?,?,getdate(),null,?)'''
        collist=[
                'ACCT_ID',
                'CUST_ID',
                'TENANT_ORG_ID',
               'ACCT_STS_ID',
                'ACCT_TYPE_ID',
                'EMAIL',
                'VALID_CUST_IND',
                'CRE_DT',
                'CRE_USER',
                'UPD_TS',
                'UPD_USER',
                'DELTD_YN']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
      
    conn.commit()
    
    query='''
    insert into DIM_CUST_ACCT_PYTHON_IN1560
            select 
            s.ACCT_ID,
            s.CUST_ID,
            s.TENANT_ORG_ID,
            s.ACCT_STS_ID,
            s.ACCT_TYPE_ID,
            s.EMAIL,
            s.VALID_CUST_IND,
            s.CRE_DT,
            s.CRE_USER,
            s.UPD_TS,
            s.UPD_USER,
            getdate(),
            null as end_date,
            s.DELTD_YN
            from 
            STG_DIM_CUST_ACCT_PYTHON_IN1560 s
            left join DIM_CUST_ACCT_PYTHON_IN1560 t
            on s.ACCT_ID=t.ACCT_ID
            where (t.end_date is null and t.EMAIL <> s.EMAIL) or t.ACCT_ID is null
            
            update DIM_CUST_ACCT_PYTHON_IN1560
            set EMAIL=s.EMAIL,
            end_date=getdate()
            from 
            STG_DIM_CUST_ACCT_PYTHON_IN1560 s join DIM_CUST_ACCT_PYTHON_IN1560 t on s.ACCT_ID=t.ACCT_ID
            where t.end_date is null and t.EMAIL <> s.EMAIL
    '''


    cursor.execute(query)
    conn.commit()


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    