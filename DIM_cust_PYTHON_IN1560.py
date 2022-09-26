# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 08:41:41 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re
import numpy as np

logger=utils.setlogger(logfile='DIM_cust_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST

    '''
    
    
    
    cust_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    
    cust_df['CUST_ID']= cust_df['CUST_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    cust_df['TENANT_ORG_ID']= cust_df['TENANT_ORG_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    cust_df['CUST_TYPE_ID']= cust_df['CUST_TYPE_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    cust_df[ 'NICKNAME']=cust_df[ 'NICKNAME'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'SALUTE']=cust_df[ 'SALUTE'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'MIDDLE_NM']=cust_df[ 'MIDDLE_NM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'CUST_TITLE']=cust_df[ 'CUST_TITLE'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'SUFFIX']=cust_df[ 'SUFFIX'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    cust_df['WM_EMPLOYEE_ID']= cust_df['WM_EMPLOYEE_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    cust_df[ 'CRE_DT']=pd.to_datetime(cust_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    
    cust_df[ 'CRE_USER']=cust_df[ 'CRE_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    cust_df[ 'UPD_TS']=pd.to_datetime(cust_df['UPD_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    
    cust_df[ 'UPD_USER']=cust_df[ 'UPD_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    #cust_df[ 'StartDate']=pd.to_datetime(cust_df['StartDate'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    #cust_df[ 'UPD_TS']=pd.to_datetime(cust_df['UPD_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    cust_df[ 'SIGNUP_TS']=pd.to_datetime(cust_df['SIGNUP_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    
    cust_df[ 'REALM_ID']=cust_df[ 'REALM_ID'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'VALID_CUST_IND']=cust_df[ 'VALID_CUST_IND'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    cust_df[ 'DELTD_YN']=cust_df[ 'DELTD_YN'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    


    cleaned_df=cust_df
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_cust_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
 
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into STG_DIM_cust_PYTHON_IN1560
                                values(?,?,?,?,?,?,?,?,?,?,?,?,?,getdate(),null,?,?,?,?)'''
        collist=['CUST_ID',
                                'TENANT_ORG_ID',
                                'CUST_TYPE_ID',
                                'NICKNAME',
                                'SALUTE',
                                'MIDDLE_NM',
                                'CUST_TITLE',
                                'SUFFIX',
                                'WM_EMPLOYEE_ID',
                                'CRE_DT',
                                'CRE_USER',
                                'UPD_TS',
                                'UPD_USER',
                                'SIGNUP_TS',
                                'REALM_ID',
                                'VALID_CUST_IND',
                                'DELTD_YN']
                                    
    
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
          
    conn.commit()   



    scd_query='''insert into DIM_cust_PYTHON_IN1560
                        select 
                        s.CUST_ID,
                        s.TENANT_ORG_ID,
                        s.CUST_TYPE_ID,
                        s.NICKNAME,
                        s.SALUTE,
                        s.MIDDLE_NM,
                        s.CUST_TITLE,
                        s.SUFFIX,
                        s.WM_EMPLOYEE_ID,
                        s.CRE_DT,
                        s.CRE_USER,
                        s.UPD_TS,
                        s.UPD_USER,
                        getdate(),
                        null,
                        s.SIGNUP_TS,
                        s.REALM_ID,
                        s.VALID_CUST_IND,
                        s.DELTD_YN
                        from 
                        STG_DIM_cust_PYTHON_IN1560 s left join DIM_cust_PYTHON_IN1560 t on s.CUST_ID=t.CUST_ID
                        where t.CUST_KEY is null or (t.EndDate is null and (
                        s.SALUTE  <> t.SALUTE or 
                        s.CUST_TITLE <> t.CUST_TITLE or 
                        s.SUFFIX <> t.SUFFIX))
                        
                        update DIM_cust_PYTHON_IN1560
                        set EndDate=getdate()
                        from STG_DIM_cust_PYTHON_IN1560 s join DIM_cust_PYTHON_IN1560 t
                        on s.CUST_ID=t.CUST_ID
                        where t.enddate is null and(
                        s.SALUTE  <> t.SALUTE or 
                        s.CUST_TITLE <> t.CUST_TITLE or 
                        s.SUFFIX <> t.SUFFIX)'''
    
    cursor.execute(scd_query)
    conn.commit()







    
    