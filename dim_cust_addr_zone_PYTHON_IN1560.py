# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 15:53:50 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re

logger=utils.setlogger(logfile='dim_cust_addr_zone_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_ADDR_ZONE

    '''
    
    
    
    cust_addr_zone_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    logger.info('Applying transformations')
    
    
    cust_addr_zone_df[ 'ADDR_ZONE_ID']=cust_addr_zone_df[ 'ADDR_ZONE_ID'].replace('NULL',101).astype('int64')
    cust_addr_zone_df[ 'TENANT_ORG_ID']=cust_addr_zone_df[ 'TENANT_ORG_ID'].replace('NULL',101).astype('int64')
    cust_addr_zone_df[ 'DATA_SRC_ID']=cust_addr_zone_df[ 'DATA_SRC_ID'].replace('NULL',101).astype('int64')
    
    
    cust_addr_zone_df[ 'CITY']=cust_addr_zone_df[ 'CITY'].replace('NULL','N/A').str.strip().astype('str')
    cust_addr_zone_df[ 'POSTAL_CD']=cust_addr_zone_df[ 'POSTAL_CD'].replace('NULL','N/A').str.strip().astype('str')
    cust_addr_zone_df[ 'STATE']=cust_addr_zone_df[ 'STATE'].replace('NULL','N/A').str.strip().astype('str')
    cust_addr_zone_df[ 'DELTD_YN']=cust_addr_zone_df[ 'DELTD_YN'].replace('NULL','N/A').str.strip().astype('str')
    cust_addr_zone_df[ 'CRE_USER']=cust_addr_zone_df[ 'CITY'].replace('NULL','N/A').str.strip().astype('str')
    
    cust_addr_zone_df['CRE_DT']=pd.to_datetime(cust_addr_zone_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date
    cust_addr_zone_df[ 'UPD_USER']=cust_addr_zone_df[ 'UPD_USER'].replace('NULL','N/A').str.strip().astype('str')
    cust_addr_zone_df[ 'UPD_TS']=pd.to_datetime(cust_addr_zone_df['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    





    cleaned_df=cust_addr_zone_df
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_dim_cust_addr_zone_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
    
    
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into IN1560.STG_dim_cust_addr_zone_PYTHON_IN1560
             values (?,?,?,?,?,?,?,?,?,?,?)'''
        collist=['ADDR_ZONE_ID',
                    'TENANT_ORG_ID',
                    'DATA_SRC_ID',
                    'CITY',
                    'POSTAL_CD',
                    'STATE',
                    'DELTD_YN',
                    'CRE_USER',
                    'CRE_DT',
                    'UPD_USER',
                    'UPD_TS']
        
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
      
    conn.commit()




    sql_query='''insert into DIM_cust_addr_zone_PYTHON_IN1560
                    SELECT 
                    ADDR_ZONE_ID,
                    TENANT_ORG_ID,
                    DATA_SRC_ID,
                    CITY,
                    POSTAL_CD,
                    STATE,
                    DELTD_YN,
                    CRE_USER,
                    CRE_DT,
                    UPD_USER,
                    UPD_TS
                    FROM 
                    STG_dim_cust_addr_zone_PYTHON_IN1560'''
                    
    cursor.execute(sql_query)
    conn.commit()
    
    close=utils.close_conn(conn,cursor)


























