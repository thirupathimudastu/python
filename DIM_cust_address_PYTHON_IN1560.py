# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 19:53:16 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re
import numpy as np

logger=utils.setlogger(logfile='dim_cust_address_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_ADDR

    '''
    date='''select * from DIM_DAY_SQL_IN1560'''
    
    
    
    addr_df=pd.read_sql(src_query,conn)
    date_df=pd.read_sql(date,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    addr_df['ADDR_ID']= addr_df['ADDR_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float64')
    
    addr_df['TENANT_ORG_ID']= addr_df['TENANT_ORG_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr_df['DATA_SRC_ID']= addr_df['DATA_SRC_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr_df[ 'VALID_TS']=pd.to_datetime(addr_df['VALID_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    #addr_df['VALID_TS']=pd.to_datetime(addr_df['VALID_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date
    #addr_df['VALID_TS'] = pd.to_datetime(addr_df['VALID_TS'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('01-01-1900 00:00:00'), infer_datetime_format=True)
    
    addr_df[ 'VALID_STS']=addr_df[ 'VALID_STS'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    addr_df[ 'CITY']=addr_df[ 'CITY'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'MUNICIPALITY']=addr_df[ 'MUNICIPALITY'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'TOWN']=addr_df[ 'TOWN'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'VILLAGE']=addr_df[ 'VILLAGE'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'COUNTY']=addr_df[ 'COUNTY'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'DISTRICT']=addr_df[ 'DISTRICT'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    addr_df['ZIP_CD']= addr_df['ZIP_CD'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr_df['POSTAL_CD']= addr_df['POSTAL_CD'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr_df['ZIP_EXTN']= addr_df['ZIP_EXTN'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    addr_df[ 'ADDR_TYPE']=addr_df[ 'ADDR_TYPE'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'AREA']=addr_df[ 'AREA'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'CNTRY_CD']=addr_df[ 'CNTRY_CD'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'STATE_PRVNCE_TYPE']=addr_df[ 'STATE_PRVNCE_TYPE'].replace(['NULL','?'],'N/A').str.strip().astype('str')

    addr_df['OWNER_ID']= addr_df['OWNER_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr_df['PARENT_ID']= addr_df['PARENT_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    addr_df[ 'DELTD_YN']=addr_df[ 'DELTD_YN'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    addr_df[ 'CRE_DT']=pd.to_datetime(addr_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    #addr_df['CRE_DT']=pd.to_datetime(addr_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date
    
    addr_df[ 'CRE_USER']=addr_df[ 'CRE_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    
    addr_df[ 'UPD_TS']=pd.to_datetime(addr_df['UPD_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    addr_df[ 'UPD_USER']=addr_df[ 'UPD_USER'].replace(['NULL','?'],'N/A').str.strip().astype('str')





    cleaned_df=addr_df
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_dim_cust_address_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
 
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into STG_dim_cust_address_PYTHON_IN1560
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate(),null,?,?,?,?)'''

        collist=['ADDR_ID',
                'TENANT_ORG_ID',
                'DATA_SRC_ID',
                'VALID_TS',
                'VALID_STS',
                'CITY',
                'MUNICIPALITY',
                'TOWN',
                'VILLAGE',
                'COUNTY',
                'DISTRICT',
                'ZIP_CD',
                'POSTAL_CD',
                'ZIP_EXTN',
                'ADDR_TYPE',
                'AREA',
                'CNTRY_CD',
                'STATE_PRVNCE_TYPE',
                'OWNER_ID',
                'PARENT_ID',
                'DELTD_YN',
                'CRE_DT',
                'CRE_USER',
                'UPD_TS',
                'UPD_USER']
        
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
         
    conn.commit() 



    query='''insert into dim_cust_address_PYTHON_IN1560
            select 
            s.ADDR_ID,
            s.TENANT_ORG_ID,
            s.DATA_SRC_ID,
            s.VALID_TS,
            s.VALID_STS,
            s.CITY,
            s.MUNICIPALITY,
            s.TOWN,
            s.VILLAGE,
            s.COUNTY,
            s.DISTRICT,
            s.ZIP_CD,
            s.POSTAL_CD,
            s.ZIP_EXTN,
            s.ADDR_TYPE,
            s.AREA,
            s.CNTRY_CD,
            s.STATE_PRVNCE_TYPE,
            s.OWNER_ID,
            s.PARENT_ID,
            s.DELTD_YN,
            getdate(),
            null,
            s.CRE_DT,
            s.CRE_USER,
            s.UPD_TS,
            s.UPD_USER
            from 
            STG_dim_cust_address_PYTHON_IN1560 s left join dim_cust_address_PYTHON_IN1560 t on  s.addr_id=t.addr_id
            where  t.ADDR_ID is null or (t.enddate is null and(
            s.CITY<> t.CITY or
            s.MUNICIPALITY<> t.MUNICIPALITY or
            s.TOWN<> t.TOWN or 
            s.VILLAGE<> t.VILLAGE or
            s.COUNTY<> t.COUNTY or
            s.DISTRICT<> t.DISTRICT or
            s.ZIP_CD<> t.ZIP_CD or
            s.POSTAL_CD<> t.POSTAL_CD or
            s.ZIP_EXTN<> t.ZIP_EXTN or
            s.ADDR_TYPE<> t.ADDR_TYPE or
            s.AREA<> t.AREA or
            s.CNTRY_CD<> t.CNTRY_CD or
            s.STATE_PRVNCE_TYPE<> t.STATE_PRVNCE_TYPE or
            s.OWNER_ID<> t.OWNER_ID or
            s.PARENT_ID<> t.PARENT_ID ))
            
            update dim_cust_address_PYTHON_IN1560
            set enddate=getdate()
            from STG_dim_cust_address_PYTHON_IN1560 s join dim_cust_address_PYTHON_IN1560 t
            on s.addr_id=t.addr_id
            where t.enddate is null and(
            s.CITY<> t.CITY or
            s.MUNICIPALITY<> t.MUNICIPALITY or
            s.TOWN<> t.TOWN or 
            s.VILLAGE<> t.VILLAGE or
            s.COUNTY<> t.COUNTY or
            s.DISTRICT<> t.DISTRICT or
            s.ZIP_CD<> t.ZIP_CD or
            s.POSTAL_CD<> t.POSTAL_CD or
            s.ZIP_EXTN<> t.ZIP_EXTN or
            s.ADDR_TYPE<> t.ADDR_TYPE or
            s.AREA<> t.AREA or
            s.CNTRY_CD<> t.CNTRY_CD or
            s.STATE_PRVNCE_TYPE<> t.STATE_PRVNCE_TYPE or
            s.OWNER_ID<> t.OWNER_ID or
            s.PARENT_ID<> t.PARENT_ID )'''
            
    cursor.execute(query)
    conn.commit()































