# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 08:52:24 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CUST_PHONE_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_PHONE

    '''

    phone_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    phone_df[ 'PHONE_ID']=phone_df[ 'PHONE_ID'].replace('NULL',101).astype('int64')
    phone_df[ 'TENANT_ORG_ID']=phone_df[ 'TENANT_ORG_ID'].replace('NULL',101).astype('int')
    phone_df[ 'CNTCT_TYPE_ID']=phone_df[ 'CNTCT_TYPE_ID'].replace('NULL',101).astype('int64')
    phone_df[ 'SRC_PHONE_ID']=phone_df[ 'SRC_PHONE_ID'].str.strip().astype('str')
    phone_df[ 'DATA_SRC_ID']=phone_df[ 'DATA_SRC_ID'].replace('NULL',101).astype('int64')
    phone_df[ 'AREA_CD']=phone_df[ 'AREA_CD'].replace('NULL','N/A').str.strip().astype('str')
    phone_df[ 'CNTRY_CD']=phone_df[ 'CNTRY_CD'].replace('NULL','N/A').str.strip().astype('str')
    phone_df[ 'EXTN']=phone_df[ 'EXTN'].replace('NULL','N/A').str.strip().replace('NULL','N/A').str.strip().astype('str')
    phone_df[ 'CRE_DT']=pd.to_datetime(phone_df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True).dt.date
    phone_df[ 'DELTD_YN']=phone_df[ 'DELTD_YN'].str.strip().replace('NULL','N/A').str.strip().astype('str')
    phone_df[ 'UPD_TS']=pd.to_datetime(phone_df['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    

    
    
    cleaned_df=utils.nullhandler(phone_df)
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_CUST_PHONE_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
    
    
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into IN1560.STG_DIM_CUST_PHONE_PYTHON_IN1560
         values (?,?,?,?,?,?,?,?,?,?,?)'''
        collist=['PHONE_ID',
                    'TENANT_ORG_ID',
                    'CNTCT_TYPE_ID',
                    'SRC_PHONE_ID',
                    'DATA_SRC_ID',
                    'AREA_CD',
                    'CNTRY_CD',
                    'EXTN',
                    'CRE_DT',
                    'DELTD_YN',
                    'UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
      
    conn.commit()
    



    sql_query='''INSERT INTO DIM_CUST_PHONE_PYTHON_IN1560
        SELECT 
        PHONE_ID,
        TENANT_ORG_ID,
        CNTCT_TYPE_ID,
        SRC_PHONE_ID,
        DATA_SRC_ID,
        AREA_CD,
        CNTRY_CD,
        EXTN,
        CRE_DT,
        DELTD_YN,
        UPD_TS
        FROM
        STG_DIM_CUST_PHONE_PYTHON_IN1560'''

    cursor.execute(sql_query)
    conn.commit()

    
    
if __name__=='__main__':
    main()
    
    
    
    
    
    
    
    
    
    
    
    
    
    