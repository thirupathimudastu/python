# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 14:29:56 2022

@author: TMudastu
"""
import utils
import pandas as pd
from datetime import datetime 
import logging
import datetime


logger=utils.setlogger(logfile='Dim_RSN_LKP_PYTHON_IN1560.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.RSN_LKP
    '''
    
    
    rsn_lkp_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    rsn_lkp_df['RSN_ID']=rsn_lkp_df['RSN_ID'].astype('int64')
    rsn_lkp_df['TENANT_ORG_ID']=rsn_lkp_df['TENANT_ORG_ID'].astype('int64')
    rsn_lkp_df['DATA_SRC_ID']=rsn_lkp_df['DATA_SRC_ID'].astype('int64')
    rsn_lkp_df['RSN_TYPE_ID']=rsn_lkp_df['RSN_TYPE_ID'].astype('int64')
    rsn_lkp_df['RSN_CD']=rsn_lkp_df['RSN_CD'].apply(lambda x:101 if 'NULL' else 101 if '%[a-zA-Z]%' else x).astype('int64')
    rsn_lkp_df['SRC_RSN_ID']=rsn_lkp_df['SRC_RSN_ID'].astype('int64')
    
    rsn_lkp_df['RSN_DESC']=rsn_lkp_df['RSN_DESC'].str.strip().astype(str)
    rsn_lkp_df['RSN_LONG_DESC']=rsn_lkp_df['RSN_LONG_DESC'].str.strip().astype(str)
    rsn_lkp_df[ 'CRE_TS']=pd.to_datetime(rsn_lkp_df['CRE_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    rsn_lkp_df['CRE_USER']=rsn_lkp_df['CRE_USER'].str.strip().astype(str)
    rsn_lkp_df[ 'UPD_TS']=pd.to_datetime(rsn_lkp_df['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    rsn_lkp_df['UPD_USER']=rsn_lkp_df['UPD_USER'].str.strip().astype(str)
    
    cleaned_df=utils.nullhandler(rsn_lkp_df)
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_Dim_RSN_LKP_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into IN1560.STG_Dim_RSN_LKP_PYTHON_IN1560
    values (?,?,?,?,?,?,?,?,?,?,?,?)'''
    collist=[
                'RSN_ID',
                'TENANT_ORG_ID',
                'DATA_SRC_ID',
                'RSN_TYPE_ID',
                'RSN_CD',
                'SRC_RSN_ID',
                'RSN_DESC',
                'RSN_LONG_DESC',
                'CRE_TS',
                'CRE_USER',
                'UPD_TS',
                'UPD_USER']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
          
    conn.commit()
        
    
    
    sql_query='''INSERT INTO Dim_RSN_LKP_PYTHON_IN1560
        SELECT 
        RSN_ID,
        TENANT_ORG_ID,
        DATA_SRC_ID,
        RSN_TYPE_ID,
        RSN_CD,
        SRC_RSN_ID,
        RSN_DESC,
        RSN_LONG_DESC,
        CRE_TS,
        CRE_USER,
        UPD_TS,
        UPD_USER
        FROM STG_Dim_RSN_LKP_PYTHON_IN1560'''
    cursor.execute(sql_query)
    conn.commit()
    
    
if __name__=='__main__':
    main()
    
    
    
    