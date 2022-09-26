# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 16:48:07 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_STS_LKP_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select *  from BCMPWMT.STS_LKP

    '''
    
    
    
    sts_lkp_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    sts_lkp_df[ 'STS_ID']=sts_lkp_df[ 'STS_ID'].replace('NULL',101).astype('int64')
    sts_lkp_df[ 'STS_MASTER_ID']=sts_lkp_df[ 'STS_MASTER_ID'].replace('NULL',101).astype('int64')
    sts_lkp_df[ 'TENANT_ORG_ID']=sts_lkp_df[ 'TENANT_ORG_ID'].replace('NULL',101).astype('int64')
    sts_lkp_df[ 'DATA_SRC_ID']=sts_lkp_df[ 'DATA_SRC_ID'].replace('NULL',101).astype('int64')
    sts_lkp_df[ 'STS_CD']=sts_lkp_df[ 'STS_CD'].replace('NULL','N/A').str.strip().astype('str')
    sts_lkp_df['SRC_STS_ID']=sts_lkp_df[ 'SRC_STS_ID'].replace('NULL',101).astype('int64')
    
    sts_lkp_df[ 'STS_DESC']=sts_lkp_df[ 'STS_DESC'].replace('NULL','N/A').str.strip().astype('str')
    sts_lkp_df[ 'STS_LONG_DESC']=sts_lkp_df[ 'STS_LONG_DESC'].replace('NULL','N/A').str.strip().astype('str')
    sts_lkp_df[ 'CRE_TS']=sts_lkp_df[ 'CRE_TS'].replace('NULL','N/A').str.strip().astype('str')
    sts_lkp_df[ 'UPD_TS']=sts_lkp_df[ 'UPD_TS'].replace('NULL','N/A').str.strip().astype('str')



    cleaned_df=utils.nullhandler(sts_lkp_df)
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_STS_LKP_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''

    cursor.fast_executemany = True
    
    
    insert_to_tmp_tbl_stmt='''insert into IN1560.STG_DIM_STS_LKP_PYTHON_IN1560
     values (?,?,?,?,?,?,?,?,?,?)'''
    collist=['STS_ID',
            'STS_MASTER_ID',
            'TENANT_ORG_ID',
            'DATA_SRC_ID',
            'STS_CD',
            'SRC_STS_ID',
            'STS_DESC',
            'STS_LONG_DESC',
            'CRE_TS',
            'UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
  
    conn.commit()
    
    
    
    
    sql_query='''
    insert into DIM_STS_LKP_PYTHON_IN1560
        select
        STS_ID,
        STS_MASTER_ID,
        TENANT_ORG_ID,
        DATA_SRC_ID,
        STS_CD,
        SRC_STS_ID,
        STS_DESC,
        STS_LONG_DESC,
        CRE_TS,
        UPD_TS 
        from 
        STG_DIM_STS_LKP_PYTHON_IN1560
    '''
    cursor.execute(sql_query)
    conn.commit()

if __name__=='__main__':
    main()
    


