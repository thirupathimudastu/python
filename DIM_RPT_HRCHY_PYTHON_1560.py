# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 07:20:44 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re

logger=utils.setlogger(logfile='DIM_RPT_HRCHY_PYTHON_1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.RPT_HRCHY

    '''
    
    
    
    rpt_hrchy_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    rpt_hrchy_df[ 'RPT_HRCHY_ID']=rpt_hrchy_df[ 'RPT_HRCHY_ID'].replace('NULL',101).astype('float64')
    rpt_hrchy_df[ 'SRC_RPT_HRCHY_ID']=rpt_hrchy_df[ 'SRC_RPT_HRCHY_ID'].replace('NULL',101.0).astype('float64')
    rpt_hrchy_df[ 'TENANT_ORG_ID']=rpt_hrchy_df[ 'TENANT_ORG_ID'].replace('NULL','N/A').astype('str').str.strip()
    rpt_hrchy_df[ 'RPT_HRCHY_PATH']=rpt_hrchy_df[ 'RPT_HRCHY_PATH'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'DIV_ID']=rpt_hrchy_df[ 'DIV_ID'].replace('NULL',101.0).astype('float64')
    rpt_hrchy_df[ 'DIV_NM']=rpt_hrchy_df[ 'DIV_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'SUPER_DEPT_ID']=rpt_hrchy_df[ 'SUPER_DEPT_ID'].replace('NULL',101.0).astype('float64')
    rpt_hrchy_df[ 'SUPER_DEPT_NM']=rpt_hrchy_df[ 'SUPER_DEPT_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'DEPT_ID']=rpt_hrchy_df[ 'DEPT_ID'].replace('NULL',101).astype('float64')
    rpt_hrchy_df[ 'DEPT_NM']=rpt_hrchy_df[ 'DEPT_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'CATEG_NM']=rpt_hrchy_df[ 'CATEG_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'SUB_CATEG_ID']=rpt_hrchy_df[ 'SUB_CATEG_ID'].replace('NULL',101).astype('float64')
    rpt_hrchy_df[ 'SUB_CATEG_NM']=rpt_hrchy_df[ 'SUB_CATEG_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'ITEM_CATEG_GROUPING_ID']=rpt_hrchy_df[ 'ITEM_CATEG_GROUPING_ID'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'SRC_CRE_TS']=rpt_hrchy_df[ 'SRC_CRE_TS'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'SRC_MODFD_TS']=rpt_hrchy_df[ 'SRC_MODFD_TS'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'SRC_HRCHY_MODFD_TS']=pd.to_datetime(rpt_hrchy_df['SRC_HRCHY_MODFD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    rpt_hrchy_df[ 'CATEG_MGR_NM']=rpt_hrchy_df[ 'CATEG_MGR_NM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'BUYER_NM']=rpt_hrchy_df[ 'BUYER_NM'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'EFF_BEGIN_DT']=pd.to_datetime(rpt_hrchy_df['EFF_BEGIN_DT'].replace('NULL','01/01/1900').fillna('01/01/1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date
    rpt_hrchy_df[ 'EFF_END_DT']=pd.to_datetime(rpt_hrchy_df['EFF_END_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True).dt.date
    rpt_hrchy_df[ 'RPT_HRCHY_ID_PATH']=rpt_hrchy_df[ 'RPT_HRCHY_ID_PATH'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'CATEG_ID']=rpt_hrchy_df[ 'CATEG_ID'].replace('NULL',101).astype('float64')
    rpt_hrchy_df[ 'CONSUMABLE_IND']=rpt_hrchy_df[ 'CONSUMABLE_IND'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'CURR_IND']=rpt_hrchy_df[ 'CURR_IND'].replace('NULL',101).astype('float64')
    rpt_hrchy_df[ 'CRE_DT']=pd.to_datetime(rpt_hrchy_df['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900').apply(lambda x: x if re.match(r'(\d+/\d+/\d+)',x) else '01/01/1900')).dt.date  
    rpt_hrchy_df[ 'CRE_USER']=rpt_hrchy_df[ 'CRE_USER'].replace('NULL','N/A').str.strip().astype('str')
    rpt_hrchy_df[ 'UPD_TS']=pd.to_datetime(rpt_hrchy_df['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    rpt_hrchy_df[ 'UPD_USER']=rpt_hrchy_df[ 'UPD_USER'].replace('NULL','N/A').str.strip().astype('str')



    
    cleaned_df=rpt_hrchy_df
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_RPT_HRCHY_PYTHON_1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
    
    
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into IN1560.STG_DIM_RPT_HRCHY_PYTHON_1560
             values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
        collist=['RPT_HRCHY_ID',
                        'SRC_RPT_HRCHY_ID',
                        'TENANT_ORG_ID',
                        'RPT_HRCHY_PATH',
                        'DIV_ID',
                        'DIV_NM',
                        'SUPER_DEPT_ID',
                        'SUPER_DEPT_NM',
                        'DEPT_ID',
                        'DEPT_NM',
                        'CATEG_NM',
                        'SUB_CATEG_ID',
                        'SUB_CATEG_NM',
                        'ITEM_CATEG_GROUPING_ID',
                        'SRC_CRE_TS',
                        'SRC_MODFD_TS',
                        'SRC_HRCHY_MODFD_TS',
                        'CATEG_MGR_NM',
                        'BUYER_NM',
                        'EFF_BEGIN_DT',
                        'EFF_END_DT',
                        'RPT_HRCHY_ID_PATH',
                        'CATEG_ID',
                        'CONSUMABLE_IND',
                        'CURR_IND',
                        'CRE_DT',
                        'CRE_USER',
                        'UPD_TS',
                        'UPD_USER']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
      
    conn.commit()
    
    
    
    
    
    sql_query='''insert into DIM_RPT_HRCHY_PYTHON_1560
                    select 
                    RPT_HRCHY_ID,
                    SRC_RPT_HRCHY_ID,
                    TENANT_ORG_ID,
                    RPT_HRCHY_PATH,
                    DIV_ID,
                    DIV_NM,
                    SUPER_DEPT_ID,
                    SUPER_DEPT_NM,
                    DEPT_ID,
                    DEPT_NM,
                    CATEG_NM,
                    SUB_CATEG_ID,
                    SUB_CATEG_NM,
                    ITEM_CATEG_GROUPING_ID,
                    SRC_CRE_TS,
                    SRC_MODFD_TS,
                    SRC_HRCHY_MODFD_TS,
                    CATEG_MGR_NM,
                    BUYER_NM,
                    EFF_BEGIN_DT,
                    EFF_END_DT,
                    RPT_HRCHY_ID_PATH,
                    CATEG_ID,
                    CONSUMABLE_IND,
                    CURR_IND,
                    CRE_DT,
                    CRE_USER,
                    UPD_TS,
                    UPD_USER
                    from 
                    STG_DIM_RPT_HRCHY_PYTHON_1560'''
    cursor.execute(sql_query)
    conn.commit()
    
if __name__=='__main__':
    main()
    
    
    
    
    
    
    
    
    
    
    
    