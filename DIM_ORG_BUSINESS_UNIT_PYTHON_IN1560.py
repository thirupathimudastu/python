# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 07:38:11 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re
import numpy as np


logger=utils.setlogger(logfile='DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.ORG_BUSINESS_UNIT

    '''
    
    
    
    business_unit=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    
    business_unit[ 'ORG_ID']=business_unit[ 'ORG_ID'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'SRC_ORG_CD']=business_unit[ 'SRC_ORG_CD'].apply(pd.to_numeric, errors='coerce').replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit['ORG_TYPE_ID']= business_unit['ORG_TYPE_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    business_unit[ 'ORG_NM']=business_unit[ 'ORG_NM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'PARENT_ORG_ID']=business_unit[ 'PARENT_ORG_ID'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'PARENT_ORG_NM']=business_unit[ 'PARENT_ORG_NM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'WM_RDC_NUM']=business_unit[ 'WM_RDC_NUM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'WM_STORE_NUM']=business_unit[ 'WM_STORE_NUM'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'WM_DSTRBTR_NO']=business_unit[ 'WM_DSTRBTR_NO'].replace(['NULL','?'],'N/A').str.strip().astype('str')

    business_unit['WH_IND']= business_unit['WH_IND'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    business_unit['DSV_IND']= business_unit['DSV_IND'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    business_unit['ACTV_IND']= business_unit['ACTV_IND'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    business_unit[ 'EFF_BEGIN_DT']=pd.to_datetime(business_unit['EFF_BEGIN_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    business_unit[ 'EFF_END_DT']=pd.to_datetime(business_unit['EFF_END_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    business_unit[ 'CRE_DT']=pd.to_datetime(business_unit['CRE_DT'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    #business_unit[ 'Is_Valid_Flag']=business_unit[ 'Is_Valid_Flag'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    business_unit[ 'UPD_TS']=pd.to_datetime(business_unit['UPD_TS'].replace(['NULL','?'],'01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    


    cleaned_df=business_unit
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
 
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into STG_DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560
                                  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Y',?)'''
        collist=['ORG_ID',
                            'SRC_ORG_CD',
                            'ORG_TYPE_ID',
                            'ORG_NM',
                            'PARENT_ORG_ID',
                            'PARENT_ORG_NM',
                            'WM_RDC_NUM',
                            'WM_STORE_NUM',
                            'WM_DSTRBTR_NO',
                            'WH_IND',
                            'DSV_IND',
                            'ACTV_IND',
                            'EFF_BEGIN_DT',
                            'EFF_END_DT',
                            'CRE_DT',
                            'UPD_TS']


    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
         
    conn.commit() 
    
    
    scd_query='''INSERT INTO DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560
                        SELECT 
                        S.ORG_ID,
                        S.SRC_ORG_CD,
                        S.ORG_TYPE_ID,
                        S.ORG_NM,
                        S.PARENT_ORG_ID,
                        S.PARENT_ORG_NM,
                        S.WM_RDC_NUM,
                        S.WM_STORE_NUM,
                        S.WM_DSTRBTR_NO,
                        S.WH_IND,
                        S.DSV_IND,
                        S.ACTV_IND,
                        S.EFF_BEGIN_DT,
                        S.EFF_END_DT,
                        S.CRE_DT,
                        'Y' AS Is_Valid_Flag,
                        S.UPD_TS
                        FROM 
                        STG_DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560 S
                        LEFT JOIN 
                        DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560 T ON S.ORG_ID=T.ORG_ID
                        WHERE T.ORG_ID IS NULL OR(
                        S.ORG_NM <> t.ORG_NM or
                        S.PARENT_ORG_ID <> t.PARENT_ORG_ID or
                        S.PARENT_ORG_NM <> t.PARENT_ORG_NM or
                        S.WM_RDC_NUM <> t.WM_RDC_NUM or
                        S.WM_STORE_NUM <> t.WM_STORE_NUM or
                        S.WM_DSTRBTR_NO <> t.WM_DSTRBTR_NO and t.Is_Valid_Flag='Y')
                        
                        UPDATE DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560
                        SET 
                        Is_Valid_Flag='N'
                        FROM 
                        STG_DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560 S
                        JOIN 
                        DIM_ORG_BUSINESS_UNIT_PYTHON_IN1560 T ON S.ORG_ID=T.ORG_ID
                        WHERE 
                        T.ORG_ID IS NOT NULL AND T.Is_Valid_Flag='Y' AND
                        (S.ORG_NM <> t.ORG_NM or
                        S.PARENT_ORG_ID <> t.PARENT_ORG_ID or
                        S.PARENT_ORG_NM <> t.PARENT_ORG_NM or
                        S.WM_RDC_NUM <> t.WM_RDC_NUM or
                        S.WM_STORE_NUM <> t.WM_STORE_NUM or
                        S.WM_DSTRBTR_NO <> t.WM_DSTRBTR_NO)
                        '''
    
    cursor.execute(scd_query)
    conn.commit()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    