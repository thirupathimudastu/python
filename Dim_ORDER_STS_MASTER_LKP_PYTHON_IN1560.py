# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 20:37:04 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='Dim_ORDER_STS_MASTER_LKP_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.ORDER_STS_MASTER_LKP

    '''
    
    
    
    o_s_m_lkp=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    
    o_s_m_lkp['ORDER_STS_MASTER_ID']=o_s_m_lkp['ORDER_STS_MASTER_ID'].replace('NULL',101).astype('int64')
    o_s_m_lkp[ 'ORDER_STS_MASTER_CD']=o_s_m_lkp[ 'ORDER_STS_MASTER_CD'].replace('NULL','N/A').str.strip().astype('str')
    o_s_m_lkp[ 'ORDER_STS_SHORT_DESC']=o_s_m_lkp[ 'ORDER_STS_SHORT_DESC'].replace('NULL','N/A').str.strip().astype('str')
    o_s_m_lkp[ 'ORDER_STS_LONG_DESC']=o_s_m_lkp[ 'ORDER_STS_LONG_DESC'].replace('NULL','N/A').str.strip().astype('str')
    
    o_s_m_lkp[ 'CRE_TS']=pd.to_datetime(o_s_m_lkp['CRE_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    o_s_m_lkp[ 'UPD_TS']=pd.to_datetime(o_s_m_lkp['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)


    
    cleaned_df=utils.nullhandler(o_s_m_lkp)
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_Dim_ORDER_STS_MASTER_LKP_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    
    
    
    insertstmt=''
    cursor.fast_executemany = True
    insert_to_tmp_tbl_stmt='''insert into IN1560.STG_Dim_ORDER_STS_MASTER_LKP_PYTHON_IN1560
     values (?,?,?,?,?,?)'''
    collist=['ORDER_STS_MASTER_ID',
            'ORDER_STS_MASTER_CD',
            'ORDER_STS_SHORT_DESC',
            'ORDER_STS_LONG_DESC',
            'CRE_TS',
            'UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
  
    conn.commit()
    
    sql_query='''insert into Dim_ORDER_STS_MASTER_LKP_PYTHON_IN1560
                    select 
                    
                    ORDER_STS_MASTER_ID,
                    ORDER_STS_MASTER_CD,
                    ORDER_STS_SHORT_DESC,
                    ORDER_STS_LONG_DESC,
                    CRE_TS,
                    UPD_TS
                    from 
                    STG_Dim_ORDER_STS_MASTER_LKP_PYTHON_IN1560'''
    cursor.execute(sql_query)
    conn.commit()
if __name__=='__main__':
    main()
    


