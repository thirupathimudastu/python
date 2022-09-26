# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 11:07:57 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime 
import logging
import datetime


logger=utils.setlogger(logfile='Dim_FULFMT_TYPE_LKP_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.FULFMT_TYPE_LKP

    '''
    
    
    fulfmt_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    
    fulfmt_df['FULFMT_TYPE_ID']=fulfmt_df['FULFMT_TYPE_ID'].astype('int64')
    fulfmt_df['FULFMT_TYPE_CD']=fulfmt_df['FULFMT_TYPE_CD'].str.strip().astype(str)
    fulfmt_df['FULFMT_TYPE_DESC']=fulfmt_df['FULFMT_TYPE_DESC'].str.strip().astype(str)
    fulfmt_df[ 'CRE_DT']=pd.to_datetime(fulfmt_df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'))
    
    fulfmt_df['UPD_TS']=pd.to_datetime(fulfmt_df['UPD_TS'].replace('NULL','01/01/1900').fillna('01/01/1900'))
    fulfmt_df['UPD_TS']=fulfmt_df['UPD_TS'].dt.strftime('%d%b%Y')
    #y=pd.to_datetime(fulfmt_df['UPD_TS']).dt.year
    #dd=pd.to_datetime(fulfmt_df['UPD_TS']).dt.date
    #fulfmt_df['UPD_TS']=dd
    cleaned_df=utils.nullhandler(fulfmt_df)
    
    
    
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_CHARGE_CATEG_PYTHON_IN1560'''
    conn.execute(truncate_table)
    insertstmt=''
    
    for index,row in cleaned_df.iterrows():
        
        insertstmt+=f'''insert into IN1560.STG_FULFMT_TYPE_LKP_PYTHON_IN1560
        values ({row['FULFMT_TYPE_ID']},'{row['FULFMT_TYPE_CD']}','{row['FULFMT_TYPE_DESC']}','{row['CRE_DT']}','{row['UPD_TS']}')
        '''
        print(insertstmt)
   
    cursor.execute(insertstmt)
    conn.commit()
        
        
    ss='''INSERT INTO Dim_FULFMT_TYPE_LKP_PYTHON_IN1560
            SELECT 
            FULFMT_TYPE_ID,
            FULFMT_TYPE_CD,
            FULFMT_TYPE_DESC,
            CRE_DT,
            UPD_TS
            FROM 
            STG_FULFMT_TYPE_LKP_PYTHON_IN1560'''    


    cursor.execute(ss)
    conn.commit()


if __name__=='__main__':
    main()
    
        

        
        
        
        