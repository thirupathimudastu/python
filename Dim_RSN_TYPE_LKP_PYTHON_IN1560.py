# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 20:32:45 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='Dim_RSN_TYPE_LKP_PYTHON_IN1560.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from [BCMPWMT].[RSN_TYPE_LKP]

    '''
    
    
    df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    


    logger.info('Applying transformations')
    #['RSN_TYPE_LKP_KEY']=df['RSN_TYPE_LKP_KEY'].strip().astype('int')
    df['RSN_TYPE_ID']=df['RSN_TYPE_ID'].astype('int')
    df['RSN_TYPE_CD']=df['RSN_TYPE_CD'].replace('NULL','N/A').fillna('N/A')
    df['RSN_TYPE_DESC']=df['RSN_TYPE_DESC'].replace('NULL','N/A').fillna('N/A')
    
    df['CRE_TS']=pd.to_datetime(df['CRE_TS'],infer_datetime_format=True)
    df['CRE_USER']=df['CRE_USER'].replace('NULL','N/A').fillna('N/A')
    
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    #truncate_table='''TRUNCATE TABLE STG_DIM_CHARGE_CATEG_PYTHON_IN1560'''
    #conn.execute(truncate_table)
    insertstmt=''
    
    
    

    for index,row in cleaned_df.iterrows():
        
      insertstmt+=f'''insert into IN1560.STG_Dim_RSN_TYPE_LKP_PYTHON_IN1560
      values ({row['RSN_TYPE_ID']},'{row['RSN_TYPE_CD']}','{row['RSN_TYPE_DESC']}','{row['CRE_TS']}', '{row['CRE_USER']}')
      '''
      print(insertstmt)
 
    cursor.execute(insertstmt)
    conn.commit()
    
    
    sql_query='''INSERT INTO Dim_RSN_TYPE_LKP_PYTHON_IN1560
                    SELECT 
                    RSN_TYPE_ID,
                    RSN_TYPE_CD,
                    RSN_TYPE_DESC,
                    CRE_TS,
                    CRE_USER
                    FROM 
                    STG_Dim_RSN_TYPE_LKP_PYTHON_IN1560'''
                       
    cursor.execute(sql_query)
    conn.commit()
    
if __name__=='__main__':
    main()
        
    