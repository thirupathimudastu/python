# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 14:19:07 2022

@author: DSethura
"""
import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CUST_EMAIL_PYTHON_IN1560.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_EMAIL

    '''
    
    
# Convert pandas multiple columns to Datetime
#df[[]] = df[['Inserted','Updated']].apply(pd.to_datetime, errors='coerce')
#print(df)

    src_cust_emaildf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
   
    src_cust_emaildf['EMAIL_ID']=src_cust_emaildf['EMAIL_ID'].astype('int64')
    src_cust_emaildf['TENANT_ORG_ID']=src_cust_emaildf['TENANT_ORG_ID'].astype('int64')
    src_cust_emaildf['CNTCT_TYPE_ID']=src_cust_emaildf['CNTCT_TYPE_ID'].replace('NULL',101).astype('int64')
    
    src_cust_emaildf['DATA_SRC_ID']=src_cust_emaildf['DATA_SRC_ID'].replace('NULL',101).astype('int64')
    src_cust_emaildf['DELTD_YN']=src_cust_emaildf['DELTD_YN'].replace('NULL','N/A').str.strip().astype(str)
    
    src_cust_emaildf['CRE_DT']=pd.to_datetime(src_cust_emaildf['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    src_cust_emaildf['UPD_TS']=pd.to_datetime(src_cust_emaildf['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    



    
    cleaned_df=utils.nullhandler(src_cust_emaildf)
    logger.info('Null values handled')
    truncate_table='''TRUNCATE TABLE STG_DIM_CUST_EMAIL_PYTHON_IN1560'''
    cursor.execute(truncate_table)
    conn.commit()
    insertstmt=''
    
    cursor.fast_executemany = True
    
    for index,row in cleaned_df.iterrows():
        insert_to_tmp_tbl_stmt='''insert into IN1560.STG_DIM_CUST_EMAIL_PYTHON_IN1560
         values (?,?,?,?,?,?,?)'''
        collist=[
                'EMAIL_ID',
                'TENANT_ORG_ID',
                'CNTCT_TYPE_ID',
                'DATA_SRC_ID',
                'DELTD_YN',
                'CRE_DT',
                'UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
      
    conn.commit()
      
 

    sql_query='''insert into DIM_CUST_EMAIL_PYTHON_IN1560
                    select 
                    EMAIL_ID,
                    TENANT_ORG_ID,
                    CNTCT_TYPE_ID,
                    DATA_SRC_ID,
                    DELTD_YN,
                    CRE_DT,
                    UPD_TS
                    from 
                    STG_DIM_CUST_EMAIL_PYTHON_IN1560'''
                     
                    
    cursor.execute(sql_query)
    conn.commit()
    
    


if __name__=='__main__':
    main()
    
    

    





