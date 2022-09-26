# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 14:19:07 2022

@author: DSethura
"""
import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from [BCMPWMT].CHARGE_CATEG_LKP

    '''
    
    
    src_charge_categdf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    src_charge_categdf['CHARGE_CATEG_ID']=src_charge_categdf['CHARGE_CATEG_ID'].str.strip().astype('int')
    src_charge_categdf['TENANT_ORG_ID']=src_charge_categdf['TENANT_ORG_ID'].str.strip().astype('int')
    src_charge_categdf['CHARGE_CATEG']=src_charge_categdf['CHARGE_CATEG'].str.lower()
    src_charge_categdf['CHARGE_CATEG']=src_charge_categdf['CHARGE_CATEG'].apply(lambda x: x.strip() if len(x)>5 else str.upper(x.strip()))
    
    src_charge_categdf['CHARGE_CATEG_DESC']=src_charge_categdf['CHARGE_CATEG_DESC'].str.strip()
    src_charge_categdf['TAX_IND']=src_charge_categdf['TAX_IND'].str.strip().astype('int')
    
    
    cleaned_df=utils.nullhandler(src_charge_categdf)
    logger.info('Null values handled')
    #truncate_table='''TRUNCATE TABLE STG_DIM_CHARGE_CATEG_PYTHON_IN1560'''
    #conn.execute(truncate_table)
    insertstmt=''
    
    for index,row in cleaned_df.iterrows():
        
      insertstmt+=f'''insert into IN1560.STG_DIM_CHARGE_CATEG_PYTHON_IN1560
      values ({row['CHARGE_CATEG_ID']},{row['TENANT_ORG_ID']},'{row['CHARGE_CATEG']}','{row['CHARGE_CATEG_DESC']}', {row['TAX_IND']},1)
      '''
      print(insertstmt)
 
    cursor.execute(insertstmt)
    conn.commit()
    
    scd1_query= '''insert into DIM_CHARGE_CATEG_PYTHON_IN1560
                    select 
                     s.CHARGE_CATEG_ID
                    , s.TENANT_ORG_ID
                    , s.CHARGE_CATEG
                    , s.CHARGE_CATEG_DESC
                    , s.TAX_IND ,
                    case
                    when t.CHARGE_CATEG_ID is null then 1
                    else 1+(select max(t.version) from DIM_CHARGE_CATEG_PYTHON_IN1560 t
                    join STG_DIM_CHARGE_CATEG_PYTHON_IN1560 s on 
                    s.CHARGE_CATEG_ID=t.CHARGE_CATEG_ID
                    where t.CHARGE_CATEG <> s.CHARGE_CATEG) end as version
                    from STG_DIM_CHARGE_CATEG_PYTHON_IN1560 s
                    left join DIM_CHARGE_CATEG_PYTHON_IN1560 t on 
                    s.CHARGE_CATEG_ID=t.CHARGE_CATEG_ID
                    left join 
                    (select CHARGE_CATEG_ID,max(version) as max_version from DIM_CHARGE_CATEG_PYTHON_IN1560 group by CHARGE_CATEG_ID) a
                    on t.CHARGE_CATEG_ID=a.CHARGE_CATEG_ID
                    where 
                    t.CHARGE_CATEG_ID is null or ((t.CHARGE_CATEG_ID is not null) and (t.CHARGE_CATEG <> s.CHARGE_CATEG) and 
                    t.version=a.max_version)'''
                    
    cursor.execute(scd1_query)
    conn.commit()
    

if __name__=='__main__':
    main()
    
    



