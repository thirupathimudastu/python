# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 10:41:08 2022

@author: TMudastu
"""

import utils
import pandas as pd
from datetime import datetime
import logging
import re
import numpy as np

logger=utils.setlogger(logfile='DIM_CUST_ADDR1_PYTHON_IN1560.log')


def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from BCMPWMT.CUST_ADDR1

    '''
    
    
    
    addr1_df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    logger.info('Applying transformations')
    
    addr1_df['ADDR_ID']= addr1_df['ADDR_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr1_df['TENANT_ORG_ID']= addr1_df['TENANT_ORG_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    addr1_df['DATA_SRC_ID']= addr1_df['DATA_SRC_ID'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
    
    addr1_df[ 'VALID_TS']=addr1_df[ 'VALID_TS'].replace(['NULL','?'],'N/A').str.strip().astype('str')
    

VALID_STS
CITY
MUNICIPALITY
TOWN
VILLAGE
COUNTY
DISTRICT
ZIP_CD
POSTAL_CD
ZIP_EXTN
ADDR_TYPE
AREA
CNTRY_CD
STATE_PRVNCE_TYPE
OWNER_ID
PARENT_ID
DELTD_YN
CRE_DT
CRE_USER
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    