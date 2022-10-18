# -*- coding: utf-8 -*-
"""
Created on Wed Jul 21 11:33:57 2021

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""Fossil model training

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1SYoHlQM-I5ZN3p0xMcB3EWxRnLkZA5eT
"""

import pandas as pd
from fbprophet import Prophet
import json
from fbprophet.serialize import model_to_json
#!pip install mysql-connector-python
import warnings
import asyncio
#!pip install asyncio
import nest_asyncio
#!pip3 install nest_asyncio
nest_asyncio.apply()
import multiprocessing
import logging
import os



arr = []
processes = []
subprocesses = []


def model_creation(m_a, m_x, m_df_source, m_save_path, m_model_prefix):
    #await asyncio.sleep(0.2)
    #print("Printing ", m_x, "in: ", m_a)
    
    x = m_x
    df_source = m_df_source
    Save_path = m_save_path
    model_prefix = m_model_prefix

    warnings.filterwarnings("ignore")
    Store_Code_Val = x
    
    df_input = df_source[(df_source['UniqueKey'] == Store_Code_Val)]
    
    #print(x)
    df = pd.DataFrame()
    df = df_input
    #df[Time_col]=pd.to_datetime(df[Time_col])
  
    #print(df)
    #from fbprophet import Prophet
    logger = logging.getLogger('fbprophet')
    logger.setLevel(logging.ERROR)
    logger.setLevel(logging.DEBUG)
    model=Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    model.fit(df)
    with open(Save_path + model_prefix + Store_Code_Val + '.json', 'w') as fout:
        json.dump(model_to_json(model), fout)  # Save model
        mpath=Save_path + model_prefix + Store_Code_Val + '.json'
        os.chmod(mpath, 0o777)
    global arr
    arr.append(Store_Code_Val)


def unique_val_call(a, l, df_source, Save_path, model_prefix):
    #print(" inside loop of ", str(l)))
    #print("i am in: ", a)
    #print(Save_path)
    #await asyncio.sleep(0.2)

    for x in l:
        #await asyncio.sleep(0.2)
        #print(x)
        model_creation(a, x, df_source, Save_path, model_prefix)
        #sp = multiprocessing.Process(target=(model_creation), args = (a, x, df_source, Save_path, model_prefix))
        #sp.start()
        #subprocesses.append(sp)
        #loop = asyncio.get_event_loop()
        #task = loop.create_task(model_creation(a, x, df_source, Save_path, model_prefix))
        #await task
        #await asyncio.sleep(0.2)


def task_creation(Time_col, Data_col, partition_col, Input_file_name,
                        df_Uniquevalues, Save_path, model_prefix, Debug_mode, parallel_processes, loop):
    uniqueValues = (df_Uniquevalues[partition_col].unique())
    #print(len(uniqueValues))
    
    unique_df = pd.DataFrame(uniqueValues)
    #print(unique_df)
    if Debug_mode == 'ON':
        with open(Save_path + '/unique.csv', 'w') as f:
            unique_df.to_csv(f)
            #os.chmod(Save_path + '/unique.csv', 0o777)
            
    else:
        pass
    
    
    df_source = pd.read_csv(Save_path + Input_file_name, low_memory=False)
    
    
    df_source = df_source[[Time_col, Data_col, partition_col]]
    df_source = df_source.rename(columns = {Time_col : 'ds', Data_col : 'y'}, inplace = False)
    df_source["ds"]=pd.to_datetime(df_source["ds"])
    
    
    a = int(len(uniqueValues)/parallel_processes)
    print(len(uniqueValues))
    #time.sleep(5)
    
    for i in range(0,parallel_processes):
        print(str((i*a)) + ' , ' + str((i+1)*a))
        l = uniqueValues[(i*a) : (i+1)*a]
        #task_calls = loop.create_task(unique_val_call(i,l, df_source, Save_path, model_prefix))
        p = multiprocessing.Process(target=(unique_val_call), args = (i, l, df_source, Save_path, model_prefix, ))
        p.start()
        processes.append(p)
    #time.sleep(5)
    dif = (len(uniqueValues) - (parallel_processes * a))
    l = uniqueValues[((i+1)*a) : (parallel_processes*a)+dif]
    print(str(((i+1)*a)) + ' , ' +  str((parallel_processes*a)+dif))
    ##task_final = loop.create_task(unique_val_call(parallel_processes, l, df_source, Save_path, model_prefix))
    p1 = multiprocessing.Process(target=(unique_val_call), args = (i+1, l, df_source, Save_path, model_prefix, ))
    p1.start()
    #time.sleep(5)
    processes.append(p1)
    for process in processes:
        process.join()
    #await task_final
    