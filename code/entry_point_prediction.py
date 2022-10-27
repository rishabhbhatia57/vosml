# -*- coding: utf-8 -*-
"""
Created on Mon Aug  9 09:01:35 2021

@author: Administrator
"""

import pandas as pd
import mysql.connector
import time
from datetime import datetime
#import asyncio
#!pip install asyncio
#import nest_asyncio
#!pip3 install nest_asyncio
#nest_asyncio.apply()
import model_utilization
import sqlalchemy
import urllib.parse as urlquote
import sys
from logs import setup_custom_logger

logger = setup_custom_logger('root')

db_name = sys.argv[1]

batchno = sys.argv[2]
analstageseq = sys.argv[3]
ruleseq = sys.argv[4]  

now = datetime.now()
#loop = asyncio.get_event_loop()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
start = time.time()


model_prefix  = ''
partition_col = "UniqueKey"
db_username   = 'cbiadbuser'
db_passw      = 'Platform@2018'
db_ip         = '127.0.0.1'
#db_name       = 'EthosBI'
input_table_name    = 'R_InputTable'
#model_path          = 'E:/Jash/Procedural code/Models/'
# model_path          = '/home/ubuntu/vosml/model/' + db_name + '/'

# model_path          = 'C:/Users/HP/Desktop/CBIA Projects/Vosml/model/'+db_name+'/'
model_path          = 'E:/MPSTME/Sem 11 (Internship)/vosml/vosml/model/'+db_name+'/'
#R_inputtable col names
output_prediction_col = 'SM1'
output_date_col       = 'DM7'
output_partition_col  = 'CaseID'
model_type_col_name   = "DM5" 
model_last_date_col   = 'DM8'
from_date_col         = 'From_Date'
to_date_col            = 'To_Date'
output_table_name = 'R_OutputTable'
frequency      = 'w'

parallel_processes = 1
debug_mode = 'ON'



print("------------Initializing Prediction queries------------")
logger.info("------------Initializing Prediction queries------------")
print('Selected Debug mode: '+debug_mode)
#sql_query = 'select distinct p.dimkey from test.fossil_performbi p,(select a.dimkey from test.fossil_performbi a group by a.dimkey having count(a.dimkey)>2) q where p.dimkey = q.dimkey'
Prediction_sql_query = 'Select ' + from_date_col + ',' +  to_date_col  + ',' + partition_col + ',' + model_type_col_name + ' From ' + db_name + "." + input_table_name
df_R_InputTable = pd.read_excel('Data.xlsx', sheet_name ='R_InputTable')
df_R_InputTable = df_R_InputTable[['From_Date','To_Date', 'UniqueKey', 'DM5']]
Prediction_pre_sql_query = 'delete from ' + db_name + '.R_OutputTable'
Prediction_pre_sql_query_2 = ''
 


print("------------Prediction Started------------")
logger.info("------------Prediction Started------------")

#df_Source = df

# cnx = mysql.connector.connect(user = db_username, password = db_passw, host = db_ip, database = db_name, auth_plugin='mysql_native_password')
# mycursor = cnx.cursor()

# if Prediction_pre_sql_query != '':
#   mycursor.execute(Prediction_pre_sql_query)
#   cnx.commit()

# if Prediction_pre_sql_query_2 != '':
#   mycursor.execute(Prediction_pre_sql_query_2)
#   cnx.commit()

# mycursor.execute(Prediction_sql_query)
# result=mycursor.fetchall()
# df_sql_data = pd.DataFrame(result, columns= mycursor.column_names)
df_sql_data = df_R_InputTable
# cnx.close()


database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:%s@{1}/{2}?auth_plugin=mysql_native_password'.
                                       format(db_username, db_ip, db_name)
                                        % urlquote.quote_plus(db_passw))





#main_task = loop.create_task(fossil_model_utilization.prediction(df_sql_data, from_date_col, to_date_col , model_type_col_name, partition_col, 
#                                                                 database_connection, model_path, model_prefix, frequency, output_date_col,
#                                                                 output_prediction_col, output_partition_col, model_last_date_col, 
#                                                                 output_table_name, result_not_created))

logger.info("model_utilization task_creation started")
Arguments = [df_sql_data, from_date_col, to_date_col, model_type_col_name, partition_col, database_connection, 
                                       model_path, model_prefix, frequency, output_date_col, output_prediction_col, output_partition_col, 
                                       model_last_date_col, output_table_name, parallel_processes, debug_mode]
print('\n')
for i in range(len(Arguments)) :
    print(str(i)+": "+str(Arguments[i])) 
print('\n')                                    
model_utilization.task_creation(df_sql_data, from_date_col, to_date_col, model_type_col_name, partition_col, database_connection, 
                                       model_path, model_prefix, frequency, output_date_col, output_prediction_col, output_partition_col, 
                                       model_last_date_col, output_table_name, parallel_processes, debug_mode)

logger.info("model_utilization task_creation ended")
#await main_task


end = time.time()
End_time = now.strftime("%H:%M:%S")
print("Current Time =", End_time)
# total time taken
print(f"Runtime of the program is {(end - start)/60} mins")


