#!/usr/bin/python3
import sys
import fcntl
import logging
import time
import io
import datetime

from astm_bidirectional_common import my_sql , file_mgmt, print_to_log
#For mysql password
sys.path.append('/var/gmcs_config')
import astm_var
####Settings section start#####
logfile_name='/var/log/ma.log'
log=1
####Settings section end#####
'''

select sample_id,result,avg(result)
over (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) 
from result where result>0 and examination_id=5031 order by sample_id desc limit 40


'''
logging.basicConfig(filename=logfile_name,level=logging.DEBUG,format='%(asctime)s %(message)s')
if(log==0):
  logging.disable(logging.DEBUG)

print_to_log("Moving Average Logging Test","[OK]")

def calculate_moving_average(examination_id):
  ms=my_sql()
  con=ms.get_link(astm_var.my_host,astm_var.my_user,astm_var.my_pass,astm_var.my_db)

  prepared_sql='select avg(result) from result where examination_id=%s and result>1 order by sample_id desc limit 50'
  data_tpl=(examination_id,)
  try:
    cur=ms.run_query(con,prepared_sql,data_tpl)
    msg=prepared_sql
    print_to_log('prepared_sql:',msg)
    msg=data_tpl
    print_to_log('data tuple:',msg)
    print_to_log('cursor:',cur)

    r=ms.get_single_row(cur)
    ms.close_cursor(cur)
  except Exception as my_ex:
    msg=prepared_sql
    print_to_log('prepared_sql:',msg)
    msg=data_tpl
    print_to_log('data tuple:',msg)
    print_to_log('exception description:',my_ex)

  dt=datetime.datetime.now()
  print_to_log("datetime",dt.strftime("%Y-%m-%d-%H-%M-%S"))
      
  prepared_sql_insert='insert into moving_average (examination_id,date_time,avg_value) values(%s,%s,%s)'
  data_tpl_insert=(examination_id,dt,r[0])
  cur=ms.run_query(con,prepared_sql_insert,data_tpl_insert)
    
  ms.close_link(con)



while True:
  calculate_moving_average(5031)
  time.sleep(10)
