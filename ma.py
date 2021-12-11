#!/usr/bin/python3
import sys
import fcntl
import logging
import time
import io
import datetime
import decimal
import statistics

from astm_bidirectional_common import my_sql , file_mgmt, print_to_log
#For mysql password
sys.path.append('/var/gmcs_config')
import astm_var
####Settings section start#####
logfile_name='/var/log/ma.log'
log=1
n_size=50
####Settings section end#####
'''

select sample_id,result,avg(result)
over (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) 
from result where result>0 and examination_id=5031 order by sample_id desc limit 40


'''
last_sample_id_dict={}

logging.basicConfig(filename=logfile_name,level=logging.DEBUG,format='%(asctime)s %(message)s')
if(log==0):
  logging.disable(logging.DEBUG)

print_to_log("Moving Average Logging Test","[OK]")

def check_if_new_result_arrived(ms,examination_id):
  global last_sample_id_dict
  prepared_sql='select max(sample_id) from result where examination_id=%s and result>0'
  data_tpl=(examination_id,)
  cur=ms.run_query_with_log(prepared_sql,data_tpl)
  if(cur!=None):
    r=ms.get_single_row(cur)
    print_to_log("max sample_id for {}".format(examination_id),r[0])    
    ms.close_cursor(cur)
    if(examination_id in last_sample_id_dict):
      if(last_sample_id_dict[examination_id]==r[0]):
        print_to_log("Last sample id is not changed {}".format(last_sample_id_dict),"{}:{}".format(examination_id,r[0]))
        return False
      else:
        print_to_log("Last sample id is changed {}".format(last_sample_id_dict),"{}:{}".format(examination_id,r[0]))        
        last_sample_id_dict.update({examination_id:r[0]})
        print_to_log("updated dictionary",format(last_sample_id_dict))


        prepared_sql_sample_data='select * from result where examination_id=%s and sample_id=%s'
        data_tpl_sample_data=(examination_id,r[0])
        cur_sample_data=ms.run_query_with_log(prepared_sql_sample_data,data_tpl_sample_data)
        r_sample_data=ms.get_single_row(cur_sample_data)
        return r_sample_data[0],r_sample_data[2] #sample id and result
    else:
      print_to_log("Examination not in dict:{}".format(last_sample_id_dict),examination_id)
      last_sample_id_dict.update({examination_id:r[0]})
      print_to_log("updated dictionary",format(last_sample_id_dict))
      return 0,0,0

def calculate_moving_average(ms,examination_id):
  chk=check_if_new_result_arrived(ms,examination_id)
  if(chk==False):
    print_to_log("Last sample id is not changed.. nothing to do for:",examination_id)
    return
  
  #prepared_sql='select avg(result) from result where examination_id=%s and result>0 order by sample_id desc limit %s'
  prepared_sql='select result from result where examination_id=%s and result>0 order by sample_id desc limit %s'
  data_tpl=(examination_id,n_size)
  cur=ms.run_query_with_log(prepared_sql,data_tpl)
  r_tuple=()
  if(cur!=None):
    r=ms.get_single_row(cur)
    while(r!=None):
      r_tuple=r_tuple+(decimal.Decimal(r[0]),)
      r=ms.get_single_row(cur)
      
    ms.close_cursor(cur)
    r_avg=statistics.mean(r_tuple)
 
    dt=datetime.datetime.now()
    print_to_log("datetime",dt.strftime("%Y-%m-%d-%H-%M-%S"))
      
    prepared_sql_insert='insert into moving_average (examination_id,date_time,avg_value,sample_id,value) values(%s,%s,%s,%s,%s)'
    data_tpl_insert=(examination_id,dt,r_avg,chk[0],chk[1])
    curi=ms.run_query_with_log(prepared_sql_insert,data_tpl_insert)

ms=my_sql()
ms.get_link(astm_var.my_host,astm_var.my_user,astm_var.my_pass,astm_var.my_db)

while True:
  calculate_moving_average(ms,5031)
  time.sleep(10)

ms.close_link()
