from datetime import datetime, timedelta
from flatten_json import unflatten_list
import pandas as pd
import logging
import json
import time
from airflow import DAG
from airflow.hooks import S3Hook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from poc_plugins import FTPSSensor,SFTPSensor


#from mongo_plugin.operators.s3_to_mongo_operator import S3ToMongoOperator
from sftp_plugin import SFTPToS3Operator


start_time = time.time()
SSH_CONN_ID = 'ssh_default'
FILENAME = 'rpd_blueridge_palmerton_1_0_20200312.gz'
FILEPATH = '/shentel/harvest/angoss/'
SFTP_PATH = '/{0}/{1}'.format(FILEPATH, FILENAME)

S3_CONN_ID = 's3_conn_name'
S3_BUCKET = 'rovi2.0'                                                                                                                                                       S3_KEY = 'AirflowCheck/'

today = "{{ ds }}"

S3_KEY_TRANSFORMED = '{0}_{1}.json'.format(S3_KEY, today)
default_args = {
            'start_date': datetime(2020, 3,20, 0, 0, 0),
                'email': [],
                    'email_on_failure': True,
                        'email_on_retry': False,
                            'retries': 5,
                                'retry_delay': timedelta(minutes=5)
                                }

dag = DAG(
            'ftp_to_amazon_s3',
                    default_args=default_args,
                        catchup=False
                        )
						

def check_for_file_py(**kwargs):
        path = kwargs.get('path', None)
        sftp_conn_id = kwargs.get('sftp_conn_id', None)
        #filename = kwargs.get('templates_dict').get('filename', None)
        ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
        sftp_client = ssh_hook.get_conn().open_sftp()
        ftp_files = sftp_client.listdir(path)
        for filename in ftp_files:
            print(filename)
            logging.info('Filename: ' + str(filename))
        #if filename in ftp_files:
        #    return True
       # else:
       #     return False

with dag:
        #files = PythonOperator(task_id='check_for_file',python_callable=check_for_file_py,templates_dict={'filename': FILENAME},op_kwargs={"path": FILEPATH,"sftp_conn_id": SSH_CONN_ID},provide_context=True)
        dummy = DummyOperator(task_id = 'Start')
        #ftp_sense_task = FTPSSensor(task_id='FTP_Sensor_Task',path = FILEPATH,ftp_conn_id = 'fs_default',dag=dag)
        ftp_sense_task =SFTPSensor(task_id='FTP_Sensor_Task',filepath = FILEPATH,filename = FILENAME,sftp_conn_id=SSH_CONN_ID,dag=dag)
        #sftp = SFTPToS3Operator(task_id='retrieve_file',sftp_conn_id=SSH_CONN_ID,sftp_path=SFTP_PATH,s3_conn_id=S3_CONN_ID,s3_bucket=S3_BUCKET,s3_key=S3_KEY)
        #end = PythonOperator(task_id = 'Measuring_performance',python_callable = time_taken_py,provide_context = True)
		

dummy >> ftp_sense_task