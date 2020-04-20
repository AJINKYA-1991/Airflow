from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from airflow.executors.local_executor import LocalExecutor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.models import BaseOperator
from poc_plugins import SFTPSensor
from datetime import datetime,timedelta
from airflow.hooks import MyHooks

S3_BUCKET = ''
S3_KEY = ''
default_args = {'email_on_failure': False,'email_on_retry': False,'retries': 1,'retry_delay': timedelta(minutes=5),'start_date': datetime(2020,4,14),}
schedule_interval = "@daily"
dsid = Variable.get("hh", deserialize_json=True).get("Batch_Loader_dtl")
hh_dsids = []
for key in dsid:
    hh_dsids.append(key)
data_source_size = len(hh_dsids)
def file_move_py(templates_dict,**kwargs):
    ftpath = kwargs.get('path',None)
    s3_bucket = kwargs.get('bucket', None)
    s3key = kwargs.get('s3_path', None)
    jobtype = kwargs.get('job_type',None)
    sshconn = kwargs.get('ssh_conn', None)
    s3conn = kwargs.get('s3_conn',None)
    data_date = templates_dict['data_date']
    filecount = kwargs.get('file_count',None)
    filecount = int(filecount)
    filenameprefix = templates_dict['filename_prefix']
    my_hook = MyHooks(templates_dict)
    datet = str(data_date)
    datadate = datetime.strptime(datet,"%Y-%m-%d").date().strftime('%Y%m%d')
    filelist = my_hook.get_ftp_details(ftpath,filenameprefix,filecount,datadate,sshconn)
    print(str(filelist))
    if filecount == 1:
	filename = str(filelist[0])
        my_hook.transfer_file_from_ftp_to_s3(sshconn,ftpath,filename,s3_bucket,s3key,s3conn)
    elif(jobtype == 'hhdetails' and len(filelist) == 0):
        dttime = datetime.strptime(datadate,"%Y%m%d").date()
        print(dttime.weekday())
        if (dttime.weekday() != 0):
            print('Skipping  files as they are loaded every Monday')
    else:
        for i in range(0,len(filelist)):
            filename = str(filelist[i])
            my_hook.transfer_file_from_ftp_to_s3(sshconn,ftpath,filename,s3_bucket,s3key,s3conn)

def create_subdag(main_dag, subdag_id,jobtype,dsid):
    subdag = DAG('{0}.{1}'.format(main_dag.dag_id, subdag_id),default_args=default_args)
    ftp_sense_task =SFTPSensor(task_id='FTP_Sensor_Task',filepath = dsid.get(dsid).get(jobtype).get("ftp_directory"),sftp_conn_id=Variable.get("hh", deserialize_json=True).get("FTPDetails"),input_dict={'data_date': '{{macros.ds_add(ds,-2)}}'},filename_prefix = dsid.get(dsid).get(jobtype).get("filename_prefix"),file_count = dsid.get(dsid).get(jobtype).get("file_count"),job_type = jobtype,poke_interval=120,retries=10,retry_delay=timedelta(minutes=10),dag=subdag)
    #Copy to s3
    copy_file_to_S3 = PythonOperator(task_id='move_file_to_S3',python_callable=file_move_py,templates_dict={'filename_prefix':dsid.get(dsid).get(jobtype).get("filename_prefix"),'data_date': '{{macros.ds_add(ds,-2)}}'},op_kwargs={"path": dsid.get(dsid).get(jobtype).get("ftp_directory"),"bucket":S3_BUCKET,"s3_path":S3_KEY,"ssh_conn":Variable.get("hh", deserialize_json=True).get("FTPDetails"),"s3_conn":"s3_conn_name","file_count":dsid.get(dsid).get(jobtype).get("file_count"),"job_type":jobtype},provide_context = True,dag=subdag)
    ftp_sense_task >> copy_file_to_S3
    
    return subdag

main_dag = DAG('hh_EventsAndChannelBatchCopy1',schedule_interval=schedule_interval,default_args=default_args,max_active_runs=1)
for i in range(0, data_source_size):
    jobtype = Variable.get("hh", deserialize_json=True).get("Batch_Loader_dtl").get(hh_dsids[i]).get("JobType")
    for j in range(0,len(jobtype)):
        dag_id1 = 'hh_' + str(jobtype[j]) + '_' + str(hh_dsids[i])
	complete = 'hh_Complete_' + str(jobtype[j]) + '_' + str(hh_dsids[i])
        my_subdag = SubDagOperator(task_id=dag_id1,dag=main_dag,retries=3,executor=LocalExecutor(),subdag = create_subdag(main_dag,dag_id1,str(jobtype[j]),hh_dsids[i]))
        completes = DummyOperator(task_id = complete,dag = main_dag)
        my_subdag >> completes
