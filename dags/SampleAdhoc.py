from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from FirstAdhocOperator import BasicOperator, BaseSensorOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG('my_testsensor__dag2', description='Another tutorial DAG',
                          schedule_interval='*/30 * * * *',
                                                      start_date=datetime(2020, 3, 10), catchup=False)
copy_command = "cp /home/ajinkyam/airflow/dags/scripts/inputfiles/abc.txt /home/ajinkyam/files/"
dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

sensor_task = MySampleSensor(task_id='my_sensor_task', poke_interval=30, dag=dag)

operator_task = MyFirstOperator(my_operator_param='This is a test.',
                                                task_id='my_first_operator_task', dag=dag)
copybash_task =  BashOperator(task_id= 'Copy',bash_command=copy_command,dag=dag)

dummy_task >> sensor_task >> copybash_task >> operator_task