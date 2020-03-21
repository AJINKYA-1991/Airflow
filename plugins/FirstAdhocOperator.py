import logging
from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

log = logging.getLogger(__name__)


class BasicOperator(BaseOperator):
        @apply_defaults
        def __init__(self, my_operator_param, *args, **kwargs):
            self.operator_param = my_operator_param
            super(BasicOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
            log.info("Hello World!")
            log.info('operator_param: %s', self.operator_param)

class MyFirstSensorProgram(BaseSensorOperator):
    template_fields = tuple()
    ui_color = '#b5f2ff'

    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(MyFirstSensorProgram,self).__init__(*args,**kwargs)


    def poke(self,context):
        for root, subdirs, files in os.walk('/home/ajinkyam/airflow/dags/scripts/inputfiles'):
            if 'sample.txt' not in files:
                log.info("File Not found:: sensor willretry.")
				return False
			log.info("File found in the local path:")
            return True


class MyFirstPlugin(AirflowPlugin):
        name = "Sample_plugin"
        operators = [BasicOperator, MyFirstSensorProgram]

