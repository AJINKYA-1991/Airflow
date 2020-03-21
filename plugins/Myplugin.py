import copy
import logging
import os
import re
import os.path

import gnupg
import pysftp
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable

logger = logging.getLogger()
logger.setLevel(logging.getLevelName('INFO'))

class MyHooks(BaseHook):
    #template_fields = ('input_dict',)

    def __init__(self, input_dict, *args, **kwargs):
        self.input_dict = input_dict
	
	def delete_s3_folder(self, s3_conn_name, s3_bucket_name, s3_base_path):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()

        objects_to_delete = s3Client.list_objects(Bucket=s3_bucket_name, Prefix=s3_base_path)
        if objects_to_delete.get('Contents'):
            delete_keys = {'Objects': []}
            delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
            s3Client.delete_objects(Bucket=s3_bucket_name, Delete=delete_keys)
            logger.info("Deleted " + s3_bucket_name + "/" + s3_base_path)
        else:
            logger.info("Nothing to delete at " + s3_bucket_name + "/" + s3_base_path)
			
	def delete_s3_file(self, s3_conn_name, s3_bucket_name, s3_key):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()
        s3Client.delete_object(
            Bucket=s3_bucket_name,
            Key=s3_key
        )
        logger.info("Deleted (if existed) " + s3_bucket_name + "/" + s3_key)


    def copy_s3_folder(self, s3_conn_name,src_s3_bucket,src_folder, tgt_s3_bucket, tgt_folder, s3_acl="private"):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()

        file_list = s3Hook.list_keys(bucket_name=src_s3_bucket, prefix=src_folder, delimiter='/')

        for curr_key in file_list:
            fields = curr_key.split("/")
            file_name = fields[len(fields) - 1]

            if (file_name != "_SUCCESS"):
                s3Client.copy_object(
                    ACL=s3_acl,
                    Bucket=tgt_s3_bucket,
                    CopySource=src_s3_bucket + "/" + curr_key,
                    Key=tgt_folder + file_name
                )
                logger.info("Copied " + curr_key + " to " + tgt_s3_bucket + "/" + tgt_folder)

        s3Client.copy_object(
            ACL=s3_acl,
            Bucket=tgt_s3_bucket,
            CopySource=src_s3_bucket + "/" + src_folder + "_SUCCESS" ,
            Key=tgt_folder + "_SUCCESS"
        )
        logger.info("Copied " + src_s3_bucket + "/" + src_folder + "_SUCCESS")


    def copy_s3_file(self, s3_conn_name, src_s3_bucket, src_file, tgt_s3_bucket, tgt_file, s3_acl="private"):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()

        s3Client.copy_object(
            ACL=s3_acl,
            Bucket=tgt_s3_bucket,
            CopySource=src_s3_bucket + "/" + src_file,
            Key=tgt_file,
        )
        logger.info("Copied " + src_s3_bucket + "/" + src_file + " to " + tgt_s3_bucket + "/" + tgt_file)

    def copy_single_file_from_s3_folder(self, s3_conn_name,src_s3_bucket,src_folder, tgt_s3_bucket, tgt_path, s3_acl="private"):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()

        file_list = s3Hook.list_keys(bucket_name=src_s3_bucket, prefix=src_folder, delimiter='/')

        for curr_key in file_list:
            fields = curr_key.split("/")
            file_name = fields[len(fields) - 1]

            if (file_name != "_SUCCESS"):
                s3Client.copy_object(
                    ACL=s3_acl,
                    Bucket=tgt_s3_bucket,
                    CopySource=src_s3_bucket + "/" + curr_key,
                    Key=tgt_path
                )
                logger.info("Copied " + curr_key + " to " + tgt_s3_bucket + "/" + tgt_path)

    def download_single_s3_file_from_s3_folder(self, s3_conn_name, src_s3_bucket, src_folder, local_file):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()
        file_list = s3Hook.list_keys(bucket_name=src_s3_bucket, prefix=src_folder, delimiter='/')

        for curr_key in file_list:
            fields = curr_key.split("/")
            file_name = fields[len(fields) - 1]

            if (file_name != "_SUCCESS"):
                s3Client.download_file(Bucket=src_s3_bucket, Key=curr_key, Filename=local_file)
                logger.info("Copied " + curr_key + " to local file " + local_file)

    def download_single_s3_file(self, s3_conn_name,src_s3_bucket,src_file, local_file):
        s3Hook = S3Hook(s3_conn_name)
        s3Client = s3Hook.get_conn()
        s3Client.download_file(Bucket=src_s3_bucket, Key=src_file, Filename=local_file)
        logger.info("Copied " + src_file + " to local file " + local_file)
		
	def sftp_copy(self,local_file, remote_path):
        try:
            conn = pysftp.CnOpts()
            conn.hostkeys = None
            sftp = pysftp.Connection(host='********', username='***', cnopts=conn, password='*******')
            path = sftp.exists(remote_path)
            if not path:
                sftp.mkdir(remote_path, mode=755)

            sftp.put(local_file, remote_path + local_file.split('/')[-1])
            sftp.close()
            os.remove(local_file)
            print("Copied " + local_file + " to sftp@~/ " + remote_path)

        except Exception as e:
            print(e)
            print("SFTP upload failed: ")
            raise e
			
    def mkdir_p(self,sftp, remote_directory):
        """Change to this directory, recursively making new folders if needed.
        Returns True if any folders were created."""
        if remote_directory == '/':
        # absolute path so change directory to root
            sftp.chdir('/')
            return
        if remote_directory == '':
        # top-level relative directory must exist
            return
        try:
            sftp.chdir(remote_directory) # sub-directory exists
        except IOError:
            dirname, basename = os.path.split(remote_directory.rstrip('/'))
            self.mkdir_p(sftp, dirname) # make parent directories
            sftp.mkdir(basename) # sub-directory missing, so created it
            print("created directory ::"+basename)
            sftp.chdir(basename)
            return True
			
class CopyS3FolderOperator(BaseOperator):
    """
    Copies a S3 folder from one bucket to another using given ACL
    """

    template_fields = ["src_s3_path", "tgt_s3_path"]
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(
            self,
            s3_conn_id='s3_default',
            src_s3_bucket=None,
            src_s3_path=None,
            tgt_s3_bucket=None,
            tgt_s3_path=None,
            s3_acl="private",
            *args, **kwargs):
        super(CopyS3FolderOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.src_s3_bucket = src_s3_bucket
        self.src_s3_path = src_s3_path
        self.tgt_s3_bucket = tgt_s3_bucket
        self.tgt_s3_path = tgt_s3_path
        self.s3_acl = s3_acl

    def execute(self, context):
        tvd_hook = MyHooks(input_dict=None)
        tvd_hook.copy_s3_folder(self.s3_conn_id, self.src_s3_bucket, self.src_s3_path, self.tgt_s3_bucket, self.tgt_s3_path, self.s3_acl)

class CopyS3FileOperator(BaseOperator):
    """
    Copies a S3 file from one bucket to another using given ACL
    """
    template_fields = ["src_s3_file", "tgt_s3_file"]
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(
            self,
            s3_conn_id='s3_default',
            src_s3_bucket=None,
            src_s3_file=None,
            tgt_s3_bucket=None,
            tgt_s3_file=None,
            s3_acl="private",
            *args, **kwargs):
        super(CopyS3FileOperator,self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.src_s3_bucket = src_s3_bucket
        self.src_s3_file = src_s3_file
        self.tgt_s3_bucket = tgt_s3_bucket
        self.tgt_s3_file = tgt_s3_file
        self.s3_acl = s3_acl

    def execute(self, context):
        tvd_hook = MyHooks(input_dict=None)
        tvd_hook.copy_s3_file(s3_conn_name=self.s3_conn_id, src_s3_bucket=self.src_s3_bucket, src_file=self.src_s3_file, tgt_s3_bucket=self.tgt_s3_bucket, tgt_file=self.tgt_s3_file, s3_acl=self.s3_acl)


class CopySingleS3FileFromFolderOperator(BaseOperator):
    """
    Copies a single part file from given S3 folder to target S3 file path
    """
    template_fields = ["src_folder", "tgt_path"]
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(
            self,
            s3_conn_id='s3_default',
            src_s3_bucket=None,
            src_folder=None,
            tgt_s3_bucket=None,
            tgt_path=None,
            s3_acl="private",
            *args, **kwargs):
        super(CopySingleS3FileFromFolderOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.src_s3_bucket = src_s3_bucket
        self.src_folder = src_folder
        self.tgt_s3_bucket = tgt_s3_bucket
        self.tgt_path = tgt_path
        self.s3_acl = s3_acl

    def execute(self, context):
        tvd_hook = MyHooks(input_dict=None)
        tvd_hook.copy_single_file_from_s3_folder(s3_conn_name=self.s3_conn_id, src_s3_bucket=self.src_s3_bucket, src_folder=self.src_folder,
                                                 tgt_s3_bucket=self.tgt_s3_bucket, tgt_path=self.tgt_path, s3_acl=self.s3_acl)
												 
class TVDPlugins(AirflowPlugin):
    name = "TVDPlugins"
    hooks = [MyHooks]
    operators = [CopyS3FolderOperator, CopyS3FileOperator, CopySingleS3FileFromFolderOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []