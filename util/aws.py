import os
import json
import boto3
import logging

class S3:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def upload_file(self, filepath: str, s3path: str) -> bool:
        try:
            self.s3_client.upload_file(filepath, self.bucket_name, s3path)
            logging.info(f'File {filepath} uploaded to S3 successfully')
            return True
        except Exception as e:
            logging.info(f'File: {filepath}')
            logging.error(f'Failed to upload file to S3: {e}')
            return False
        
    def upload_folder(self, folderpath: str, s3folder: str) -> bool:
        try:
            for filename in os.listdir(folderpath):
                local_path = os.path.join(folderpath, filename)
                if os.path.isfile(local_path):
                    s3path = os.path.join(s3folder, filename)
                    self.upload_file(local_path, s3path)
            logging.info(f'Folder {folderpath} uploaded to S3 successfully')
            return True
        except Exception as e:
            logging.info(f'Folder: {folderpath}')
            logging.error(f'Failed to upload folder to S3: {e}')

    def delete_s3_folder(self, s3folder) -> bool:
        try:
            objects_to_delete = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3folder)
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete.get('Contents', [])]}
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete=delete_keys)
            logging.info(f'Objects in S3 folder {s3folder} deleted successfully')
            return True
        except Exception as e:
            logging.info(f'S3 Folder: {s3folder}')
            logging.error(f'Failed to delete objects from S3 folder: {e}')
            return False
        
class ParameterStore:
    def __init__(self):
        self.ssm_client = boto3.client('ssm')

    def get_dict(self, key) -> dict:
        try:
            response = self.ssm_client.get_parameter(Name=key, WithDecryption=True)
            parameter_value = response['Parameter']['Value']
            return json.loads(parameter_value)
        except Exception as e:
            logging.info(f'Key: {key}')
            logging.error(f'Failed to retrieve dict from SSM: {e}')
            return None