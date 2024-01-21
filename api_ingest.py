import logging
import os
import argparse
import yaml
import datetime as dt
from time import perf_counter

from util.log import setup_log
from target.db.snowflake import Snowflake
from util.aws import S3, ParameterStore
from util.local import removes_files_in_local_folder

PROJECT_ROOT = os.path.dirname(__file__)
S3_BUCKET = ''
NUM_RECORDS_PER_FETCH = 10000
NUM_FILES_PER_PUSH = 1


def push_to_target(s3bucket: S3, localpath: str, s3path: str, target: Snowflake, scriptpath: str):
    s3bucket.upload_folder(localpath, s3path)
    target.run_script(scriptpath)

def main():
    #Set variables
    all_ingestion_stats = []
    s3bucket = S3(S3_BUCKET)

    #Start Logging
    setup_log(os.path.join(PROJECT_ROOT,f'logs/api_ingest_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.log'))
    
    #Parse command line arguments for yaml configuration
    parser = argparse.ArgumentParser(description="Ingests data into Snowflake")
    parser.add_argument('config_yaml', type=str, help=str(os.path.join(PROJECT_ROOT,'full_load.yaml')))
    args = parser.parse_args()

    #Load yaml configuration
    with open(args.config_yaml, 'r') as file:
        try:
            config = yaml.safe_load(file)
            logging.info('Using YAML File {args.config_yaml}')
        except Exception as e:
            logging.debug('YAML File: {args.config_yaml}')
            logging.error('Failed to load YAML file')

    for table in config.keys():
        #Set variables and create dirs when necessary
        results = True
        s3path = '/'.join([config[table]['Environment'],config[table]['Source']['platform'],config[table]['Source']['table']]).upper().replace(' ','_')
        datapath = os.path.join(PROJECT_ROOT,'data',config[table]['Environment'],config[table]['Source']['platform'],config[table]['Source']['table'])
        if not os.path.isdir(localpath):
            os.makedirs(localpath)

        #Get credentials from AWS SSM
        source_key = f'/{config[table]['Environment']}/{config[table]['Source']['platform']}'
        target_key = f'//{config[table]['Environment']}/{config[table]['Target']['platform']}/{config[table]['Target']['database']}/{config[table]['Target']['schema']}'
        secrets = ParameterStore()
        target_dict = secrets.get_dict(target_key)

        #Create Source and Target
        target = Snowflake(target_dict)
        
        #Track starting statistics
        start = perf_counter()
        ingestion_stats = {
            'db_name': config[table]['Target']['database'],
            'schema_name': config[table]['Target']['schema'],
            'table_name': config[table]['Target']['schema'],
            'started_at': dt.datetime.now(),
        }

        #Truncate if necessary
        if config['Load Type'] == 'reload':
            full_table_name = '.'.join(config['Target']['database'],config['Target']['schema'],config['Target']['table'])
            target.truncate(full_table_name)

        #Data extraction loop
        while results:
            #Generate data files locally

            #Upload to S3 and merge to Snowflake

            #Check soft deletes if necessary

            #Clean up S3 and local folder
            s3bucket.delete_s3_folder()
            removes_files_in_local_folder()

        #Track ending statistics
if __name__ == '__main__':
    main()