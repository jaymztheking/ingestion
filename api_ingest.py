import logging
import os
import argparse
import yaml
import datetime as dt
from time import perf_counter
from source.api.base import RESTAPIBase

from util.log import setup_log
from target.db.snowflake import Snowflake
from util.aws import S3, ParameterStore
from util.local import removes_files_in_local_folder, create_local_csv

PROJECT_ROOT = os.path.dirname(__file__)
S3_BUCKET = ''
NUM_FILES_PER_PUSH = 1000

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
        file_count = 0
        load_type = config['Load Type'].replace(' ','_').lower()
        env = config[table]['Environment'].replace(' ','_').lower()
        src_platform = config[table]['Source']['platform'].replace(' ','_').lower()
        src_table = config[table]['Source']['table'].replace(' ','_').lower()
        tgt_platform = config[table]['Target']['platform'].replace(' ','_').lower()
        tgt_database = config[table]['Target']['database'].replace(' ', '_').lower()
        tgt_schema = config[table]['Target']['schema'].replace(' ', '_').lower()
        tgt_table = config[table]['Target']['table'].replace(' ','_')
        s3path = '/'.join([env, src_platform, src_table])
        datapath = os.path.join(PROJECT_ROOT,'data', env, src_platform, load_type)
        if not os.path.isdir(datapath):
            os.makedirs(datapath)
        scriptpath = os.path.join(PROJECT_ROOT,'sql_scripts', env, src_platform, load_type)
        if not os.path.isdir(scriptpath):
            path = os.path.join(PROJECT_ROOT,'sql_scripts', env, src_platform)
            os.makedirs(path)
            for load in ['reload', 'full_load', 'incremental_load', 'soft_delete']:
                subpath = os.path.join(path, load)
                os.makedirs(subpath)

        #Get credentials from AWS SSM
        source_key = f'/{env}/{src_platform}'
        target_key = f'/{env}/{tgt_platform}/{tgt_database}/{tgt_schema}'
        secrets = ParameterStore()
        source_dict = secrets.get_dict(source_key)
        target_dict = secrets.get_dict(target_key)

        #Create Source and Target
        source = RESTAPIBase(source_dict)
        target = Snowflake(target_dict)
        
        try:
            #Track starting statistics
            start = perf_counter()
            ingestion_stats = {
                'db_name': tgt_database,
                'schema_name': tgt_schema,
                'table_name': tgt_table,
                'started_at': dt.datetime.now(),
            }

            #Truncate if necessary
            if config['Load Type'] == 'reload':
                full_table_name = '.'.join(tgt_database.upper(),tgt_schema.upper(),tgt_table.upper())
                target.truncate(full_table_name)

            #Data extraction loop
            while results:
                params = RESTAPIBase.get_initial_params()
                #Generate data files locally
                if source.authenticate():
                    endpoint = 'endpoint'
                    results, params, data = RESTAPIBase.get_data(load_type, endpoint, params)
                    create_local_csv(data, datapath, params['filename'])
                    file_count += 1

                    #Upload to S3 and merge to Snowflake
                    if (not results) or (file_count % NUM_FILES_PER_PUSH == 0):
                        s3bucket.upload_folder(datapath, s3path)
                        target.run_script(os.path.join(scriptpath, load_type, tgt_table.upper()+'.sql'))
                        s3bucket.move_to_processed(s3path)
                else:
                    break

            #Check soft deletes if necessary
            if config['Soft Delete']:
                target.run_script(os.path.join(scriptpath,'soft_delete', tgt_table.upper()+'.sql'))
            
            #Clean up S3 processed folder and local data folder
            s3bucket.delete_s3_folder('processed/'+s3path)
            removes_files_in_local_folder(datapath)

            #Track ending statistics
            end = perf_counter()
            ingestion_stats = {
                'ended_at': dt.datetime.now(),
                'total_time_taken_mins': round((end-start)/60, 2),
                'run_status': 'SUCCESSFUL',
                'error': 'NA'
            }
        except Exception as e:
            end = perf_counter()
            ingestion_stats = {
                'ended_at': dt.datetime.now(),
                'total_time_taken_mins': round((end-start)/60, 2),
                'run_status': 'UNSUCCESSFUL',
                'error': e
            }
        
        target.write_stats_to_table('.'.join([tgt_database, tgt_schema, 'ingestion_stats']), ingestion_stats)
 
if __name__ == '__main__':
    main()