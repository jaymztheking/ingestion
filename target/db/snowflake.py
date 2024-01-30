import datetime as dt
import snowflake.connector
import logging

class Snowflake:
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def __enter__(self):
        try:
            self.connection = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                role=self.config['role'],
            )
            logging.info('Successfully connected to Snowflake')
            self.cursor = self.connection.cursor()
        except Exception as e:
            logging.error(f'Failed to connect to Snowflake: {e}')
            raise e
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def run_query(self, query: str) -> list:
        try:
            with self as db:
                db.cursor.execute(query)
                return db.cursor.fetchall()
        except Exception as e:
            logging.debug(f'Query: {query}')
            logging.error(f'Failed to execute query: {e}')
            raise e
    
    def run_script(self, scriptfilepath: str) -> bool:
        try:
            with open(scriptfilepath, 'r') as script:
                query = script.read()
                with self as db:
                    db.cursor.execute(query)
                    return True
        except Exception as e:
            logging.debug(f'Script: {scriptfilepath}')
            logging.error(f'Failed to execute script: {e}')
            raise e

    def truncate(self, table: str) -> bool:
        query = f'TRUNCATE TABLE {table};'
        results = self.run_query(query)
        if results is not None and len(results) > 0:
            return True
        else:
            return False
        
    def get_row_count(self, table: str) -> int:
        query = f'SELECT COUNT(*) FROM {table};'
        results = self.run_query(query)
        if results is not None and len(results) > 0:
            return int(results[0][0])
        else:
            return None
        
    def get_latest_date(self, table: str, columns: list) -> dt.datetime:
        query = f'SELECT MAX(max_date_loaded) FROM {table}' \
                f'UNPIVOT (max_date_loaded FOR date in ({','.join(columns)}));'
        results = self.run_query
        if results is not None and len(results) > 0:
            return results[0][0]
        else:
            return None
        
    def write_stats_to_table(self, table: str, ingestion_stats: dict) -> bool:
        query = f'INSERT INTO {table} VALUES ('
        for value in ingestion_stats.values():
            query += str(value)+','
        query += ');'
        results = self.run_query(query)
        if results is not None and len(results) > 0:
            return True
        else:
            return False