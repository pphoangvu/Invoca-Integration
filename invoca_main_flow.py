# Import necessary libraries/modules
import os
import sys

from prefect import flow, task, get_run_logger
from dateutil import parser
from prefect.blocks.system import JSON
from transaction_network_flow import transaction_network_flow
from transcript_flow import transcript_flow
from sqlalchemy.sql import text
from dcapiutils.dcutils import DCUtils

# Set Configuration
script_directory = os.path.dirname(os.path.abspath(__file__))
config_directory = os.path.join(script_directory, 'config.yaml')
utils_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(script_directory))
sys.path.insert(1, os.path.dirname(utils_directory))

# Set configuration variables
utils = DCUtils(config_directory)
config = utils.config
env = config['APP']['ENVIRONMENT']
debug = config['APP']['DEBUG']

# Configure SQL Server connection parameters
# Extract SQL Server connection parameters from configuration
sqlDriver = config['APP']['SQLSERVER']['DRIVER']
sqlDialect = config['APP']['SQLSERVER']['DIALECT']
sqlserver_auth_env = config['APP']['PREFECTBLOCKS']['DB'][env]
sqlserver_auth = JSON.load(sqlserver_auth_env) 
databaseName = config['DATABASE']['DATABASENAME']
host =  sqlserver_auth.value["server"]
port =  sqlserver_auth.value["port"]
username = sqlserver_auth.value["username"]
password =  sqlserver_auth.value["password"]
sql_connectionString = utils.build_sql_alchemy_url(databaseName, username, password, host, sqlDriver)
# Invoca API credentials from block
invoca_key = config['APP']['PREFECTBLOCKS']['API'][env]
api_auth = JSON.load(invoca_key) 
AUTH_TOKEN = api_auth.value["AUTH_TOKEN"]

# Define task to get the latest date from the database
@task
def get_last_start_time_local(sql_conn):
    # Query to get the maximum start time from transactions_network table
    query = "SELECT MAX(start_time_local) FROM transactions_network"
    result = sql_conn.execute(text(query)).fetchone()

    # If there's a result, process it 
    if result[0] is not None:
        # Check if the result is a string or a datetime object
        if isinstance(result[0], str):
            date = result[0]
            input_datetime = parser.parse(date)
            # Get the date part of the datetime object
            output_date = input_datetime.strftime("%Y-%m-%d")
    # If no result, assign a default date
    else:
        date = '2023-12-06'
    return output_date

# Define Prefect flow
@flow(retries=0, retry_delay_seconds=3)
def invoca_main_flow():
    try:
        # Initialize logging
        utils.logger = get_run_logger()
        utils.logger.info('start main flow..')
        utils.logger.info('connect to database server..')
        
        # Connect to database
        sql_conn = utils.sqlalchemy_connect(sql_connectionString)
    
        # Get latest date from database
        date = get_last_start_time_local(sql_conn)    
        utils.logger.info('pulling data from ' + date + ' for both transaction networks and transcripts')

        # Calling transaction_network flow
        utils.logger.info('calling transaction_network_flow from main')
        transaction_network_flow(date)
        utils.logger.info('finish running transaction_network_flow from main')

        # Calling transcript flow
        utils.logger.info('calling transcript_flow from main')
        transcript_flow(date)
        utils.logger.info('finish running transcript_flow from main')
        
        # Log completion message
        utils.logger.info('Invoca process completed successfully.')
        # Close database connection
        sql_conn.close()

    except Exception as e:
        # Log any errors and raise exception
        utils.logger.error(e)
        raise Exception(e)

# Run Prefect flow if the script is executed directly
if __name__ == '__main__':
    invoca_main_flow()

