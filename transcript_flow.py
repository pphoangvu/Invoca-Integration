from sqlalchemy.sql import text
import os
import sys
from prefect import flow, task, get_run_logger
import requests as req
import datetime
from dateutil import parser
from prefect.blocks.system import JSON

# Set Configuration
# Import configurations from YAML file and set variables
script_directory = os.path.dirname(os.path.abspath(__file__))
config_directory = os.path.join(script_directory, 'config.yaml')
utils_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(script_directory))
sys.path.insert(1, os.path.dirname(utils_directory))
from dcapiutils.dcutils import DCUtils

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

# Task to insert transcript into staging table
@task
def InsertTranscript(call_id, sql_conn, AUTH_TOKEN):
    # Retrieve transcript from API and insert into staging table
    geturl = "https://dentalcorporationcanada.invoca.net/call/transcript/"+call_id+"?transcript_format=caller_agent_conversation" # caller using agent

    response = req.get(geturl, headers={'Authorization': AUTH_TOKEN})
    
    # Print out the transcript with error
    if response.status_code != 200:
        utils.logger.info("Error call_id: " + str(call_id))
        utils.logger.info(response.text)
    else:
        # If there are no error, proceed with insert into staging stg_transcript table
        try:
            transcript_obj = {
                "call_record_id": call_id,
                "transcript": response.text
            }
            utils.logger.info("Success call_id: " + str(call_id))
            sql_conn.execute(text("""
                INSERT INTO stg_transcript (call_record_id, transcript) 
                VALUES(:call_record_id, :transcript)
                """), transcript_obj)
            sql_conn.commit()
            utils.logger.info("Inserted transcript for call_id : " + str(call_id))
        # Catch any error during insert
        except Exception as e:
            sql_conn.rollback()
            utils.logger.info("Error inserting transcript for call_id : " + str(call_id))
            utils.logger.info(e)
    return None

# Task to get call record IDs based on query date
@task
def getCallRecordId(date, sql_conn, AUTH_TOKEN):
    # Retrieve call record IDs based on query date
    utils.logger.info("Retrieve call id from date:  " + date)

    sql_query = text("""
        SELECT  DISTINCT complete_call_id 
        FROM transactions_network 
        WHERE signal_name != '' AND start_time_local >= :qdate
        """)
    
    result = sql_conn.execute(sql_query, {"qdate": date})
        
    for row in result.fetchall():
        InsertTranscript.fn(row[0],sql_conn,AUTH_TOKEN)

# Task to insert data into the final table
@task
def upsertIntoFinalTable(sql_conn):
    # Insert data into the final table, avoid duplicate with rank
    utils.logger.info("Inserting into final table transcript")
    mergequery = """
    MERGE INTO  transcript AS TARGET
    USING       stg_transcript AS SOURCE
    ON          SOURCE.call_record_id = TARGET.call_record_id 
    WHEN MATCHED THEN
        UPDATE SET
                transcript = SOURCE.transcript     
    WHEN NOT MATCHED THEN
        INSERT (call_record_id, transcript)
                VALUES (SOURCE.call_record_id, SOURCE.transcript);
    """
    sql_conn.execute(text(mergequery))
    sql_conn.commit()

# Define Prefect flow
@flow(retries=0, retry_delay_seconds=3)
def transcript_flow(date):
    # Main Prefect flow for the transcript processing
    try:
        # Initialize logging
        utils.logger = get_run_logger()
        utils.logger.info('start transcript_flow..')
        utils.logger.info('connect to database server..')
        
        # Connect to database
        sql_conn = utils.sqlalchemy_connect(sql_connectionString)

        # Truncate staging table
        utils.logger.info('truncating staging table stg_transcript')
        utils.truncate_table(sql_conn, 'stg_transcript')
       
        # Get latest date from database
        utils.logger.info('pulling data from ' + date )

        # Define URL for Invoca API
        start = datetime.datetime.now()
        getCallRecordId(date, sql_conn, AUTH_TOKEN)
        end = datetime.datetime.now()
        
        # Insert into final table from staging table
        upsertIntoFinalTable(sql_conn)
        utils.logger.info('Finish inserting into final table transcript from staging')


        # Total processing time
        utils.logger.info("elapsed time: " +  str(end-start))
        # Log completion message
        utils.logger.info('Invoca transript completed successfully.')
        # Close database connection
        sql_conn.close()

    except Exception as e:
        # Log any errors and raise exception
        utils.logger.error(e)
        raise Exception(e)

# Run Prefect flow if the script is executed directly
if __name__ == '__main__':
    transcript_flow()
