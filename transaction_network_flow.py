# Import necessary libraries/modules
from sqlalchemy.sql import text
import os
import sys
from prefect import flow, task, get_run_logger
import requests as req
import json
import datetime
from dateutil import parser
import pandas as pd
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

# Define task to insert transaction data into staging table
@task
def insertTransaction(start, network_url, sql_conn):
    # Initialize variables
    transaction_id = ''
    response_length = 4000
    finaldf = []
    # Loop until all transactions are retrieved
    while response_length == 4000:
        # Make request to Invoca API
        response = req.get(network_url + '=' + transaction_id, headers={'Authorization': AUTH_TOKEN})
        result = json.loads(response.text)
        
        # Log information about the response
        utils.logger.info(str(response.status_code) + " is the status code. Number of items returned: " + str(len(result)))

        # Update response_length and transaction_id for next iteration
        response_length = len(result)
        transaction_id = result[-1]['transaction_id']
        try:
            all_df = pd.DataFrame(result)
            utils.logger.info('Manipulate data...')
            # Add start date and rename columns
            all_df['Record_Transcript'] = ''
            all_df['last_inserted_date'] = start
            all_df.columns = all_df.columns.str.replace(' ', '_')
            all_df['Voicemail_Left'] = all_df['Voicemail_20_Seconds_Plus'] 
            all_df = all_df.drop('Voicemail_20_Seconds_Plus', axis=1)
        
            # Select specified columns
            selected_columns = [
                "transaction_id",
                "corrects_transaction_id",
                "transaction_type",
                "original_order_id",
                "advertiser_id",
                "advertiser_id_from_network",
                "advertiser_name",
                "advertiser_campaign_id",
                "advertiser_campaign_id_from_network",
                "advertiser_campaign_name",
                "media_type",
                "call_source_description",
                "promo_line_description",
                "virtual_line_id",
                "call_result_description_detail",
                "call_fee_localized",
                "advertiser_call_fee_localized",
                "city",
                "region",
                "qualified_regions",
                "repeat_calling_phone_number",
                "calling_phone_number",
                "mobile",
                "duration",
                "connect_duration",
                "ivr_duration",
                "keypresses",
                "keypress_1",
                "keypress_2",
                "keypress_3",
                "keypress_4",
                "dynamic_number_pool_referrer_search_engine",
                "dynamic_number_pool_referrer_search_keywords_id",
                "dynamic_number_pool_referrer_search_keywords",
                "dynamic_number_pool_referrer_ad",
                "dynamic_number_pool_referrer_ad_id",
                "dynamic_number_pool_referrer_ad_group",
                "dynamic_number_pool_referrer_ad_group_id",
                "dynamic_number_pool_referrer_referrer_campaign",
                "dynamic_number_pool_referrer_referrer_campaign_id",
                "dynamic_number_pool_referrer_keyword_match_type",
                "dynamic_number_pool_referrer_param1_name",
                "dynamic_number_pool_referrer_param1_value",
                "dynamic_number_pool_referrer_param2_name",
                "dynamic_number_pool_referrer_param2_value",
                "dynamic_number_pool_referrer_param3_name",
                "dynamic_number_pool_referrer_param3_value",
                "dynamic_number_pool_referrer_param4_name",
                "dynamic_number_pool_referrer_param4_value",
                "dynamic_number_pool_referrer_param5_name",
                "dynamic_number_pool_referrer_param5_value",
                "dynamic_number_pool_referrer_param6_name",
                "dynamic_number_pool_referrer_param6_value",
                "dynamic_number_pool_referrer_param7_name",
                "dynamic_number_pool_referrer_param7_value",
                "dynamic_number_pool_referrer_param8_name",
                "dynamic_number_pool_referrer_param8_value",
                "dynamic_number_pool_referrer_param9_name",
                "dynamic_number_pool_referrer_param9_value",
                "dynamic_number_pool_referrer_param10_name",
                "dynamic_number_pool_referrer_param10_value",
                "dynamic_number_pool_referrer_search_type",
                "dynamic_number_pool_pool_type",
                "dynamic_number_pool_id",
                "start_time_local",
                "start_time_xml",
                "start_time_utc",
                "start_time_network_timezone",
                "start_time_network_timezone_xml",
                "recording",
                "corrected_at",
                "opt_in_SMS",
                "complete_call_id",
                "transfer_from_type",
                "notes",
                "verified_zip",
                "hangup_cause",
                "signal_name",
                "signal_partner_unique_id",
                "signal_occurred_at",
                "signal_source",
                "revenue",
                "sale_amount",
                "destination_phone_number",
                "30_Seconds_Plus_Calls",
                "Not_Answered_by_Agent",
                "Answered_by_Voicemail",
                "Voicemail_Left",
                "invoca_id",
                "invoca_detected_destination",
                "gclid",
                "g_cid",
                "msclkid",
                "recording_active",
                "transfer_to_number",
                "google_analytics_property_id",
                "customer_id",
                "gclsrc",
                "landing_page",
                "utm_medium",
                "utm_source",
                "utm_campaign",
                "utm_content",
                "calling_page",
                "location_name",
                "ga_session_id",
                "New_Patient",
                "Record_Transcript",
                "last_inserted_date"
            ]

            #  specified columns to be inserted
            utils.logger.info('import into dataframe...')
            selected_df = all_df.loc[:, selected_columns]
            # insert into staging table            
            selected_df.to_sql(con=sql_conn, name='stg_transactions_network', schema='Invoca.dbo',if_exists='append', index=False)
            utils.logger.info(' Complete insert into stg_transactions_network table')  
        except Exception as e:
            utils.logger.error(e)
            utils.logger.info('Error inserting ' +transaction_id + ' into database')
            raise Exception(e)
          
# Define task to merge staging table with final table
@task
def insertIntoFinalTable(sql_conn):
    # Define merge query to upsert data from staging table into final table
    mergequery = """
    MERGE transactions_network AS TARGET
    USING stg_transactions_network as SOURCE
    ON SOURCE.transaction_id = TARGET.transaction_id
     WHEN MATCHED THEN
    UPDATE SET     
        transaction_id = SOURCE.transaction_id,
        corrects_transaction_id = SOURCE.corrects_transaction_id,
        transaction_type = SOURCE.transaction_type,
        original_order_id = SOURCE.original_order_id,
        advertiser_id = SOURCE.advertiser_id,
        advertiser_id_from_network = SOURCE.advertiser_id_from_network,
        advertiser_name = SOURCE.advertiser_name,
        advertiser_campaign_id = SOURCE.advertiser_campaign_id,
        advertiser_campaign_id_from_network = SOURCE.advertiser_campaign_id_from_network,
        advertiser_campaign_name = SOURCE.advertiser_campaign_name,
        media_type = SOURCE.media_type,
        call_source_description = SOURCE.call_source_description,
        promo_line_description = SOURCE.promo_line_description,
        virtual_line_id = SOURCE.virtual_line_id,
        call_result_description_detail = SOURCE.call_result_description_detail,
        call_fee_localized = SOURCE.call_fee_localized,
        advertiser_call_fee_localized = SOURCE.advertiser_call_fee_localized,
        city = SOURCE.city,
        region = SOURCE.region,
        qualified_regions = SOURCE.qualified_regions,
        repeat_calling_phone_number = SOURCE.repeat_calling_phone_number,
        calling_phone_number = SOURCE.calling_phone_number,
        mobile = SOURCE.mobile,
        duration = SOURCE.duration,
        connect_duration = SOURCE.connect_duration,
        ivr_duration = SOURCE.ivr_duration,
        keypresses = SOURCE.keypresses,
        keypress_1 = SOURCE.keypress_1,
        keypress_2 = SOURCE.keypress_2,
        keypress_3 = SOURCE.keypress_3,
        keypress_4 = SOURCE.keypress_4,
        dynamic_number_pool_referrer_search_engine = SOURCE.dynamic_number_pool_referrer_search_engine,
        dynamic_number_pool_referrer_search_keywords_id = SOURCE.dynamic_number_pool_referrer_search_keywords_id,
        dynamic_number_pool_referrer_search_keywords = SOURCE.dynamic_number_pool_referrer_search_keywords,
        dynamic_number_pool_referrer_ad = SOURCE.dynamic_number_pool_referrer_ad,
        dynamic_number_pool_referrer_ad_id = SOURCE.dynamic_number_pool_referrer_ad_id,
        dynamic_number_pool_referrer_ad_group = SOURCE.dynamic_number_pool_referrer_ad_group,
        dynamic_number_pool_referrer_ad_group_id = SOURCE.dynamic_number_pool_referrer_ad_group_id,
        dynamic_number_pool_referrer_referrer_campaign = SOURCE.dynamic_number_pool_referrer_referrer_campaign,
        dynamic_number_pool_referrer_referrer_campaign_id = SOURCE.dynamic_number_pool_referrer_referrer_campaign_id,
        dynamic_number_pool_referrer_keyword_match_type = SOURCE.dynamic_number_pool_referrer_keyword_match_type,
        dynamic_number_pool_referrer_param1_name = SOURCE.dynamic_number_pool_referrer_param1_name,
        dynamic_number_pool_referrer_param1_value = SOURCE.dynamic_number_pool_referrer_param1_value,
        dynamic_number_pool_referrer_param2_name = SOURCE.dynamic_number_pool_referrer_param2_name,
        dynamic_number_pool_referrer_param2_value = SOURCE.dynamic_number_pool_referrer_param2_value,
        dynamic_number_pool_referrer_param3_name = SOURCE.dynamic_number_pool_referrer_param3_name,
        dynamic_number_pool_referrer_param3_value = SOURCE.dynamic_number_pool_referrer_param3_value,
        dynamic_number_pool_referrer_param4_name = SOURCE.dynamic_number_pool_referrer_param4_name,
        dynamic_number_pool_referrer_param4_value = SOURCE.dynamic_number_pool_referrer_param4_value,
        dynamic_number_pool_referrer_param5_name = SOURCE.dynamic_number_pool_referrer_param5_name,
        dynamic_number_pool_referrer_param5_value = SOURCE.dynamic_number_pool_referrer_param5_value,
        dynamic_number_pool_referrer_param6_name = SOURCE.dynamic_number_pool_referrer_param6_name,
        dynamic_number_pool_referrer_param6_value = SOURCE.dynamic_number_pool_referrer_param6_value,
        dynamic_number_pool_referrer_param7_name = SOURCE.dynamic_number_pool_referrer_param7_name,
        dynamic_number_pool_referrer_param7_value = SOURCE.dynamic_number_pool_referrer_param7_value,
        dynamic_number_pool_referrer_param8_name = SOURCE.dynamic_number_pool_referrer_param8_name,
        dynamic_number_pool_referrer_param8_value = SOURCE.dynamic_number_pool_referrer_param8_value,
        dynamic_number_pool_referrer_param9_name = SOURCE.dynamic_number_pool_referrer_param9_name,
        dynamic_number_pool_referrer_param9_value = SOURCE.dynamic_number_pool_referrer_param9_value,
        dynamic_number_pool_referrer_param10_name = SOURCE.dynamic_number_pool_referrer_param10_name,
        dynamic_number_pool_referrer_param10_value = SOURCE.dynamic_number_pool_referrer_param10_value,
        dynamic_number_pool_referrer_search_type = SOURCE.dynamic_number_pool_referrer_search_type,
        dynamic_number_pool_pool_type = SOURCE.dynamic_number_pool_pool_type,
        dynamic_number_pool_id = SOURCE.dynamic_number_pool_id,
        start_time_local = SOURCE.start_time_local,
        start_time_xml = SOURCE.start_time_xml,
        start_time_utc = SOURCE.start_time_utc,
        start_time_network_timezone = SOURCE.start_time_network_timezone,
        start_time_network_timezone_xml = SOURCE.start_time_network_timezone_xml,
        recording = SOURCE.recording,
        corrected_at = SOURCE.corrected_at,
        opt_in_SMS = SOURCE.opt_in_SMS,
        complete_call_id = SOURCE.complete_call_id,
        transfer_from_type = SOURCE.transfer_from_type,
        notes = SOURCE.notes,
        verified_zip = SOURCE.verified_zip,
        hangup_cause = SOURCE.hangup_cause,
        signal_name = SOURCE.signal_name,
        signal_partner_unique_id = SOURCE.signal_partner_unique_id,
        signal_occurred_at = SOURCE.signal_occurred_at,
        signal_source = SOURCE.signal_source,
        revenue = SOURCE.revenue,
        sale_amount = SOURCE.sale_amount,
        destination_phone_number = SOURCE.destination_phone_number,
        [30_Seconds_Plus_Calls] = SOURCE.[30_Seconds_Plus_Calls],
        Not_Answered_by_Agent = SOURCE.Not_Answered_by_Agent,
        Answered_by_Voicemail = SOURCE.Answered_by_Voicemail,
        Voicemail_Left = SOURCE.Voicemail_Left,
        Record_Transcript = SOURCE.Record_Transcript,
        invoca_id = SOURCE.invoca_id,
        invoca_detected_destination = SOURCE.invoca_detected_destination,
        gclid = SOURCE.gclid,
        g_cid = SOURCE.g_cid,
        msclkid = SOURCE.msclkid,
        recording_active = SOURCE.recording_active,
        transfer_to_number = SOURCE.transfer_to_number,
        google_analytics_property_id = SOURCE.google_analytics_property_id,
        customer_id = SOURCE.customer_id,
        gclsrc = SOURCE.gclsrc,
        landing_page = SOURCE.landing_page,
        utm_medium = SOURCE.utm_medium,
        utm_source = SOURCE.utm_source,
        utm_campaign = SOURCE.utm_campaign,
        utm_content = SOURCE.utm_content,
        calling_page = SOURCE.calling_page,
        location_name = SOURCE.location_name,
        ga_session_id = SOURCE.ga_session_id,
        new_patient = SOURCE.new_patient,
        last_inserted_date = SOURCE.last_inserted_date               
    WHEN NOT MATCHED BY Target THEN
    	INSERT (transaction_id,corrects_transaction_id,transaction_type,original_order_id,advertiser_id,advertiser_id_from_network,advertiser_name,advertiser_campaign_id,advertiser_campaign_id_from_network,advertiser_campaign_name,media_type,call_source_description,promo_line_description,virtual_line_id,call_result_description_detail,call_fee_localized,advertiser_call_fee_localized,city,region,qualified_regions,repeat_calling_phone_number,calling_phone_number,mobile,duration,connect_duration,ivr_duration,keypresses,keypress_1,keypress_2,keypress_3,keypress_4,dynamic_number_pool_referrer_search_engine,dynamic_number_pool_referrer_search_keywords_id,dynamic_number_pool_referrer_search_keywords,dynamic_number_pool_referrer_ad,dynamic_number_pool_referrer_ad_id,dynamic_number_pool_referrer_ad_group,dynamic_number_pool_referrer_ad_group_id,dynamic_number_pool_referrer_referrer_campaign,dynamic_number_pool_referrer_referrer_campaign_id,dynamic_number_pool_referrer_keyword_match_type,dynamic_number_pool_referrer_param1_name,dynamic_number_pool_referrer_param1_value,dynamic_number_pool_referrer_param2_name,dynamic_number_pool_referrer_param2_value,dynamic_number_pool_referrer_param3_name,dynamic_number_pool_referrer_param3_value,dynamic_number_pool_referrer_param4_name,dynamic_number_pool_referrer_param4_value,dynamic_number_pool_referrer_param5_name,dynamic_number_pool_referrer_param5_value,dynamic_number_pool_referrer_param6_name,dynamic_number_pool_referrer_param6_value,dynamic_number_pool_referrer_param7_name,dynamic_number_pool_referrer_param7_value,dynamic_number_pool_referrer_param8_name,dynamic_number_pool_referrer_param8_value,dynamic_number_pool_referrer_param9_name,dynamic_number_pool_referrer_param9_value,dynamic_number_pool_referrer_param10_name,dynamic_number_pool_referrer_param10_value,dynamic_number_pool_referrer_search_type,dynamic_number_pool_pool_type,dynamic_number_pool_id,start_time_local,start_time_xml,start_time_utc,start_time_network_timezone,start_time_network_timezone_xml,recording,corrected_at,opt_in_SMS,complete_call_id,transfer_from_type,notes,verified_zip,hangup_cause,signal_name,signal_partner_unique_id,signal_occurred_at,signal_source,revenue,sale_amount,destination_phone_number,[30_Seconds_Plus_Calls],Not_Answered_by_Agent,Answered_by_Voicemail,Voicemail_Left,Record_Transcript,invoca_id,invoca_detected_destination,gclid,g_cid,msclkid,recording_active,transfer_to_number,google_analytics_property_id,customer_id,gclsrc,landing_page,utm_medium,utm_source,utm_campaign,utm_content,calling_page,location_name,ga_session_id,new_patient, last_inserted_date)
    	VALUES (SOURCE.transaction_id,
    	        SOURCE.corrects_transaction_id,
    			SOURCE.transaction_type,
    			SOURCE.original_order_id,
    			SOURCE.advertiser_id,
    			SOURCE.advertiser_id_from_network,
    			SOURCE.advertiser_name,
    			SOURCE.advertiser_campaign_id,
    			SOURCE.advertiser_campaign_id_from_network,
    			SOURCE.advertiser_campaign_name,
    			SOURCE.media_type,
    			SOURCE.call_source_description,
    			SOURCE.promo_line_description,
    			SOURCE.virtual_line_id,
    			SOURCE.call_result_description_detail,
    			SOURCE.call_fee_localized,
    			SOURCE.advertiser_call_fee_localized,
    			SOURCE.city,
    			SOURCE.region,
    			SOURCE.qualified_regions,
    			SOURCE.repeat_calling_phone_number,
    			SOURCE.calling_phone_number,
    			SOURCE.mobile,
    			SOURCE.duration,
    			SOURCE.connect_duration,
    			SOURCE.ivr_duration,
    			SOURCE.keypresses,
    			SOURCE.keypress_1,
    			SOURCE.keypress_2,
    			SOURCE.keypress_3,
    			SOURCE.keypress_4,
    			SOURCE.dynamic_number_pool_referrer_search_engine,
    			SOURCE.dynamic_number_pool_referrer_search_keywords_id,
    			SOURCE.dynamic_number_pool_referrer_search_keywords,
    			SOURCE.dynamic_number_pool_referrer_ad,
    			SOURCE.dynamic_number_pool_referrer_ad_id,
    			SOURCE.dynamic_number_pool_referrer_ad_group,
    			SOURCE.dynamic_number_pool_referrer_ad_group_id,
    			SOURCE.dynamic_number_pool_referrer_referrer_campaign,
    			SOURCE.dynamic_number_pool_referrer_referrer_campaign_id,
    			SOURCE.dynamic_number_pool_referrer_keyword_match_type,
    			SOURCE.dynamic_number_pool_referrer_param1_name,
    			SOURCE.dynamic_number_pool_referrer_param1_value,
    			SOURCE.dynamic_number_pool_referrer_param2_name,
    			SOURCE.dynamic_number_pool_referrer_param2_value,
    			SOURCE.dynamic_number_pool_referrer_param3_name,
    			SOURCE.dynamic_number_pool_referrer_param3_value,
    			SOURCE.dynamic_number_pool_referrer_param4_name,
    			SOURCE.dynamic_number_pool_referrer_param4_value,
    			SOURCE.dynamic_number_pool_referrer_param5_name,
    			SOURCE.dynamic_number_pool_referrer_param5_value,
    			SOURCE.dynamic_number_pool_referrer_param6_name,
    			SOURCE.dynamic_number_pool_referrer_param6_value,
    			SOURCE.dynamic_number_pool_referrer_param7_name,
    			SOURCE.dynamic_number_pool_referrer_param7_value,
    			SOURCE.dynamic_number_pool_referrer_param8_name,
    			SOURCE.dynamic_number_pool_referrer_param8_value,
    			SOURCE.dynamic_number_pool_referrer_param9_name,
    			SOURCE.dynamic_number_pool_referrer_param9_value,
    			SOURCE.dynamic_number_pool_referrer_param10_name,
    			SOURCE.dynamic_number_pool_referrer_param10_value,
    			SOURCE.dynamic_number_pool_referrer_search_type,
    			SOURCE.dynamic_number_pool_pool_type,dynamic_number_pool_id,
    			SOURCE.start_time_local,start_time_xml,start_time_utc,
    			SOURCE.start_time_network_timezone,start_time_network_timezone_xml,
    			SOURCE.recording,corrected_at,opt_in_SMS,complete_call_id,
    			SOURCE.transfer_from_type,notes,verified_zip,
    			SOURCE.hangup_cause,signal_name,
    			SOURCE.signal_partner_unique_id,
    			SOURCE.signal_occurred_at,
    			SOURCE.signal_source,revenue,
    			SOURCE.sale_amount,
    			SOURCE.destination_phone_number,
    			SOURCE.[30_Seconds_Plus_Calls],
    			SOURCE.Not_Answered_by_Agent,
    			SOURCE.Answered_by_Voicemail,
    			SOURCE.Voicemail_Left,
    			SOURCE.Record_Transcript,
    			SOURCE.invoca_id,
    			SOURCE.invoca_detected_destination,
    			SOURCE.gclid,
    			SOURCE.g_cid,
    			SOURCE.msclkid,
    			SOURCE.recording_active,
    			SOURCE.transfer_to_number,
    			SOURCE.google_analytics_property_id,
    			SOURCE.customer_id,
    			SOURCE.gclsrc,
    			SOURCE.landing_page,
    			SOURCE.utm_medium,
    			SOURCE.utm_source,
    			SOURCE.utm_campaign,
    			SOURCE.utm_content,
    			SOURCE.calling_page,
    			SOURCE.location_name,
                SOURCE.ga_session_id,
                SOURCE.new_patient,
                SOURCE.last_inserted_date );
            """
    
    # Execute merge query and commit changes
    sql_conn.execute(text(mergequery))
    sql_conn.commit()
    return None

# Define Prefect flow
@flow(retries=0, retry_delay_seconds=3)
def transaction_network_flow(date):
    try:
        # Initialize logging
        utils.logger = get_run_logger()
        utils.logger.info('start transaction_network_flow...')
        utils.logger.info('connect to database server...')
        
        # Connect to database
        sql_conn = utils.sqlalchemy_connect(sql_connectionString)

        # Truncate staging table
        utils.logger.info('truncating staging table stg_transactions_network')
        utils.truncate_table(sql_conn, 'stg_transactions_network')
       
        # Get latest date from database
        utils.logger.info('pulling data for transacions from: ' + date )

        # Define URL for Invoca API
        network_url = "https://dentalcorporationcanada.invoca.net/api/2020-10-01/networks/transactions/2137.json?&include_columns=$invoca_custom_columns,$invoca_default_columns&limit=4000&from="+date+"&start_after_transaction_id"

        # Insert data into staging table
        start = datetime.datetime.now()
        utils.logger.info("start inserting staging stg_transaction_network: " + str(start))
        insertTransaction(start, network_url,sql_conn)
        end = datetime.datetime.now()
        utils.logger.info("end inserting staging stg_transaction_network: " + str(end))

        # Upsert data into final table
        utils.logger.info("start upsert into transaction_network")
        insertIntoFinalTable(sql_conn)
        utils.logger.info("completed upsert into transaction_network")

        # Log completion message
        utils.logger.info('Invoca transaction network completed successfully.')
        # Close database connection
        sql_conn.close()

    except Exception as e:
        # Log any errors and raise exception
        utils.logger.error(e)
        raise Exception(e)

# Run Prefect flow if the script is executed directly
if __name__ == '__main__':
    transaction_network_flow()
