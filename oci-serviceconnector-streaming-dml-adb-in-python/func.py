import base64
import io
import json
import logging
import pathlib
import pandas as pd
import pytest
import os
import oci
import oracledb
import zipfile

from fdk import fixtures
from fdk import response
from time import process_time
from pandas import json_normalize

# [Class:utl] Utility
class Utility():
    def base64_decode(encoded):
        base64_bytes = encoded.encode('utf-8')
        message_bytes = base64.b64decode(base64_bytes)
        return message_bytes.decode('utf-8')

    # Retrieve Secret
    def read_secret_value(secret_ocid):
        logger = logging.getLogger()        
        try:
            # Get instance principal context
            if (oci.config.validate_config(par_oci_config) is None):
                secret_client = oci.secrets.SecretsClient(par_oci_config)                
                response = secret_client.get_secret_bundle(secret_ocid)
                base64_Secret_content = response.data.secret_bundle_content.content

                return Utility.base64_decode(base64_Secret_content)
        except Exception as e:
            logger.error(str(e))
            raise

################################
#           Parameter          #
################################
# [Parameter:utl] Utility
utl_path                                = str(pathlib.Path(__file__).parent.absolute())
# [Parameter:oci]
par_oci_config                          = oci.config.from_file(utl_path + '/_oci/config')
# [Parameter:oci_adb] Autonomous Database
par_oci_adb_user_name_secret_ocid       = Utility.read_secret_value('ocid1.vaultsecret.oc1.iad.amaaaaaavoaa5zia5h2xoqogjl7eacvubji7eyax3orafftra6eidfhur2ma')
par_oci_adb_password_secret_ocid        = Utility.read_secret_value('ocid1.vaultsecret.oc1.iad.amaaaaaavoaa5zian2sgagsiqm5wdsyahbfn7ktylihyapk5v4zhvtaniema')
par_oci_adb_wallet_password_secret_ocid = Utility.read_secret_value('ocid1.vaultsecret.oc1.iad.amaaaaaavoaa5ziaddhnxt56tjso6souyodoj2erffa2h3vmswhpzidd6f7q')
par_oci_adb_service_name                = "adwdemo_high"
par_oci_adb_wallet_location             = utl_path + '/_oci/adb_wallet'
# [Parameter:oci_str] OCI Streaming
par_oci_str_tbl_unique_key              = ["PER_RUT", "DEP_FECINI", "LIC_NUMLIC"]

################################
#    Controlador de Eventos    #
################################
def handler(ctx, data: io.BytesIO=None):       
    logger = logging.getLogger()
    var_data = data.getvalue()

    try:
        if (var_data.decode(encoding='utf-8') != ''):
            logs = json.loads(var_data)
            logger.info('Received {} entries.'.format(len(logs)))
            
            #Variables
            var_key       = None
            var_value     = None
            var_timestamp = None

            # Decode [OCI Streaming]
            for idx, item in enumerate(logs):            
                if 'key' in item:
                    var_key = Utility.base64_decode(item['key'])
                else:
                    logger.warn('[OCI Streaming] No "key" in the message.')
                
                if 'value' in item:
                    var_value = Utility.base64_decode(item['value'])
                else:
                    logger.warn('[OCI Streaming] No "Value" in the message.')
                
                if 'timestamp' in item:
                    var_timestamp = str(item['timestamp'])
                else:
                    logger.error('[OCI Streaming] No "timestamp" in the message.')
                    break

                # df
                df_value = json_normalize(json.loads(var_value))
                var_df = []
                var_dml = str(df_value['op_type'][0])
                
                if(var_dml=='I'):
                    var_df = oci_str.get_select_after(df_value)
                elif (var_dml=='U'):
                    df_after = oci_str.get_select_after(df_value)
                    df_before = oci_str.get_select_before(df_value)
                    var_df = oci_str.set_update_before(par_oci_str_tbl_unique_key, df_before, df_after)
                elif (var_dml=='D'):
                    var_df = oci_str.get_select_before(df_value)

                var_value = var_df.to_json(orient='records')[1:-1].replace('},{', '} {')

                #print('==============='+var_dml+'===============')
                #print(var_value + '\n')

                # Execute procedure from Autnomous Database
                logger.info('[INI]['+ str(idx) +'] Execute Stored Procedure from Autnomous Database...')
                oci_adb.execute_stored_procedure('utl_sp_load_data_from_adb_to_oci_streaming_poc', [var_dml, var_key, var_value, var_timestamp])
        else:
            logger.error('[OCI Streaming] No message.')
    except (Exception, ValueError) as e:
        logger.error(str(e))
        raise

    return response.Response(ctx, status_code=200)

# [Class:oci_str] OCI Streaming
class oci_str():
    def get_select_before(input_df):
        before_list = []
        for col in input_df.columns:
            if col[0:7] == "before.":
                before_list.append(col)
        return input_df[before_list].rename(lambda x: x.replace("before.", ""), axis="columns")
    
    def get_select_after(input_df):
        after_list = []
        for col in input_df.columns:
            if col[0:6] == "after.":
                after_list.append(col)
        return input_df[after_list].rename(lambda x: x.replace("after.", ""), axis="columns")

    def set_update_before(table_unique_key, df_before, df_after):
        df_before.set_index(table_unique_key, inplace=True)
        df_before.update(df_after.set_index(table_unique_key))
        return df_before.reset_index()

# [Class:oci_adb] Autonomous Database
class oci_adb():
    def execute_stored_procedure(stored_name, par_list):
        logger = logging.getLogger()
        
        try:
            ts = process_time()
            conn = oracledb.connect(user            = par_oci_adb_user_name_secret_ocid,
                                    password        = par_oci_adb_password_secret_ocid,
                                    dsn             = par_oci_adb_service_name,
                                    config_dir      = par_oci_adb_wallet_location,
                                    wallet_location = par_oci_adb_wallet_location,
                                    wallet_password = par_oci_adb_wallet_password_secret_ocid)
            
            # create a cursor
            with conn.cursor() as cursor:
                # execute the insert statement
                cursor.callproc(stored_name, par_list)
                # commit the change
                conn.commit()

            te = process_time()
            logger.info('[END] Execute Stored Procedure...[DML='+ par_list[0] +'][seg: '+ str(te-ts) +']...[Succeded]')

        except (Exception, ValueError) as e:
            logger.error(str(e))
            raise


############[pytest]############
@pytest.mark.asyncio
async def test_parse_request_with_data():
    # Produce Test Message
    with open("streaming_test_message.json", "rb") as fh:
        data = io.BytesIO(fh.read())
    
    call = await fixtures.setup_fn_call(handler, content=data)

    content, status, headers = await call

    assert 200 == status
    # python -m pytest -v -s --tb=long func.py
    # fn -v deploy --app app-streaming
    # fn invoke app-streaming oci-serviceconnector-streaming-dml-adb-in-python