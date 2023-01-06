import os
import io
import oci
import json
import base64
import zipfile
import tempfile
import logging
import pandas as pd

import cx_Oracle
import pathlib
import pytest
from fdk import response
from fdk import fixtures
from time import process_time
from pandas import json_normalize

# [Class:utl] Utility
class Utility():
    def base64_decode(encoded):
        base64_bytes = encoded.encode('utf-8')
        message_bytes = base64.b64decode(base64_bytes)
        return message_bytes.decode('utf-8')

    # Retrieve secret
    def read_secret_value(secret_id):
        logger = logging.getLogger()        
        try:
            # Get instance principal context
            if (oci.config.validate_config(par_oci_config) is None):
                secret_client = oci.secrets.SecretsClient(par_oci_config)                
                response = secret_client.get_secret_bundle(secret_id)
                base64_Secret_content = response.data.secret_bundle_content.content

                return Utility.base64_decode(base64_Secret_content)
        except Exception as e:
            logger.error(str(e))
            raise

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

    def set_update_before(table_unique_key,df_before, df_after):
        df_before.set_index(table_unique_key, inplace=True)
        df_before.update(df_after.set_index(table_unique_key))
        return df_before.reset_index()

################################
#           Parameter          #
################################
# [Parameter:utl] Utility
utl_path                          = str(pathlib.Path(__file__).parent.absolute())
# [Parameter:oci]
par_oci_config                    = oci.config.from_file(utl_path + '/_oci/config')
# [Parameter:oci_adb] Autonomous Database
par_oci_adb_user_name_secret_ocid = Utility.read_secret_value('ocid1.vaultsecret.oc1.iad.amaaaaaavoaa5ziaifr*****************************************')
par_oci_adb_password_secret_ocid  = Utility.read_secret_value('ocid1.vaultsecret.oc1.iad.amaaaaaavoaa5zian2s*****************************************')
par_oci_adb_service_name          = "atpemo_high"
par_oci_adb_wallet_location       = utl_path + '/wallet/Wallet_atpdemo.zip'
# [Parameter:tbl] Table
par_oci_tbl_unique_key            = ["PER_RUT", "DEP_FECINI", "LIC_NUMLIC"]

################################
#    Controlador de Eventos    #
################################
def handler(ctx, data: io.BytesIO=None):
    logger = logging.getLogger()

    try:
        logs = json.loads(data.getvalue())
        logger.info('Received {} entries.'.format(len(logs)))
        
        # Decode [OCI Streaming]
        for idx, item in enumerate(logs):            
            if 'key' in item:
                item['key'] = Utility.base64_decode(item['key'])
            if 'value' in item:
                item['value'] = Utility.base64_decode(item['value'])
            if 'value' in item:
                item['timestamp'] = str(item['timestamp'])

            # df
            df_value = json_normalize(json.loads(item['value']))
            var_df = []
            var_dml = str(df_value['op_type'][0])
            
            if(var_dml=='I'):
                var_df = Utility.get_select_after(df_value)
            elif (var_dml=='U'):
                df_after = Utility.get_select_after(df_value)
                df_before = Utility.get_select_before(df_value)
                var_df = Utility.set_update_before(par_oci_tbl_unique_key, df_before, df_after)
            elif (var_dml=='D'):
                var_df = Utility.get_select_before(df_value)

            var_value = var_df.to_json(orient='records')[1:-1].replace('},{', '} {')

            #print('==============='+var_dml+'===============')
            #print(var_value + '\n')

            # Execute procedure from Autnomous Database
            logger.info('[INI]['+ str(idx) +'] Execute Stored Procedure from Autnomous Database...')
            Connection.execute_stored_procedure('utl_sp_load_data_from_adb_to_oci_streaming_poc', var_dml, item['key'], var_value, item['timestamp'])

    except (Exception, ValueError) as e:
        logger.error(str(e))
        raise

    return response.Response(ctx, status_code=200)


# [Class:Connection] Autonomous Database
class Connection(cx_Oracle.Connection):
    def __init__(self, *args, cloud_config=None, **kwargs):
        self.temp_dir = None
        self.tns_entries = {}
        if cloud_config is not None:
            self.__setup_cloud_config(cloud_config)
            if len(args) > 2:
                dsn = args[2]
            else:
                dsn = kwargs.get('dsn')
            if dsn in self.tns_entries:
                dsn = self.tns_entries[dsn]
                if len(args) > 2:
                    args = args[:2] + (dsn,) + args[3:]
                else:
                    kwargs = kwargs.copy()
                    kwargs['dsn'] = dsn
        super(Connection, self).__init__(*args, **kwargs)

    def __del__(self):
        if self.temp_dir is not None:
            self.temp_dir.cleanup()

    def __setup_cloud_config(self, cloud_config):
        # extract files in wallet zip to a temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        zipfile.ZipFile(cloud_config).extractall(self.temp_dir.name)

        # parse tnsnames.ora to get list of entries and modify them to include
        # the wallet location
        fname = os.path.join(self.temp_dir.name, 'tnsnames.ora')
        for line in open(fname):
            pos = line.find(' = ')
            if pos < 0:
                continue
            name = line[:pos]
            entry = line[pos + 3:].strip()
            key_phrase = '(security='
            pos = entry.find(key_phrase) + len(key_phrase)
            wallet_entry = '(MY_WALLET_DIRECTORY=%s)' % self.temp_dir.name
            entry = entry[:pos] + wallet_entry + entry[pos:]
            self.tns_entries[name] = entry

    def execute_stored_procedure(stored_name, par_dml, par_key, par_value, par_timestamp):
        logger = logging.getLogger()
        
        try:
            ts = process_time()
            conn = Connection(par_oci_adb_user_name_secret_ocid,
                              par_oci_adb_password_secret_ocid,
                              par_oci_adb_service_name,
                              cloud_config=par_oci_adb_wallet_location)
            
            # create a cursor
            with conn.cursor() as cursor:
                # execute the insert statement
                cursor.callproc(stored_name, [par_dml, par_key, par_value, par_timestamp])
                # commit the change
                conn.commit()

            te = process_time()
            logger.info('[END] Execute Stored Procedure...[DML='+ par_dml +'][seg: '+ str(te-ts) +']...[Succeded]')

        except cx_Oracle.Error as e:
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
    # fn invoke app-streaming fn-data-manipulation-language-for-autonomous-database