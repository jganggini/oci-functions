import io
import oci
import json
import logging
import pytest
import pandas as pd
import pathlib
import numpy as np

from fdk import fixtures
from fdk import response

funcDefinition = {
    'status': {
        'returnCode': 0,
        'errorMessage': ''
    },
    'funcDescription': {
        'outputs': [{
            'name': 'word_count', 'dataType': 'integer'}
        ],
        'parameters': [{
            'name': 'textColumn',
            'displayName': 'Text Column',
            'description': 'Choose column to count words',
            'required': 'True',
            'value': {
                'type': 'column'
            }
        }],
        'bucketName': 'obj-demo',
        'isOutputJoinableWithInput': 'true'
    }
}

# [Class:utl] Utility
class Utility():
    def count(text):
        return len(text.split())

################################
#         Event Handler        #
################################
def handler(ctx, data: io.BytesIO = None):
    logger        = logging.getLogger()
    response_data = ''
    
    try:
        body = json.loads(data.getvalue())

        # [OAC] funcMode
        funcMode = body.get('funcMode')

        # [OAC] funcMode':'describeFunction'
        if funcMode == 'describeFunction':
           response_data = json.dumps(funcDefinition)
           logger.info('[END] response_data='+ str(response_data))

        # [OAC] funcMode':'executeFunction'
        elif funcMode == 'executeFunction':           
           
            # [OCI Object Storage] Input data from the OAC dataset
            input_method = body.get('input').get('method')
            if input_method == 'csv':
                # [Variables]
                bucket_name        = body.get("input").get("bucketName")
                input_object_name  = body.get("input").get("fileName") + body.get("input").get("fileExtension")
                rowID              = body.get('input').get('rowID')
                args               = body.get('args')
                
                # [OCI Object Storage:Input] Read file with the information of the OAC data set
                input_csv_path     = read_from_object_store(bucket_name, input_object_name)
                data               = pd.read_csv(input_csv_path, sep=",", quotechar="\"", encoding="utf-8", parse_dates=True, infer_datetime_format=True)               

                # [OCI Functions] Data Transformation Logic
                input_textColumn   = args.get('textColumn')
                output_data        = pd.DataFrame(np.vectorize(Utility.count)(data[input_textColumn]), columns=['word_count'])
                

                # [OCI Object Storage] Output data that will return to OAC
                output_object_name = body.get('output').get("fileName") + body.get("output").get("fileExtension")
                output_csv_path    = '/tmp/' + output_object_name
                output_data.to_csv(output_csv_path, index=True, index_label=rowID)
                write_to_object_store(bucket_name, output_object_name, output_csv_path)

                response_data = prepare_response(bucket_name, output_object_name)
                logger.info('[END] response_data='+ str(response_data))
            else:
                response_data = prepare_response_error('[CSV] Input method not supported: ' + input_method)
        else:
            response_data = prepare_response_error('[OAC] Invalid funcMode: ' + funcMode)
    except (Exception, ValueError) as e:
        logger.error(str(e))
        response_data = prepare_response_error('Error while executing ' + str(e))
    return response.Response(
        ctx, response_data,
        headers={'Content-Type': 'application/json'}
    )

def prepare_response(bucket_name, object_name):
    ret_template = """{
        "status": {
            "returnCode": "",
            "errorMessage": ""
            }
        }"""
    ret_json = json.loads(ret_template)
    ret_json["status"]["returnCode"] = 0
    ret_json["status"]["errorMessage"] = ""
    return json.dumps(ret_json)

def prepare_response_error(errorMsg):
    ret_template = """{
        "status": {
            "returnCode": "",
            "errorMessage": ""
            }
        }"""
    ret_json = json.loads(ret_template)
    ret_json['status']['returnCode'] = -1
    ret_json['status']['errorMessage'] = errorMsg
    return json.dumps(ret_json)

def read_from_object_store(bucket_name, object_name):
    logger        = logging.getLogger()

    # [Parameter:utl] Utility
    utl_path                          = str(pathlib.Path(__file__).parent.absolute())
    # [Parameter:oci]
    par_oci_profile_name              = 'OAC' #OAC/CLOUDSHELL
    par_oci_config                    = oci.config.from_file(utl_path + '/_oci/config', par_oci_profile_name)
    # [Parameter:oci_obj]
    par_oci_obj_object_storage_client = oci.object_storage.ObjectStorageClient(par_oci_config)
    par_oci_obj_namespace_name        = par_oci_obj_object_storage_client.get_namespace().data

    try:
        if (oci.config.validate_config(par_oci_config) is None):
            logger.info('[OCI_OBJ] Reading from object storage {}:{}'.format(bucket_name, object_name))            
            obj = par_oci_obj_object_storage_client.get_object(par_oci_obj_namespace_name,bucket_name, object_name)

        file = open('/tmp/'+object_name, 'wb')
        for chunk in obj.data.raw.stream(2048 ** 2, decode_content=False):
            file.write(chunk)
        file.close()

        return '/tmp/'+object_name
    except (Exception, ValueError) as e:
        logger.error(str(e))
        return None

def write_to_object_store(bucket_name, object_name, source_file):
    logger        = logging.getLogger()

    # [Parameter:utl] Utility
    utl_path                          = str(pathlib.Path(__file__).parent.absolute())
    # [Parameter:oci]
    par_oci_profile_name              = 'OAC' #OAC/CLOUDSHELL
    par_oci_config                    = oci.config.from_file(utl_path + '/_oci/config', par_oci_profile_name)
    # [Parameter:oci_obj]
    par_oci_obj_object_storage_client = oci.object_storage.ObjectStorageClient(par_oci_config)
    par_oci_obj_namespace_name        = par_oci_obj_object_storage_client.get_namespace().data

    try:
        if (oci.config.validate_config(par_oci_config) is None):
            logger.info("[OCI_OBJ] Writing to object storage {}:{}".format(bucket_name, object_name))
            with open(source_file, 'rb') as f:
                par_oci_obj_object_storage_client.put_object(par_oci_obj_namespace_name,bucket_name, object_name, f)

    except (Exception, ValueError) as e:
        logger.error(str(e))
        return None

'''
############[pytest]############
@pytest.mark.asyncio
async def test_parse_request_with_data():
    # [json] Test funcMode
    # funcMode':'executeFunction',   <-- Register OCI Function in OAC
    # funcMode':'describeFunction',  <-- Invoke OCI Function from Data Flow
    with open('describeFunction.json', 'rb') as fh:
        data = io.BytesIO(fh.read())
    
    call = await fixtures.setup_fn_call(handler, content=data)

    content, status, headers = await call

    assert 200 == status
    # python -m pytest -v -s --tb=long func.py
    # fn -v deploy --app app-oracle-analytics
    # fn invoke app-oracle-analytics oci-oracleanalytics-count-words-python
'''