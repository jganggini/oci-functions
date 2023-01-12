import base64

################################
#           Parameter          #
################################
# [Parameter:utl] Message
par_message_key_01   = '16102234_2016-06-09 00:00:00_9977370'
par_message_value_01 = '{"table": "LICMEDRAW.SIL_DEPOSITO", "op_type": "I", "op_ts": "2022-11-15 08:50:10.000000", "current_ts": "2022-11-15 08:50:10.100000", "pos": "00000000050010734001", "after": {"PER_RUT": 25102234, "DEP_FECINI": "2016-06-09 00:00:00", "FML_FORMUL": "3", "LIC_NUMLIC": 1077370, "DEP_DIALIC": "null", "TCT_TIPCTA": "V", "BAN_CODBAN": 12, "DEP_CTABAN": "123456789", "DEP_CORREOE": "correo@dominio.com", "DEP_AREAFONO": "32", "DEP_FONO": 1422300, "DEP_AREAFONO_COM": "null", "DEP_FONO_COM": "null", "DEP_CELULAR": 10788810, "DEP_FECTERVIGEN": "null", "DEP_LUGAR": "null", "DEP_ESTADO": "GRA", "DEP_FECGRA": "2016-07-05 21:17:33", "DEP_USUGRA": "ATRXLM", "DEP_FECACT": "2016-07-05 21:17:33", "DEP_USUACT": "ATRXLM", "DEP_CANAL": "null"}}'

par_message_key_02   = '16102234_2016-06-09 00:00:00_9977370'
par_message_value_02 = '{"table": "LICMEDRAW.SIL_DEPOSITO", "op_type": "U", "op_ts": "2022-11-15 08:50:20.000000", "current_ts": "2022-11-15 08:50:20.100000", "pos": "00000000050010734002", "before": {"PER_RUT": 25102234, "DEP_FECINI": "2016-06-09 00:00:00", "FML_FORMUL": "3", "LIC_NUMLIC": 1077370, "DEP_DIALIC": "null", "TCT_TIPCTA": "V", "BAN_CODBAN": 12, "DEP_CTABAN": "123456789", "DEP_CORREOE": "correo@dominio.com", "DEP_AREAFONO": "32", "DEP_FONO": 1422300, "DEP_AREAFONO_COM": "null", "DEP_FONO_COM": "null", "DEP_CELULAR": 10788810, "DEP_FECTERVIGEN": "null", "DEP_LUGAR": "null", "DEP_ESTADO": "GRA", "DEP_FECGRA": "2016-07-05 21:17:33", "DEP_USUGRA": "ATRXLM", "DEP_FECACT": "2016-07-05 21:17:33", "DEP_USUACT": "ATRXLM", "DEP_CANAL": "null"}, "after": {"PER_RUT": 25102234, "DEP_FECINI": "2016-06-09 00:00:00", "LIC_NUMLIC": 1077370, "DEP_CELULAR": 20788820, "DEP_ESTADO": "N"}}'

par_message_key_03   = '16102234_2016-06-09 00:00:00_9977370'
par_message_value_03 = '{"table": "LICMEDRAW.SIL_DEPOSITO", "op_type": "D", "op_ts": "2022-11-15 08:50:30.000000", "current_ts": "2022-11-15 08:50:30.100000", "pos": "00000000050010734003", "before": {"PER_RUT": 25102234, "DEP_FECINI": "2016-06-09 00:00:00", "FML_FORMUL": "3", "LIC_NUMLIC": 1077370, "DEP_DIALIC": "null", "TCT_TIPCTA": "V", "BAN_CODBAN": 12, "DEP_CTABAN": "123456789", "DEP_CORREOE": "correo@dominio.com", "DEP_AREAFONO": "32", "DEP_FONO": 1422300, "DEP_AREAFONO_COM": "null", "DEP_FONO_COM": "null", "DEP_CELULAR": 20788820, "DEP_FECTERVIGEN": "null", "DEP_LUGAR": "null", "DEP_ESTADO": "N", "DEP_FECGRA": "2016-07-05 21:17:33", "DEP_USUGRA": "ATRXLM", "DEP_FECACT": "2016-07-05 21:17:33", "DEP_USUACT": "ATRXLM", "DEP_CANAL": "null"}}'

# [Parameter:adb] Autonomous Database
par_oci_adb_user_name       = '******'
par_oci_adb_password        = '*******'
par_oci_adb_wallet_password = '********'

def base64_encode(encoded):
    message_bytes = encoded.encode('utf-8')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('utf-8')
    return base64_message

# [Return]
print('================================================================')
print('[streaming_test_message.json]')
print('================================================================')
print('key01==>\n'+base64_encode(par_message_key_01))
print('value01==>\n'+base64_encode(par_message_value_01)+'\n')
print('key02==>\n'+base64_encode(par_message_key_02))
print('value02==>\n'+base64_encode(par_message_value_02)+'\n')
print('key03==>\n'+base64_encode(par_message_key_03))
print('value03==>\n'+base64_encode(par_message_value_03)+'\n')
print('================================================================')
print('[OCI Vault]')
print('================================================================')
print('par_secret_user==>\n'+base64_encode(par_oci_adb_user_name))
print('par_secret_password==>\n'+base64_encode(par_oci_adb_password))
print('par_oci_adb_wallet_password==>\n'+base64_encode(par_oci_adb_wallet_password))

#python utl_base64_encode.py