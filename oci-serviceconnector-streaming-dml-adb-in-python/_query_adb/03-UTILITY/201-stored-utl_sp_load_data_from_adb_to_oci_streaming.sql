create or replace PROCEDURE utl_sp_load_data_from_adb_to_oci_streaming_poc(
  par_dml                     IN   VARCHAR2 DEFAULT 'I',
  par_key                     IN   VARCHAR2 DEFAULT 'KEY',
  par_value                   IN   VARCHAR2 DEFAULT 'VALUE',
  par_timestamp               IN   NUMBER)        
AS
  var_query                   CLOB := NULL;
BEGIN
  /*----------------------------------------------------------------------------------------------------------------.
  |                                               [UTILITY] DATA-LAYER                                              |
  |-----------------------------------------------------------------------------------------------------------------|
  | PROJECT       : LAKEHOUSE                                                                                       |
  | LAYER         : UTILITY                                                                                         |
  | MODULE        : PROCEDURE                                                                                       |
  | DESCRIPTION   : 0.- [Pre-Requisitos]                                                                            |
  |                     => OCI Functions                                                                            |
  |                     https://github.com/jganggini/oci-functions/blob/main/oci-serviceconnector-streaming-dml-adb-in-python/_query_adb/03-UTILITY/101-table-utl_log_load_data_from_adb_to_oci_streaming.sql
  |                     https://github.com/jganggini/oci-functions/blob/main/oci-serviceconnector-streaming-dml-adb-in-python/_query_adb/03-UTILITY/102-table-stg_licencias_medicas.sql
  |                 1.- [UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING]                                               |
  |                     Inserta registros en el Log de ADB.                                                         |
  |                 2.- [DELETE:STG_LICENCIAS_MEDICAS]                                                              |
  |                     Eliminar datos de la tabla.                                                                 |
  |                 3.- [INSERT:STG_LICENCIAS_MEDICAS]                                                              |
  |                     Insertar Cambios en la tabla.                                                               |
  |-----------------------.----------------------.------------------------------------------------------------------|
  | DATA-LAYER            | SCHEMA NAME          | OBJECT NAME                                                      |
  |-----------------------|----------------------|------------------------------------------------------------------|
  | UTILITY               | UTL                  | utl_sp_load_data_from_adb_to_oci_streaming_poc                   |
  `-----------------------------------------------------------------------------------------------------------------|
                                                                                                                    |
  --[UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING]------------------------------------------------------------------*/
    BEGIN                                                                                                         --|
        INSERT INTO UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING (UTL_LOG_KEY,                                     --|
                                                                 UTL_LOG_VALUE,                                   --|
                                                                 UTL_LOG_DML,                                     --|
                                                                 AUDIT_WKFRUNID)                                  --|
                                                         VALUES (par_key,                                         --|
                                                                 par_value,                                       --|
                                                                 par_dml,                                         --|
                                                                 par_timestamp);                                  --|
        COMMIT;                                                                                                   --|
    EXCEPTION                                                                                                     --|
      WHEN OTHERS THEN                                                                                            --|
      DBMS_OUTPUT.PUT_LINE('[Error] Message: ' || SQLERRM);                                                       --|
    END;                                                                                                          --|
  /*[fin] Step 01--------------------------------------------------------------------------------------------------*/
                                                                                                                  --|
  --[DELETE:STG_LICENCIAS_MEDICAS]---------------------------------------------------------------------------------*/
    BEGIN                                                                                                         --|
      DELETE FROM stg_licencias_medicas WHERE (per_rut, dep_fecini, lic_numlic) IN                                --|
      (SELECT                                                                                                     --|
      jt.per_rut,                                                                                                 --|
      to_timestamp(jt.dep_fecini,'YYYY-MM-DD HH24:MI:SS') AS dep_fecini,                                          --|
      jt.lic_numlic                                                                                               --|
      FROM utl_log_load_data_from_adb_to_oci_streaming utl,                                                       --|
      JSON_TABLE(utl_log_value, '$'                                                                               --|
      COLUMNS(per_rut         NUMBER          PATH '$.PER_RUT',                                                   --|
              dep_fecini      VARCHAR(255)    PATH '$.DEP_FECINI',                                                --|
              lic_numlic      NUMBER          PATH '$.LIC_NUMLIC'))                                               --|
      AS jt                                                                                                       --|
      WHERE utl.audit_wkfrunid = par_timestamp);                                                                  --|
                                                                                                                  --|
      COMMIT;                                                                                                     --|
                                                                                                                  --|
    EXCEPTION                                                                                                     --|
      WHEN OTHERS THEN                                                                                            --|
      DBMS_OUTPUT.PUT_LINE('[Error] Message: ' || SQLERRM);                                                       --|
    END;                                                                                                          --|
  --[fin] Step 02--------------------------------------------------------------------------------------------------*/
                                                                                                                  --|
  --[INSERT:STG_LICENCIAS_MEDICAS]---------------------------------------------------------------------------------*/
    BEGIN                                                                                                         --|
        IF par_dml!='D' THEN                                                                                      --|
          INSERT INTO stg_licencias_medicas (PER_RUT,      DEP_FECINI,   FML_FORMUL,      LIC_NUMLIC,             --|
                                            DEP_DIALIC,   TCT_TIPCTA,   BAN_CODBAN,      DEP_CTABAN,              --|       
                                            DEP_CORREOE,  DEP_AREAFONO, DEP_FONO,        DEP_AREAFONO_COM,        --|
                                            DEP_FONO_COM, DEP_CELULAR,  DEP_FECTERVIGEN, DEP_LUGAR,               --|
                                            DEP_ESTADO,   DEP_FECGRA,   DEP_USUGRA,      DEP_FECACT,              --|
                                            DEP_USUACT,   DEP_CANAL,    NEW_STATUS)                               --|
          SELECT                                                                                                  --|
          jt.c01 AS PER_RUT,                                                                                      --|
          TO_TIMESTAMP((jt.c02),'YYYY-MM-DD HH24:MI:SS') AS DEP_FECINI,                                           --|
          (jt.c03) AS FML_FORMUL,                                                                                 --|
          (jt.c04) AS LIC_NUMLIC,                                                                                 --|
          (jt.c05) AS DEP_DIALIC,                                                                                 --|
          (jt.c06) AS TCT_TIPCTA,                                                                                 --|
          (jt.c07) AS BAN_CODBAN,                                                                                 --|
          (jt.c08) AS DEP_CTABAN,                                                                                 --|
          (jt.c09) AS DEP_CORREOE,                                                                                --|
          (jt.c10) AS DEP_AREAFONO,                                                                               --|
          (jt.c11) AS DEP_FONO,                                                                                   --|
          (jt.c12) AS DEP_AREAFONO_COM,                                                                           --|
          (jt.c13) AS DEP_FONO_COM,                                                                               --|
          (jt.c14) AS DEP_CELULAR,                                                                                --|
          (jt.c15) AS DEP_FECTERVIGEN,                                                                            --|
          (jt.c16) AS DEP_LUGAR,                                                                                  --|
          (jt.c17) AS DEP_ESTADO,                                                                                 --|
          (jt.c18) AS DEP_FECGRA,                                                                                 --|
          (jt.c19) AS DEP_USUGRA,                                                                                 --|
          (jt.c20) AS DEP_FECACT,                                                                                 --|
          (jt.c21) AS DEP_USUACT,                                                                                 --|
          (jt.c22) AS DEP_CANAL,                                                                                  --|
          (CASE WHEN (c17!='GRA') THEN 'S' ELSE jt.c17 END) AS NEW_STATUS                                         --|
          FROM utl_log_load_data_from_adb_to_oci_streaming utl,                                                   --|
          JSON_TABLE(utl_log_value, '$'                                                                           --|
          COLUMNS (c01 NUMBER       PATH '$.PER_RUT',                                                             --|
                  c02 VARCHAR(255) PATH '$.DEP_FECINI',                                                           --|
                  c03 NUMBER       PATH '$.FML_FORMUL',                                                           --|
                  c04 NUMBER       PATH '$.LIC_NUMLIC',                                                           --|
                  c05 VARCHAR(255) PATH '$.DEP_DIALIC',                                                           --|
                  c06 VARCHAR(255) PATH '$.TCT_TIPCTA',                                                           --|
                  c07 NUMBER       PATH '$.BAN_CODBAN',                                                           --|
                  c08 VARCHAR(255) PATH '$.DEP_CTABAN',                                                           --|
                  c09 VARCHAR(255) PATH '$.DEP_CORREOE',                                                          --|
                  c10 VARCHAR(255) PATH '$.DEP_AREAFONO',                                                         --|
                  c11 NUMBER       PATH '$.DEP_FONO',                                                             --|
                  c12 VARCHAR(255) PATH '$.DEP_AREAFONO_COM',                                                     --|
                  c13 VARCHAR(255) PATH '$.DEP_FONO_COM',                                                         --|
                  c14 NUMBER       PATH '$.DEP_CELULAR',                                                          --|
                  c15 VARCHAR(255) PATH '$.DEP_FECTERVIGEN',                                                      --|
                  c16 VARCHAR(255) PATH '$.DEP_LUGAR',                                                            --|
                  c17 VARCHAR(255) PATH '$.DEP_ESTADO',                                                           --|
                  c18 VARCHAR(255) PATH '$.DEP_FECGRA',                                                           --|
                  c19 VARCHAR(255) PATH '$.DEP_USUGRA',                                                           --|
                  c20 VARCHAR(255) PATH '$.DEP_FECACT',                                                           --|
                  c21 VARCHAR(255) PATH '$.DEP_USUACT',                                                           --|
                  c22 VARCHAR(255) PATH '$.DEP_CANAL'))                                                           --|
              AS jt                                                                                               --|
          WHERE utl.audit_wkfrunid = par_timestamp;                                                               --|
                                                                                                                  --|
          COMMIT;                                                                                                 --|
        END IF;                                                                                                   --|
                                                                                                                  --|
    EXCEPTION                                                                                                     --|
      WHEN OTHERS THEN                                                                                            --|
      DBMS_OUTPUT.PUT_LINE('[Error] Message: ' || SQLERRM);                                                       --|
    END;                                                                                                          --|
  --[fin] Step 03--------------------------------------------------------------------------------------------------*/
END;