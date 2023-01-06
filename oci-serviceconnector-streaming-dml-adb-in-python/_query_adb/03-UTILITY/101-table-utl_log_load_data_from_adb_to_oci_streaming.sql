--DROP TABLE UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING;
-- Create table
create table UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING
(
  utl_log_key              VARCHAR2(50) default 'key' not null,
  utl_log_value            CLOB default 'value' not null,
  utl_log_dml              CHAR(1) default 'I',
  audit_status             CHAR(1) default 1,
  audit_create_date        CHAR(19) default TO_CHAR(SYSDATE-(5/24),'DD-MM-YYYY HH24:MI:SS') not null,
  audit_activity_date      CHAR(19) default TO_CHAR(SYSDATE-(5/24),'DD-MM-YYYY HH24:MI:SS') not null,
  audit_user               VARCHAR2(250) default 'app' not null,
  audit_wkfrunid           NUMBER(30) default 0 not null
)
tablespace DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )
compress;
-- Add comments to the columns
comment on column UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING.audit_status
  is 'Estado del registro. Donde 1=Active y 0=Inactive.';
comment on column UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING.audit_create_date
  is 'Fecha de creacion del registro.';
comment on column UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING.audit_activity_date
  is 'Fecha de actualizacion del registro.';
comment on column UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING.audit_user
  is 'Usuario de creacion del registro.';
comment on column UTL_LOG_LOAD_DATA_FROM_ADB_TO_OCI_STREAMING.audit_wkfrunid
  is 'Workflow Run ID.';