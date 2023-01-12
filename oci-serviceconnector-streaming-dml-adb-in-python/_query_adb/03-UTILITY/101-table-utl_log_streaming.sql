--DROP TABLE UTL_LOG_STREAMING;
-- Create table
create table UTL_LOG_STREAMING
(
  utl_log_key         VARCHAR2(50)  default 'key' null,
  utl_log_value       CLOB          default 'value' not null,
  utl_log_dml         CHAR(1)       default 'I',
  utl_log_offset      NUMBER(30)    default 0 not null,
  audit_create_date   CHAR(19)      default TO_CHAR(SYSDATE-(5/24),'DD-MM-YYYY HH24:MI:SS') not null
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
comment on column UTL_LOG_STREAMING.utl_log_key
  is 'Clave del mensaje.';
comment on column UTL_LOG_STREAMING.utl_log_value
  is 'Valor del mensaje.';
comment on column UTL_LOG_STREAMING.utl_log_dml
  is 'Estado del mensaje. Donde I=Insert, U=Update y D=Delete.';
comment on column UTL_LOG_STREAMING.utl_log_offset
  is 'Ubicación de un mensaje dentro de una secuencia/partición.';
comment on column UTL_LOG_STREAMING.audit_create_date
  is 'Fecha de creacion del registro.';