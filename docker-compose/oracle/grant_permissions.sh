#!/bin/sh
set -eu

sqlplus -s / as sysdba <<EOF
declare
  user_count integer;
begin
  select count(*) into user_count
  from dba_users
  where username = 'C##HARRY_MONITORING';

  if user_count = 0 then
    execute immediate 'create user c##harry_monitoring identified by "${ORACLE_PASSWORD}" container=all';
  end if;
end;
/
alter user c##harry_monitoring identified by "${ORACLE_PASSWORD}" container=all;
alter user c##harry_monitoring set container_data=all container=current;
grant create session, select_catalog_role to c##harry_monitoring container=all;

alter session set container=freepdb1;
alter user pdbadmin identified by "${ORACLE_PASSWORD}";
grant unlimited tablespace to pdbadmin;
grant select_catalog_role to pdbadmin;
grant execute on dbms_aq to pdbadmin;
grant execute on dbms_aqadm to pdbadmin;
grant execute on dbms_aqin to pdbadmin;
grant execute on dbms_aqjms_internal to pdbadmin;
grant execute on dbms_teqk to pdbadmin;
grant execute on DBMS_RESOURCE_MANAGER to pdbadmin;
grant select on sys.aq\$_queue_shards to pdbadmin;
grant select on sys.v_\$diag_alert_ext to pdbadmin;
grant select on user_queue_partition_assignment_table to pdbadmin;
exit;
EOF
