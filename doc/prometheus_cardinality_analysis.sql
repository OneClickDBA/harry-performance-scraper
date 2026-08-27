-- Estimate the Prometheus cardinality required to retain Harry's PostgreSQL
-- data model with equivalent troubleshooting dimensions.
--
-- Run with psql against the Harry database:
--
--   psql -X -v lookback='24 hours' -f doc/prometheus_cardinality_analysis.sql \
--     harry_monitoring
--
-- Run it for three windows when preparing a capacity comparison:
--
--   2 hours   - approximate recent/Head-series pressure
--   24 hours  - show daily label churn
--   N days    - use the proposed Prometheus retention period to show all
--               unique series and samples that retention must accommodate
--
-- The report is intentionally conservative: it does not add environment,
-- region, cluster, job, instance, or replica labels normally present in a
-- Prometheus deployment. "Estimated series" is the number of distinct label
-- sets multiplied by the scalar fields that would require separate metrics.
-- "Estimated samples" is the PostgreSQL rows multiplied by those fields.
--
-- This is an analytical query over retained data. Start with a short lookback
-- on large installations and run it on a PostgreSQL reader when available.

\if :{?lookback}
\else
\set lookback '24 hours'
\endif

set statement_timeout = '15min';
set application_name = 'harry prometheus cardinality analysis';

drop table if exists pg_temp.harry_prometheus_cardinality;
create temporary table harry_prometheus_cardinality (
    category text not null,
    dataset text not null,
    postgres_rows bigint not null,
    distinct_label_sets bigint not null,
    scalar_metrics integer not null,
    estimated_prometheus_series numeric not null,
    estimated_prometheus_samples numeric not null,
    note text not null
);

-- User-defined additional metrics are already stored in metric/value form.
insert into harry_prometheus_cardinality
select
    'additional metrics',
    'oracle_metric_samples',
    count(*),
    count(distinct row(source_database, context, metric_name, labels)),
    1,
    count(distinct row(source_database, context, metric_name, labels)),
    count(*),
    'One series per metric name and complete JSON label set.'
from oracle_metric_samples
where collected_at >= now() - :'lookback'::interval;

-- Native operational data. Scalar metric counts include values exposed by the
-- current PostgreSQL views (for example used_percent and calculated rates).
insert into harry_prometheus_cardinality
select 'operational', 'oracle_database_status_samples', count(*),
       count(distinct row(source_database, inst_id, con_id, con_name,
                          instance_name, instance_status, database_status,
                          open_mode, database_role, cdb, platform_name)),
       1,
       count(distinct row(source_database, inst_id, con_id, con_name,
                          instance_name, instance_status, database_status,
                          open_mode, database_role, cdb, platform_name)),
       count(*),
       'Minimum one info/state series per database instance and container state.'
from oracle_database_status_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_instance_samples', count(*),
       count(distinct row(source_database, inst_id)), 7,
       count(distinct row(source_database, inst_id)) * 7,
       count(*) * 7,
       'Sessions, processes, CPU, SGA, and PGA require separate metrics.'
from oracle_instance_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_resource_limit_samples', count(*),
       count(distinct row(source_database, inst_id, resource_name)), 5,
       count(distinct row(source_database, inst_id, resource_name)) * 5,
       count(*) * 5,
       'Current, maximum, initial limit, effective limit, and unlimited state.'
from oracle_resource_limit_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_tablespace_samples', count(*),
       count(distinct row(source_database, tablespace_name, contents)), 4,
       count(distinct row(source_database, tablespace_name, contents)) * 4,
       count(*) * 4,
       'Used, free, maximum bytes, and used percentage.'
from oracle_tablespace_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_asm_diskgroup_samples', count(*),
       count(distinct row(source_database, inst_id, diskgroup_name)), 3,
       count(distinct row(source_database, inst_id, diskgroup_name)) * 3,
       count(*) * 3,
       'Total, free, and usable bytes. Used percentage can be derived.'
from oracle_asm_diskgroup_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_system_counter_samples', count(*),
       count(distinct row(source_database, inst_id, con_id, stat_name)), 4,
       count(distinct row(source_database, inst_id, con_id, stat_name)) * 4,
       count(*) * 4,
       'Cumulative value, delta, interval, and counter-reset state; rate is derived.'
from oracle_system_counter_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_wait_class_samples', count(*),
       count(distinct row(source_database, inst_id, con_id, wait_class)), 4,
       count(distinct row(source_database, inst_id, con_id, wait_class)) * 4,
       count(*) * 4,
       'Cumulative wait, delta, interval, and counter-reset state; rate is derived.'
from oracle_wait_class_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'operational', 'oracle_system_metric_samples', count(*),
       count(distinct row(source_database, inst_id, con_id, metric_name, unit)), 1,
       count(distinct row(source_database, inst_id, con_id, metric_name, unit)),
       count(*),
       'Already close to the Prometheus metric/value model.'
from oracle_system_metric_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'collector health', 'oracle_scrape_status', count(*),
       count(distinct row(source_database, collector)), 3,
       count(distinct row(source_database, collector)) * 3,
       count(*) * 3,
       'Success, duration, and sample count; data age is derived and error text is excluded.'
from oracle_scrape_status
where collected_at >= now() - :'lookback'::interval;

-- SQL statistics are the first high-cardinality area. Mutable module, schema,
-- child cursor, and plan labels create new series over time.
insert into harry_prometheus_cardinality
select 'SQL performance', 'oracle_sql_samples', count(*),
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, parsing_schema_name, module)), 15,
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, parsing_schema_name, module)) * 15,
       count(*) * 15,
       'Fifteen cumulative SQL workload counters; SQL text is counted separately.'
from oracle_sql_samples
where collected_at >= now() - :'lookback'::interval;

-- Session rows are represented as one conservative info/value series. Mapping
-- each state field to its own metric would increase these figures further.
insert into harry_prometheus_cardinality
select 'sessions and blocking', 'oracle_session_samples', count(*),
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          status, sql_id, sql_child_number, prev_sql_id, event,
                          wait_class, state, blocking_instance, blocking_session,
                          machine, program, module, action, service_name)), 1,
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          status, sql_id, sql_child_number, prev_sql_id, event,
                          wait_class, state, blocking_instance, blocking_session,
                          machine, program, module, action, service_name)),
       count(*),
       'Conservative one-series encoding; label changes create new series.'
from oracle_session_samples
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'sessions and blocking', 'oracle_blocking_session_samples', count(*),
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          sql_id, event, wait_class, blocking_instance,
                          blocking_session, blocking_username, blocking_sql_id,
                          blocking_event)), 1,
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          sql_id, event, wait_class, blocking_instance,
                          blocking_session, blocking_username, blocking_sql_id,
                          blocking_event)),
       count(*),
       'Conservative one-series encoding of each observed blocking relationship.'
from oracle_blocking_session_samples
where collected_at >= now() - :'lookback'::interval;

-- This identity retains the dimensions used for ASH-style troubleshooting.
-- sample_id and timestamps are deliberately excluded as labels; including
-- either would make almost every activity row a new series by construction.
insert into harry_prometheus_cardinality
select 'activity history', 'oracle_database_activity_samples', count(*),
       count(distinct row(
           source_database, inst_id, session_id, session_serial_number,
           session_type, user_id, sql_id, sql_child_number, sql_exec_id,
           top_level_sql_id, session_state, event, wait_class,
           blocking_session, blocking_session_serial_number, blocking_inst_id,
           current_object_id, current_file_number, current_block_number,
           program, module, action, machine, con_id, sample_source,
           sql_plan_hash_value, sql_full_plan_hash_value, sql_plan_line_id,
           service_hash, service_name, client_identifier)), 5,
       count(distinct row(
           source_database, inst_id, session_id, session_serial_number,
           session_type, user_id, sql_id, sql_child_number, sql_exec_id,
           top_level_sql_id, session_state, event, wait_class,
           blocking_session, blocking_session_serial_number, blocking_inst_id,
           current_object_id, current_file_number, current_block_number,
           program, module, action, machine, con_id, sample_source,
           sql_plan_hash_value, sql_full_plan_hash_value, sql_plan_line_id,
           service_hash, service_name, client_identifier)) * 5,
       count(*) * 5,
       'Wait time, time waited, PGA, TEMP, and sample duration; conservative because timestamps are not labels.'
from oracle_database_activity_samples
where sample_time >= now() - :'lookback'::interval;

-- Current-state detail tables are not conventional time series. The estimates
-- below show the minimum number of info/numeric series needed to make their
-- contents selectable by SQL ID and plan dimensions.
insert into harry_prometheus_cardinality
select 'SQL detail', 'oracle_sql_texts', count(*), count(*), 1, count(*), count(*),
       format('Not suitable for metric labels: %s SQL text bytes in the lookback working set.',
              coalesce(sum(octet_length(sql_fulltext)), 0))
from oracle_sql_texts
where last_referenced_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'SQL detail', 'oracle_sql_plans', count(*),
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, plan_line_id, operation, options,
                          object_owner, object_name, object_type, optimizer,
                          partition_start, partition_stop)), 6,
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, plan_line_id, operation, options,
                          object_owner, object_name, object_type, optimizer,
                          partition_start, partition_stop)) * 6,
       count(*) * 6,
       'Cost, cardinality, bytes, CPU cost, I/O cost, and TEMP; predicates are excluded from labels.'
from oracle_sql_plans
where last_referenced_at >= now() - :'lookback'::interval;

\echo
\echo 'Harry -> Prometheus equivalent cardinality by dataset'
select
    category,
    dataset,
    postgres_rows,
    distinct_label_sets,
    scalar_metrics,
    estimated_prometheus_series,
    estimated_prometheus_samples,
    round(estimated_prometheus_series * 100.0 /
          nullif(sum(estimated_prometheus_series) over (), 0), 2) as series_pct,
    note
from harry_prometheus_cardinality
order by estimated_prometheus_series desc, dataset;

\echo
\echo 'Totals by data category'
select
    category,
    sum(postgres_rows) as postgres_rows,
    sum(estimated_prometheus_series) as estimated_prometheus_series,
    sum(estimated_prometheus_samples) as estimated_prometheus_samples,
    round(sum(estimated_prometheus_series) * 100.0 /
          nullif(sum(sum(estimated_prometheus_series)) over (), 0), 2) as series_pct
from harry_prometheus_cardinality
group by category
order by estimated_prometheus_series desc;

\echo
\echo 'Grand total for lookback' :lookback
select
    :'lookback' as lookback,
    sum(postgres_rows) as postgres_rows,
    sum(distinct_label_sets) as distinct_label_sets,
    sum(estimated_prometheus_series) as estimated_prometheus_series,
    sum(estimated_prometheus_samples) as estimated_prometheus_samples,
    round(sum(estimated_prometheus_samples) /
          extract(epoch from :'lookback'::interval), 2) as equivalent_samples_per_second
from harry_prometheus_cardinality;

\echo
\echo 'High-cardinality label values observed in the lookback'
select *
from (
    select 'SQL IDs in SQL samples' as dimension,
           count(distinct row(source_database, sql_id)) as distinct_values
    from oracle_sql_samples
    where collected_at >= now() - :'lookback'::interval
    union all
    select 'SQL IDs in activity history',
           count(distinct row(source_database, sql_id))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
    union all
    select 'Session executions',
           count(distinct row(source_database, inst_id, session_id,
                              session_serial_number, sql_exec_id))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
    union all
    select 'Client machines', count(distinct row(source_database, machine))
    from oracle_session_samples
    where collected_at >= now() - :'lookback'::interval
    union all
    select 'Application modules', count(distinct row(source_database, module))
    from oracle_session_samples
    where collected_at >= now() - :'lookback'::interval
    union all
    select 'Wait events', count(distinct row(source_database, event))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
    union all
    select 'Current objects/blocks',
           count(distinct row(source_database, current_object_id,
                              current_file_number, current_block_number))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
) cardinality
order by distinct_values desc;

\echo
\echo 'PostgreSQL physical storage currently allocated, including partitions'
with roots as (
    select oid, relname, relkind
    from pg_class
    where relkind in ('p', 'r')
      and relname in (
      'oracle_metric_samples',
      'oracle_sql_texts',
      'oracle_sql_plans',
      'oracle_sql_samples',
      'oracle_session_samples',
      'oracle_blocking_session_samples',
      'oracle_database_activity_samples',
      'oracle_database_status_samples',
      'oracle_instance_samples',
      'oracle_resource_limit_samples',
      'oracle_tablespace_samples',
      'oracle_asm_diskgroup_samples',
      'oracle_system_counter_samples',
      'oracle_wait_class_samples',
      'oracle_system_metric_samples',
      'oracle_scrape_status',
      'oracle_latest_scrape_status'
      )
), sizes as (
    select
        roots.relname,
        pg_total_relation_size(tree.relid) as bytes
    from roots
    cross join lateral pg_partition_tree(roots.oid) tree
    where roots.relkind = 'p'
      and tree.isleaf
    union all
    select relname, pg_total_relation_size(oid)
    from roots
    where relkind = 'r'
)
select relname as relation,
       pg_size_pretty(sum(bytes)) as total_size
from sizes
group by relname
order by sum(bytes) desc;

\echo
\echo 'Interpretation warning:'
\echo 'Prometheus storage cannot be estimated reliably from PostgreSQL bytes.'
\echo 'Compression, scrape interval, label length, churn, WAL, index, and block'
\echo 'retention differ. Use estimated series and samples as Prometheus sizing'
\echo 'inputs, then validate them with promtool against representative data.'
