-- Estimate the Prometheus cardinality required to retain Harry's PostgreSQL
-- data model with equivalent troubleshooting dimensions.
--
-- Run with psql against the Harry database:
--
--   psql -X -v lookback='24 hours' \
--     -v prometheus_scrape_interval='15 seconds' \
--     -f doc/prometheus_cardinality_analysis.sql \
--     harry_monitoring
--
-- Run it for three windows when preparing a capacity comparison:
--
--   2 hours   - approximate recent/Head-series pressure
--   24 hours  - show daily label churn
--   N days    - use the proposed Prometheus retention period to show all
--               unique series and samples that retention must accommodate
--
-- The report models the series needed to preserve Harry's current queryable
-- troubleshooting dimensions, including SQL_ID, SQL_FULLTEXT, child cursors,
-- PLAN_HASH_VALUE, and complete cached plan operations. It is a lower bound
-- for the real Prometheus deployment because it still does not add environment,
-- region, cluster, job, exporter instance, HA replica, or recording-rule
-- labels. "Estimated series" is the number of distinct label sets multiplied
-- by the scalar/info metrics required to represent the PostgreSQL row.
--
-- SQL text and plan lookup rows are current-state entities, not periodic fact
-- rows. A Prometheus target must expose them repeatedly while they exist, so
-- their sample estimate uses prometheus_scrape_interval rather than counting
-- each lookup row only once.
--
-- This is an analytical query over retained data. Start with a short lookback
-- on large installations and run it on a PostgreSQL reader when available.
-- A hot standby can cancel this workload because of WAL replay conflicts; use
-- an analytics-capable replica or an off-peak primary when necessary.

\if :{?lookback}
\else
\set lookback '24 hours'
\endif

\if :{?prometheus_scrape_interval}
\else
\set prometheus_scrape_interval '15 seconds'
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
       2,
       count(distinct row(source_database, inst_id, con_id, con_name,
                          instance_name, instance_status, database_status,
                          open_mode, database_role, cdb, platform_name)) * 2,
       count(*) * 2,
       'Database/instance state info plus startup-time gauge.'
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
       'Success, duration, and sample count; data age is derived.'
from oracle_scrape_status
where collected_at >= now() - :'lookback'::interval;

insert into harry_prometheus_cardinality
select 'collector health', 'oracle_scrape_status_error_info', count(*),
       count(distinct row(source_database, collector, error_message)), 1,
       count(distinct row(source_database, collector, error_message)),
       count(*),
       'Separate info series required to preserve non-empty collector error text without multiplying the numeric status series.'
from oracle_scrape_status
where collected_at >= now() - :'lookback'::interval
  and error_message is not null
  and error_message <> '';

-- SQL statistics are the first high-cardinality area. SQL_ID and
-- PLAN_HASH_VALUE are mandatory labels for Harry-equivalent drill-down.
-- Mutable module, schema, child cursor, and plan labels create new series over
-- time. last_active_time requires an additional timestamp gauge.
insert into harry_prometheus_cardinality
select 'SQL performance', 'oracle_sql_samples', count(*),
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, parsing_schema_name, module)), 16,
       count(distinct row(source_database, inst_id, sql_id, child_number,
                          plan_hash_value, parsing_schema_name, module)) * 16,
       count(*) * 16,
       'SQL_ID/child/PLAN_HASH labels, fifteen workload counters, and last-active timestamp; SQL text is counted separately.'
from oracle_sql_samples
where collected_at >= now() - :'lookback'::interval;

-- Session state requires an info series plus numeric wait and logon-time
-- gauges to retain the fields used by Harry's session troubleshooting views.
insert into harry_prometheus_cardinality
select 'sessions and blocking', 'oracle_session_samples', count(*),
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          status, sql_id, sql_child_number, prev_sql_id, event,
                          wait_class, state, blocking_instance, blocking_session,
                          machine, program, module, action, service_name)), 3,
       count(distinct row(source_database, inst_id, sid, serial_number, username,
                          status, sql_id, sql_child_number, prev_sql_id, event,
                          wait_class, state, blocking_instance, blocking_session,
                          machine, program, module, action, service_name)) * 3,
       count(*) * 3,
       'Session info, seconds-in-wait, and logon-time series; label changes create new series.'
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
       'One info series retains each observed waiter/blocker relationship.'
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
           sql_exec_start, top_level_sql_id, session_state, event, wait_class,
           blocking_session, blocking_session_serial_number, blocking_inst_id,
           current_object_id, current_file_number, current_block_number,
           program, module, action, machine, con_id, sample_source,
           sql_plan_hash_value, sql_full_plan_hash_value, sql_plan_line_id,
           service_hash, service_name, client_identifier)), 5,
       count(distinct row(
           source_database, inst_id, session_id, session_serial_number,
           session_type, user_id, sql_id, sql_child_number, sql_exec_id,
           sql_exec_start, top_level_sql_id, session_state, event, wait_class,
           blocking_session, blocking_session_serial_number, blocking_inst_id,
           current_object_id, current_file_number, current_block_number,
           program, module, action, machine, con_id, sample_source,
           sql_plan_hash_value, sql_full_plan_hash_value, sql_plan_line_id,
           service_hash, service_name, client_identifier)) * 5,
       count(*) * 5,
       'Wait time, time waited, PGA, TEMP, and sample duration; SQL execution start is retained as an identity label.'
from oracle_database_activity_samples
where sample_time >= now() - :'lookback'::interval;

-- Current-state detail tables are not conventional time series. Prometheus
-- must repeatedly expose them while present. SQL_FULLTEXT requires one info
-- series with the complete text as a label plus three timestamp gauges.
insert into harry_prometheus_cardinality
select 'SQL detail', 'oracle_sql_texts', count(*),
       count(*), 4,
       count(*) * 4,
       coalesce(sum(
           greatest(
               ceil(extract(epoch from
                   (now() - greatest(first_seen_at, now() - :'lookback'::interval))) /
                   extract(epoch from :'prometheus_scrape_interval'::interval)),
               1
           ) * 4
       ), 0),
       format('One SQL_FULLTEXT info series plus first-seen, last-text-seen, and last-reference gauges; %s raw SQL text bytes.',
              coalesce(sum(octet_length(sql_fulltext)), 0))
from oracle_sql_texts
where last_referenced_at >= now() - :'lookback'::interval;

-- Every plan operation needs an info series even when all optimizer estimates
-- are NULL. Structural fields and predicates are labels on that info series;
-- six optimizer estimates and three lifecycle timestamps are numeric gauges.
insert into harry_prometheus_cardinality
select 'SQL detail', 'oracle_sql_plans', count(*),
       count(*), 10,
       count(*) * 4 + count(cost) + count(cardinality) + count(bytes) +
           count(cpu_cost) + count(io_cost) + count(temp_space),
       coalesce(sum(
           greatest(
               ceil(extract(epoch from
                   (now() - greatest(first_seen_at, now() - :'lookback'::interval))) /
                   extract(epoch from :'prometheus_scrape_interval'::interval)),
               1
           ) * (4 +
               case when cost is not null then 1 else 0 end +
               case when cardinality is not null then 1 else 0 end +
               case when bytes is not null then 1 else 0 end +
               case when cpu_cost is not null then 1 else 0 end +
               case when io_cost is not null then 1 else 0 end +
               case when temp_space is not null then 1 else 0 end)
       ), 0),
       'One complete plan-operation info series, three lifecycle timestamp gauges, and only the non-NULL optimizer-estimate gauges (ten maximum).'
from oracle_sql_plans
where last_referenced_at >= now() - :'lookback'::interval;

-- Materialize the database-qualified SQL_ID working set once. This combines
-- every place where Harry can currently use a SQL_ID for navigation or
-- troubleshooting, not only the frequent GV$SQLSTATS samples.
drop table if exists pg_temp.harry_prometheus_sql_ids;
create temporary table harry_prometheus_sql_ids as
select source_database, sql_id
from oracle_sql_samples
where collected_at >= now() - :'lookback'::interval
  and sql_id is not null
union
select source_database, sql_id
from oracle_database_activity_samples
where sample_time >= now() - :'lookback'::interval
  and sql_id is not null
union
select source_database, top_level_sql_id
from oracle_database_activity_samples
where sample_time >= now() - :'lookback'::interval
  and top_level_sql_id is not null
union
select source_database, sql_id
from oracle_session_samples
where collected_at >= now() - :'lookback'::interval
  and sql_id is not null
union
select source_database, prev_sql_id
from oracle_session_samples
where collected_at >= now() - :'lookback'::interval
  and prev_sql_id is not null
union
select source_database, sql_id
from oracle_blocking_session_samples
where collected_at >= now() - :'lookback'::interval
  and sql_id is not null
union
select source_database, blocking_sql_id
from oracle_blocking_session_samples
where collected_at >= now() - :'lookback'::interval
  and blocking_sql_id is not null
union
select source_database, sql_id
from oracle_sql_texts
where last_referenced_at >= now() - :'lookback'::interval
union
select source_database, sql_id
from oracle_sql_plans
where last_referenced_at >= now() - :'lookback'::interval;

create index on harry_prometheus_sql_ids (source_database, sql_id);

drop table if exists pg_temp.harry_prometheus_plan_hashes;
create temporary table harry_prometheus_plan_hashes as
select source_database, sql_id, plan_hash_value
from oracle_sql_samples
where collected_at >= now() - :'lookback'::interval
  and sql_id is not null
  and plan_hash_value is not null
  and plan_hash_value <> 0
union
select source_database, sql_id, sql_plan_hash_value
from oracle_database_activity_samples
where sample_time >= now() - :'lookback'::interval
  and sql_id is not null
  and sql_plan_hash_value is not null
  and sql_plan_hash_value <> 0
union
select source_database, sql_id, plan_hash_value
from oracle_sql_plans
where last_referenced_at >= now() - :'lookback'::interval
  and plan_hash_value <> 0;

create index on harry_prometheus_plan_hashes
    (source_database, sql_id, plan_hash_value);

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
    :'prometheus_scrape_interval' as prometheus_scrape_interval,
    sum(postgres_rows) as postgres_rows,
    sum(distinct_label_sets) as distinct_label_sets,
    sum(estimated_prometheus_series) as estimated_prometheus_series,
    sum(estimated_prometheus_samples) as estimated_prometheus_samples,
    round(sum(estimated_prometheus_samples) /
          extract(epoch from :'lookback'::interval), 2) as equivalent_samples_per_second
from harry_prometheus_cardinality;

\echo
\echo 'SQL_ID working set and detail coverage for lookback' :lookback
with text_ids as (
    select distinct source_database, sql_id
    from oracle_sql_texts
    where last_referenced_at >= now() - :'lookback'::interval
), plan_ids as (
    select distinct source_database, sql_id
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
)
select
    count(*) as database_qualified_sql_ids,
    count(distinct ids.sql_id) as globally_distinct_sql_id_values,
    count(*) filter (where texts.sql_id is not null) as sql_ids_with_fulltext,
    count(*) filter (where texts.sql_id is null) as sql_ids_without_fulltext,
    round(count(*) filter (where texts.sql_id is not null) * 100.0 /
          nullif(count(*), 0), 2) as fulltext_coverage_pct,
    count(*) filter (where plans.sql_id is not null) as sql_ids_with_plan,
    count(*) filter (where plans.sql_id is null) as sql_ids_without_plan,
    round(count(*) filter (where plans.sql_id is not null) * 100.0 /
          nullif(count(*), 0), 2) as plan_coverage_pct
from harry_prometheus_sql_ids ids
left join text_ids texts using (source_database, sql_id)
left join plan_ids plans using (source_database, sql_id);

\echo
\echo 'SQL_FULLTEXT Prometheus label cost for lookback' :lookback
select
    count(*) as sql_text_info_label_sets,
    count(distinct sql_fulltext) as distinct_fulltext_values,
    coalesce(sum(octet_length(sql_fulltext)), 0) as raw_fulltext_bytes,
    round(coalesce(avg(octet_length(sql_fulltext)), 0), 2) as avg_fulltext_bytes,
    round(coalesce(percentile_cont(0.95) within group
                   (order by octet_length(sql_fulltext)), 0)::numeric, 2)
        as p95_fulltext_bytes,
    coalesce(max(octet_length(sql_fulltext)), 0) as max_fulltext_bytes,
    count(*) * 4 as normalized_text_series,
    coalesce(sum(
        greatest(
            ceil(extract(epoch from
                (now() - greatest(first_seen_at, now() - :'lookback'::interval))) /
                extract(epoch from :'prometheus_scrape_interval'::interval)),
            1
        )
    ), 0) as info_series_samples,
    coalesce(sum(
        greatest(
            ceil(extract(epoch from
                (now() - greatest(first_seen_at, now() - :'lookback'::interval))) /
                extract(epoch from :'prometheus_scrape_interval'::interval)),
            1
        ) * octet_length(sql_fulltext)
    ), 0) as raw_fulltext_label_byte_emissions
from oracle_sql_texts
where last_referenced_at >= now() - :'lookback'::interval;

\echo
\echo 'Cost of attaching SQL_FULLTEXT to every SQL workload metric series'
with workload_label_sets as (
    select distinct source_database, inst_id, sql_id, child_number,
                    plan_hash_value, parsing_schema_name, module
    from oracle_sql_samples
    where collected_at >= now() - :'lookback'::interval
), workload_with_text as (
    select sets.*, octet_length(texts.sql_fulltext) as text_bytes
    from workload_label_sets sets
    left join oracle_sql_texts texts using (source_database, sql_id)
    where texts.last_referenced_at >= now() - :'lookback'::interval
), normalized_text as (
    select sum(octet_length(texts.sql_fulltext))::numeric as bytes
    from oracle_sql_texts texts
    where texts.last_referenced_at >= now() - :'lookback'::interval
      and exists (
          select 1
          from workload_label_sets sets
          where sets.source_database = texts.source_database
            and sets.sql_id = texts.sql_id
      )
)
select
    count(*) as workload_label_sets_with_text,
    count(*) * 16 as workload_series_with_text,
    coalesce(sum(text_bytes), 0) * 16 as duplicated_fulltext_logical_bytes,
    coalesce(max(normalized_text.bytes), 0) as normalized_info_logical_bytes,
    round((coalesce(sum(text_bytes), 0) * 16)::numeric /
          nullif(max(normalized_text.bytes), 0), 2) as logical_byte_duplication_factor
from workload_with_text
cross join normalized_text;

\echo
\echo 'PLAN_HASH_VALUE and cached execution-plan fan-out'
with cached_plan_hashes as (
    select distinct source_database, sql_id, plan_hash_value
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
      and plan_hash_value <> 0
), plan_hash_coverage as (
    select observed.*,
           cached.plan_hash_value is not null as detail_cached
    from harry_prometheus_plan_hashes observed
    left join cached_plan_hashes cached
      using (source_database, sql_id, plan_hash_value)
), plan_cursors as (
    select source_database, inst_id, sql_id, child_number, plan_hash_value,
           count(*) as plan_operations,
           count(*) * 4 + count(cost) + count(cardinality) + count(bytes) +
               count(cpu_cost) + count(io_cost) + count(temp_space)
               as equivalent_series
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
    group by source_database, inst_id, sql_id, child_number, plan_hash_value
), per_sql as (
    select source_database, sql_id,
           count(distinct plan_hash_value) as plan_hashes
    from harry_prometheus_plan_hashes
    group by source_database, sql_id
), plan_payload as (
    select
        coalesce(sum(label_bytes), 0) as raw_label_bytes,
        coalesce(sum(
            greatest(
                ceil(extract(epoch from
                    (now() - greatest(first_seen_at,
                                      now() - :'lookback'::interval))) /
                    extract(epoch from
                            :'prometheus_scrape_interval'::interval)),
                1
            ) * label_bytes
        ), 0) as raw_label_byte_emissions
    from (
        select first_seen_at,
               octet_length(concat_ws(chr(31),
                   parent_id::text, depth::text, position::text,
                   operation, options, object_owner, object_name, object_type,
                   optimizer, partition_start, partition_stop,
                   access_predicates, filter_predicates)) as label_bytes
        from oracle_sql_plans
        where last_referenced_at >= now() - :'lookback'::interval
    ) payload
)
select
    (select count(*) from plan_hash_coverage)
        as observed_database_sql_plan_hash_identities,
    (select count(distinct plan_hash_value) from plan_hash_coverage)
        as globally_distinct_plan_hash_values,
    (select count(*) from plan_hash_coverage where detail_cached)
        as observed_plan_hashes_with_cached_detail,
    (select count(*) from plan_hash_coverage where not detail_cached)
        as observed_plan_hashes_without_cached_detail,
    (select count(distinct row(source_database, inst_id, sql_id, child_number,
                               plan_hash_value))
     from oracle_sql_samples
     where collected_at >= now() - :'lookback'::interval
       and sql_id is not null
       and plan_hash_value is not null
       and plan_hash_value <> 0) as sql_sample_child_plan_identities,
    count(*) as child_cursor_plan_identities,
    coalesce(sum(plan_operations), 0) as plan_operation_rows,
    coalesce(sum(equivalent_series), 0) as equivalent_plan_series,
    round(coalesce(avg(plan_operations), 0), 2) as avg_operations_per_child_plan,
    round(coalesce(percentile_cont(0.95) within group
                   (order by plan_operations), 0)::numeric, 2)
        as p95_operations_per_child_plan,
    coalesce(max(plan_operations), 0) as max_operations_per_child_plan,
    (select raw_label_bytes from plan_payload)
        as raw_plan_detail_label_bytes,
    (select raw_label_byte_emissions from plan_payload)
        as raw_plan_detail_label_byte_emissions,
    (select count(*) from per_sql where plan_hashes > 1)
        as sql_ids_with_multiple_plan_hashes,
    coalesce((select max(plan_hashes) from per_sql), 0)
        as max_plan_hashes_for_one_database_sql_id
from plan_cursors;

-- Calculate the number of current Harry-equivalent series attributable to
-- each database-qualified SQL_ID. This includes workload, activity, session,
-- blocking, full-text, and execution-plan series.
drop table if exists pg_temp.harry_prometheus_sql_fanout;
create temporary table harry_prometheus_sql_fanout as
with workload as (
    select source_database, sql_id,
           count(distinct row(inst_id, child_number, plan_hash_value,
                              parsing_schema_name, module)) * 16 as series
    from oracle_sql_samples
    where collected_at >= now() - :'lookback'::interval
      and sql_id is not null
    group by source_database, sql_id
), activity as (
    select source_database, sql_id,
           count(distinct row(
               inst_id, session_id, session_serial_number, session_type,
               user_id, sql_child_number, sql_exec_id, sql_exec_start,
               top_level_sql_id, session_state, event,
               wait_class, blocking_session, blocking_session_serial_number,
               blocking_inst_id, current_object_id, current_file_number,
               current_block_number, program, module, action, machine, con_id,
               sample_source, sql_plan_hash_value, sql_full_plan_hash_value,
               sql_plan_line_id, service_hash, service_name,
               client_identifier)) * 5 as series
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
      and sql_id is not null
    group by source_database, sql_id
), sessions as (
    select source_database, sql_id,
           count(distinct row(inst_id, sid, serial_number, username, status,
                              sql_child_number, prev_sql_id, event, wait_class,
                              state, blocking_instance, blocking_session,
                              machine, program, module, action,
                              service_name)) * 3 as series
    from oracle_session_samples
    where collected_at >= now() - :'lookback'::interval
      and sql_id is not null
    group by source_database, sql_id
), blocking as (
    select source_database, sql_id,
           count(distinct row(inst_id, sid, serial_number, username, event,
                              wait_class, blocking_instance, blocking_session,
                              blocking_username, blocking_sql_id,
                              blocking_event)) as series
    from oracle_blocking_session_samples
    where collected_at >= now() - :'lookback'::interval
      and sql_id is not null
    group by source_database, sql_id
), texts as (
    select source_database, sql_id, 4::bigint as series
    from oracle_sql_texts
    where last_referenced_at >= now() - :'lookback'::interval
), plans as (
    select source_database, sql_id,
           count(*) * 4 + count(cost) + count(cardinality) + count(bytes) +
               count(cpu_cost) + count(io_cost) + count(temp_space) as series
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
    group by source_database, sql_id
)
select ids.source_database, ids.sql_id,
       coalesce(workload.series, 0)::bigint as workload_series,
       coalesce(activity.series, 0)::bigint as activity_series,
       coalesce(sessions.series, 0)::bigint as session_series,
       coalesce(blocking.series, 0)::bigint as blocking_series,
       coalesce(texts.series, 0)::bigint as sql_text_series,
       coalesce(plans.series, 0)::bigint as plan_series,
       (coalesce(workload.series, 0) + coalesce(activity.series, 0) +
        coalesce(sessions.series, 0) + coalesce(blocking.series, 0) +
        coalesce(texts.series, 0) + coalesce(plans.series, 0))::bigint
           as total_equivalent_series
from harry_prometheus_sql_ids ids
left join workload using (source_database, sql_id)
left join activity using (source_database, sql_id)
left join sessions using (source_database, sql_id)
left join blocking using (source_database, sql_id)
left join texts using (source_database, sql_id)
left join plans using (source_database, sql_id);

\echo
\echo 'Equivalent Prometheus series fan-out per database-qualified SQL_ID'
select
    count(*) as database_qualified_sql_ids,
    round(avg(total_equivalent_series), 2) as avg_series_per_sql_id,
    round(percentile_cont(0.50) within group
          (order by total_equivalent_series)::numeric, 2)
        as p50_series_per_sql_id,
    round(percentile_cont(0.95) within group
          (order by total_equivalent_series)::numeric, 2)
        as p95_series_per_sql_id,
    round(percentile_cont(0.99) within group
          (order by total_equivalent_series)::numeric, 2)
        as p99_series_per_sql_id,
    max(total_equivalent_series) as max_series_for_one_sql_id,
    sum(total_equivalent_series) as total_sql_attributable_series
from harry_prometheus_sql_fanout;

\echo
\echo 'Top 25 SQL_IDs by equivalent Prometheus series fan-out'
select source_database, sql_id, workload_series, activity_series,
       session_series, blocking_series, sql_text_series, plan_series,
       total_equivalent_series
from harry_prometheus_sql_fanout
order by total_equivalent_series desc, source_database, sql_id
limit 25;

\echo
\echo 'High-cardinality label values observed in the lookback'
select *
from (
    select 'Database-qualified SQL_IDs across all Harry datasets' as dimension,
           count(*) as distinct_values
    from harry_prometheus_sql_ids
    union all
    select 'Raw SQL_ID values across all Harry datasets',
           count(distinct sql_id)
    from harry_prometheus_sql_ids
    union all
    select 'SQL_IDs in SQL samples' as dimension,
           count(distinct row(source_database, sql_id)) as distinct_values
    from oracle_sql_samples
    where collected_at >= now() - :'lookback'::interval
    union all
    select 'SQL_IDs in activity history',
           count(distinct row(source_database, sql_id))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
    union all
    select 'SQL_FULLTEXT info label sets', count(*)
    from oracle_sql_texts
    where last_referenced_at >= now() - :'lookback'::interval
    union all
    select 'Distinct SQL_FULLTEXT values', count(distinct sql_fulltext)
    from oracle_sql_texts
    where last_referenced_at >= now() - :'lookback'::interval
    union all
    select 'Observed database SQL_ID + PLAN_HASH identities', count(*)
    from harry_prometheus_plan_hashes
    union all
    select 'Activity SQL_ID + FULL_PLAN_HASH identities',
           count(distinct row(source_database, sql_id,
                              sql_full_plan_hash_value))
    from oracle_database_activity_samples
    where sample_time >= now() - :'lookback'::interval
      and sql_id is not null
      and sql_full_plan_hash_value is not null
      and sql_full_plan_hash_value <> 0
    union all
    select 'Child cursor plan identities',
           count(distinct row(source_database, inst_id, sql_id, child_number,
                              plan_hash_value))
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
    union all
    select 'Cached plan operation identities', count(*)
    from oracle_sql_plans
    where last_referenced_at >= now() - :'lookback'::interval
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
      'oracle_latest_scrape_status',
      'harry_repository_daily_ingest'
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
\echo
\echo 'SQL-specific conclusions:'
\echo '- SQL_ID and PLAN_HASH_VALUE are required labels, not optional examples,'
\echo '  when Prometheus must preserve Harry dashboard drill-down.'
\echo '- SQL_FULLTEXT cannot be a Prometheus sample value. It must be a label on'
\echo '  an info series or duplicated on every SQL workload series.'
\echo '- A functionally dependent SQL_FULLTEXT label may not increase the number'
\echo '  of workload series by itself, but it greatly increases logical label'
\echo '  payload, indexing/query cost, and remote-write traffic.'
\echo '- Normalizing text into an info series reduces duplication but still adds'
\echo '  high-cardinality series and requires PromQL vector joins. Historical joins'
\echo '  become fragile when info-series and workload-series lifetimes differ.'
\echo '- Cached plan operations require text-bearing info series plus numeric'
\echo '  estimate series for every SQL_ID/child/PLAN_HASH/plan-line identity.'
\echo '- Totals remain a lower bound: deployment labels, HA replicas, recording'
\echo '  rules, alert expressions, and exporter self-metrics are not included.'
