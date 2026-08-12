// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const databaseStatusQuery = `
select
	i.inst_id,
	i.instance_name,
	i.status,
	i.database_status,
	i.startup_time,
	d.open_mode,
	d.database_role,
	d.cdb,
	to_number(sys_context('USERENV', 'CON_ID')),
	sys_context('USERENV', 'CON_NAME'),
	d.platform_name
from gv$instance i
join gv$database d on d.inst_id = i.inst_id`

const instanceOperationalQuery = `
with sessions as (
	select
		inst_id,
		sum(case when type = 'USER' then 1 else 0 end) as user_sessions,
		sum(case when type = 'USER' and status = 'ACTIVE' then 1 else 0 end) as active_user_sessions,
		sum(case when type <> 'USER' then 1 else 0 end) as background_sessions
	from gv$session
	group by inst_id
),
processes as (
	select inst_id, count(*) as process_count
	from gv$process
	group by inst_id
),
parameters as (
	select
		inst_id,
		max(case when name = 'cpu_count' then to_number(value) end) as cpu_count,
		max(case when name = 'sga_max_size' then to_number(value) end) as sga_max_size,
		max(case when name = 'pga_aggregate_limit' then to_number(value) end) as pga_aggregate_limit
	from gv$parameter
	where name in ('cpu_count', 'sga_max_size', 'pga_aggregate_limit')
	group by inst_id
)
select
	i.inst_id,
	nvl(s.user_sessions, 0),
	nvl(s.active_user_sessions, 0),
	nvl(s.background_sessions, 0),
	nvl(p.process_count, 0),
	r.cpu_count,
	r.sga_max_size,
	r.pga_aggregate_limit
from gv$instance i
left join sessions s on s.inst_id = i.inst_id
left join processes p on p.inst_id = i.inst_id
left join parameters r on r.inst_id = i.inst_id`

const resourceLimitOperationalQuery = `
select
	inst_id,
	resource_name,
	current_utilization,
	max_utilization,
	case when regexp_like(trim(initial_allocation), '^[0-9]+$') then to_number(trim(initial_allocation)) end,
	case when regexp_like(trim(limit_value), '^[0-9]+$') then to_number(trim(limit_value)) end,
	case when trim(limit_value) = 'UNLIMITED' then 1 else 0 end
from gv$resource_limit`

const tablespaceOperationalQuery = `
select
	dt.tablespace_name,
	dt.contents,
	dt.block_size * dtum.used_space,
	dt.block_size * (dtum.tablespace_size - dtum.used_space),
	dt.block_size * dtum.tablespace_size,
	dtum.used_percent
from dba_tablespace_usage_metrics dtum
join dba_tablespaces dt on dt.tablespace_name = dtum.tablespace_name
where dt.contents <> 'TEMPORARY'
union all
select
	tablespace_name,
	'TEMPORARY',
	tablespace_size - free_space,
	free_space,
	tablespace_size,
	case when tablespace_size = 0 then 0
		else ((tablespace_size - free_space) / tablespace_size) * 100
	end
from dba_temp_free_space`

const asmDiskgroupOperationalQuery = `
select
	inst_id,
	name,
	total_mb * 1024 * 1024,
	free_mb * 1024 * 1024,
	usable_file_mb * 1024 * 1024
from gv$asm_diskgroup_stat
where exists (select 1 from gv$datafile where name like '+%')
	and inst_id = (select max(inst_id) from gv$instance)`

const systemCounterOperationalQuery = `
select inst_id, con_id, name, value
from gv$sysstat
where name in (
	'parse count (total)',
	'execute count',
	'user commits',
	'user rollbacks',
	'physical reads',
	'physical writes',
	'session logical reads',
	'redo size'
)`

const waitClassOperationalQuery = `
select inst_id, con_id, wait_class, round(time_waited * 10000)
from gv$system_wait_class
where wait_class <> 'Idle'`

const systemMetricOperationalQuery = `
select inst_id, con_id, metric_name, value, metric_unit
from gv$con_sysmetric
where metric_name in (
	'Buffer Cache Hit Ratio',
	'Cursor Cache Hit Ratio',
	'Host CPU Utilization (%)',
	'Database CPU Time Ratio',
	'Database Wait Time Ratio'
)`

type operationalCounterKey struct {
	database string
	kind     string
	instID   int64
	conID    int64
	name     string
}

type operationalCounterSnapshot struct {
	collectedAt time.Time
	value       int64
}

func (e *Scraper) shouldCollectOperational(database string, collectedAt time.Time) bool {
	if e.MetricsConfiguration == nil || !e.Operational.GetEnabled() {
		return false
	}
	e.operationalMu.Lock()
	defer e.operationalMu.Unlock()
	if e.lastOperationalScrape == nil {
		e.lastOperationalScrape = map[string]time.Time{}
	}
	last, ok := e.lastOperationalScrape[database]
	if ok && collectedAt.Before(last.Add(e.Operational.GetInterval())) {
		return false
	}
	e.lastOperationalScrape[database] = collectedAt
	return true
}

func (e *Scraper) operationalDelta(key operationalCounterKey, collectedAt time.Time, value int64) (*int64, *float64, bool) {
	e.operationalMu.Lock()
	defer e.operationalMu.Unlock()
	if e.operationalCounters == nil {
		e.operationalCounters = map[operationalCounterKey]operationalCounterSnapshot{}
	}
	previous, ok := e.operationalCounters[key]
	e.operationalCounters[key] = operationalCounterSnapshot{collectedAt: collectedAt, value: value}
	if !ok || !collectedAt.After(previous.collectedAt) {
		return nil, nil, false
	}
	interval := collectedAt.Sub(previous.collectedAt).Seconds()
	if value < previous.value {
		return nil, &interval, true
	}
	delta := value - previous.value
	return &delta, &interval, false
}

func (e *Scraper) ScrapeOperationalSamples(d *Database, collectedAt time.Time) (OperationalSamples, []ScrapeStatusSample, error) {
	type collection struct {
		name    string
		collect func(context.Context) (int, error)
	}

	var samples OperationalSamples
	timeout := e.Operational.GetQueryTimeout()
	collections := []collection{
		{name: "database_status", collect: func(ctx context.Context) (int, error) {
			return e.scrapeDatabaseStatus(ctx, d, collectedAt, &samples)
		}},
		{name: "instance", collect: func(ctx context.Context) (int, error) {
			return e.scrapeInstances(ctx, d, collectedAt, &samples)
		}},
		{name: "resource_limits", collect: func(ctx context.Context) (int, error) {
			return e.scrapeResourceLimits(ctx, d, collectedAt, &samples)
		}},
		{name: "tablespaces", collect: func(ctx context.Context) (int, error) {
			return e.scrapeTablespaces(ctx, d, collectedAt, &samples)
		}},
		{name: "asm_diskgroups", collect: func(ctx context.Context) (int, error) {
			return e.scrapeASMDiskgroups(ctx, d, collectedAt, &samples)
		}},
		{name: "system_counters", collect: func(ctx context.Context) (int, error) {
			return e.scrapeSystemCounters(ctx, d, collectedAt, &samples)
		}},
		{name: "wait_classes", collect: func(ctx context.Context) (int, error) {
			return e.scrapeWaitClasses(ctx, d, collectedAt, &samples)
		}},
		{name: "system_metrics", collect: func(ctx context.Context) (int, error) {
			return e.scrapeSystemMetrics(ctx, d, collectedAt, &samples)
		}},
	}

	statuses := make([]ScrapeStatusSample, 0, len(collections)+1)
	var joined error
	totalStarted := time.Now()
	for _, item := range collections {
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		count, err := item.collect(ctx)
		cancel()
		statuses = append(statuses, newScrapeStatus(collectedAt, d.Name, "operational."+item.name, started, count, err))
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("%s: %w", item.name, err))
		}
	}
	statuses = append(statuses, newScrapeStatus(collectedAt, d.Name, "operational", totalStarted, samples.Count(), joined))
	return samples, statuses, joined
}

func newScrapeStatus(collectedAt time.Time, database, collector string, started time.Time, count int, err error) ScrapeStatusSample {
	status := ScrapeStatusSample{
		CollectedAt:     collectedAt,
		Database:        database,
		Collector:       collector,
		Success:         err == nil,
		DurationSeconds: time.Since(started).Seconds(),
		SampleCount:     count,
	}
	if err != nil {
		message := err.Error()
		const maxErrorLength = 2048
		if len(message) > maxErrorLength {
			message = message[:maxErrorLength]
		}
		status.ErrorMessage = &message
	}
	return status
}

func queryOperationalRows(ctx context.Context, d *Database, query string, scan func(*sql.Rows) error) error {
	rows, unlock, err := d.QueryContext(ctx, query)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("Oracle query timed out: %w", ctx.Err())
		}
		return err
	}
	defer unlock()
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (e *Scraper) scrapeDatabaseStatus(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.DatabaseStatus)
	err := queryOperationalRows(ctx, d, databaseStatusQuery, func(rows *sql.Rows) error {
		var sample DatabaseStatusSample
		if err := rows.Scan(
			&sample.InstID, &sample.InstanceName, &sample.InstanceStatus, &sample.DatabaseStatus,
			&sample.StartupTime, &sample.OpenMode, &sample.DatabaseRole, &sample.CDB,
			&sample.ConID, &sample.ConName, &sample.PlatformName,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		out.DatabaseStatus = append(out.DatabaseStatus, sample)
		return nil
	})
	return len(out.DatabaseStatus) - before, err
}

func (e *Scraper) scrapeInstances(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.Instances)
	err := queryOperationalRows(ctx, d, instanceOperationalQuery, func(rows *sql.Rows) error {
		var sample InstanceSample
		var cpu, sga, pga sql.NullInt64
		if err := rows.Scan(
			&sample.InstID, &sample.UserSessions, &sample.ActiveUserSessions,
			&sample.BackgroundSessions, &sample.ProcessCount, &cpu, &sga, &pga,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		sample.CPUCount, sample.SGAMaxBytes, sample.PGAAggregateLimit = int64Ptr(cpu), int64Ptr(sga), int64Ptr(pga)
		out.Instances = append(out.Instances, sample)
		return nil
	})
	return len(out.Instances) - before, err
}

func (e *Scraper) scrapeResourceLimits(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.ResourceLimits)
	err := queryOperationalRows(ctx, d, resourceLimitOperationalQuery, func(rows *sql.Rows) error {
		var sample ResourceLimitSample
		var initial, limit sql.NullInt64
		var unlimited int64
		if err := rows.Scan(
			&sample.InstID, &sample.ResourceName, &sample.CurrentValue, &sample.MaxValue,
			&initial, &limit, &unlimited,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		sample.InitialLimit, sample.LimitValue, sample.LimitUnlimited = int64Ptr(initial), int64Ptr(limit), unlimited == 1
		out.ResourceLimits = append(out.ResourceLimits, sample)
		return nil
	})
	return len(out.ResourceLimits) - before, err
}

func (e *Scraper) scrapeTablespaces(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.Tablespaces)
	err := queryOperationalRows(ctx, d, tablespaceOperationalQuery, func(rows *sql.Rows) error {
		var sample TablespaceSample
		if err := rows.Scan(
			&sample.Tablespace, &sample.Contents, &sample.UsedBytes, &sample.FreeBytes,
			&sample.MaxBytes, &sample.UsedPercent,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		out.Tablespaces = append(out.Tablespaces, sample)
		return nil
	})
	return len(out.Tablespaces) - before, err
}

func (e *Scraper) scrapeASMDiskgroups(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.ASMDiskgroups)
	err := queryOperationalRows(ctx, d, asmDiskgroupOperationalQuery, func(rows *sql.Rows) error {
		var sample ASMDiskgroupSample
		var usable sql.NullInt64
		if err := rows.Scan(&sample.InstID, &sample.Name, &sample.TotalBytes, &sample.FreeBytes, &usable); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database, sample.UsableBytes = collectedAt, d.Name, int64Ptr(usable)
		out.ASMDiskgroups = append(out.ASMDiskgroups, sample)
		return nil
	})
	return len(out.ASMDiskgroups) - before, err
}

func (e *Scraper) scrapeSystemCounters(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.SystemCounters)
	err := queryOperationalRows(ctx, d, systemCounterOperationalQuery, func(rows *sql.Rows) error {
		var sample SystemCounterSample
		if err := rows.Scan(&sample.InstID, &sample.ConID, &sample.StatName, &sample.CumulativeValue); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		key := operationalCounterKey{
			database: d.Name, kind: "system", instID: sample.InstID, conID: sample.ConID, name: sample.StatName,
		}
		sample.DeltaValue, sample.IntervalSeconds, sample.CounterReset = e.operationalDelta(key, collectedAt, sample.CumulativeValue)
		out.SystemCounters = append(out.SystemCounters, sample)
		return nil
	})
	return len(out.SystemCounters) - before, err
}

func (e *Scraper) scrapeWaitClasses(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.WaitClasses)
	err := queryOperationalRows(ctx, d, waitClassOperationalQuery, func(rows *sql.Rows) error {
		var sample WaitClassSample
		if err := rows.Scan(&sample.InstID, &sample.ConID, &sample.WaitClass, &sample.CumulativeWaitMicro); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		key := operationalCounterKey{
			database: d.Name, kind: "wait", instID: sample.InstID, conID: sample.ConID, name: sample.WaitClass,
		}
		sample.DeltaWaitMicro, sample.IntervalSeconds, sample.CounterReset =
			e.operationalDelta(key, collectedAt, sample.CumulativeWaitMicro)
		out.WaitClasses = append(out.WaitClasses, sample)
		return nil
	})
	return len(out.WaitClasses) - before, err
}

func (e *Scraper) scrapeSystemMetrics(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	before := len(out.SystemMetrics)
	err := queryOperationalRows(ctx, d, systemMetricOperationalQuery, func(rows *sql.Rows) error {
		var sample SystemMetricSample
		var unit sql.NullString
		if err := rows.Scan(&sample.InstID, &sample.ConID, &sample.MetricName, &sample.Value, &unit); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database, sample.Unit = collectedAt, d.Name, stringPtr(unit)
		out.SystemMetrics = append(out.SystemMetrics, sample)
		return nil
	})
	return len(out.SystemMetrics) - before, err
}
