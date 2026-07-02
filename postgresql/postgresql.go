// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dodger-one/oracledb-performance-scraper/collector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Sink struct {
	pool                  *pgxpool.Pool
	logger                *slog.Logger
	retention             time.Duration
	samplesTable          pgx.Identifier
	sqlSamplesTable       pgx.Identifier
	sessionSamplesTable   pgx.Identifier
	blockingSessionsTable pgx.Identifier
	databaseActivityTable pgx.Identifier
}

func New(ctx context.Context, logger *slog.Logger, cfg collector.PostgreSQLConfig) (*Sink, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("output.postgresql.url is required")
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse postgresql url: %w", err)
	}
	poolConfig.MaxConns = cfg.GetMaxConns()
	poolConfig.MinConns = cfg.GetMinConns()
	poolConfig.MaxConnLifetime = cfg.GetConnMaxLifetime()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgresql pool: %w", err)
	}

	s := &Sink{
		pool:                  pool,
		logger:                logger,
		retention:             cfg.GetRetention(),
		samplesTable:          identifier(cfg.SamplesTable, "oracle_metric_samples"),
		sqlSamplesTable:       identifier(cfg.SQLSamplesTable, "oracle_sql_samples"),
		sessionSamplesTable:   identifier(cfg.SessionSamplesTable, "oracle_session_samples"),
		blockingSessionsTable: identifier(cfg.BlockingSessionsTable, "oracle_blocking_session_samples"),
		databaseActivityTable: identifier(cfg.DatabaseActivityTable, "oracle_database_activity_samples"),
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgresql: %w", err)
	}
	if cfg.GetAutoMigrate() {
		if err := s.Migrate(ctx); err != nil {
			pool.Close()
			return nil, err
		}
	}
	s.cleanupRetention(ctx)
	return s, nil
}

func (s *Sink) Migrate(ctx context.Context) error {
	samples := s.samplesTable.Sanitize()
	sqlSamples := s.sqlSamplesTable.Sanitize()
	sessionSamples := s.sessionSamplesTable.Sanitize()
	blockingSessions := s.blockingSessionsTable.Sanitize()
	databaseActivity := s.databaseActivityTable.Sanitize()
	ddl := fmt.Sprintf(`
create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	context text not null,
	metric_name text not null,
	metric_help text not null,
	metric_type text not null,
	value double precision not null,
	labels jsonb not null
) partition by range (collected_at);

create index if not exists oracle_metric_samples_collected_at_idx on %s (collected_at);
create index if not exists oracle_metric_samples_database_context_idx on %s (source_database, context);
create index if not exists oracle_metric_samples_labels_gin_idx on %s using gin (labels);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	sql_id text not null,
	child_number bigint,
	plan_hash_value bigint,
	parsing_schema_name text,
	module text,
	executions bigint,
	elapsed_time_micro bigint,
	cpu_time_micro bigint,
	user_io_wait_micro bigint,
	application_wait_micro bigint,
	concurrency_wait_micro bigint,
	cluster_wait_micro bigint,
	buffer_gets bigint,
	disk_reads bigint,
	direct_writes bigint,
	rows_processed bigint,
	fetches bigint,
	loads bigint,
	invalidations bigint,
	parse_calls bigint,
	last_active_time timestamptz,
	sql_text text
) partition by range (collected_at);

create index if not exists oracle_sql_samples_collected_at_idx on %s (collected_at);
create index if not exists oracle_sql_samples_sql_id_idx on %s (source_database, sql_id, child_number);
create index if not exists oracle_sql_samples_elapsed_idx on %s (elapsed_time_micro desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	sid bigint not null,
	serial_number bigint,
	username text,
	status text,
	sql_id text,
	sql_child_number bigint,
	prev_sql_id text,
	event text,
	wait_class text,
	state text,
	seconds_in_wait bigint,
	blocking_instance bigint,
	blocking_session bigint,
	machine text,
	program text,
	module text,
	action text,
	service_name text,
	logon_time timestamptz
) partition by range (collected_at);

create index if not exists oracle_session_samples_collected_at_idx on %s (collected_at);
create index if not exists oracle_session_samples_sql_id_idx on %s (source_database, sql_id);
create index if not exists oracle_session_samples_wait_class_idx on %s (source_database, wait_class);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	sid bigint not null,
	serial_number bigint,
	username text,
	sql_id text,
	event text,
	wait_class text,
	blocking_instance bigint,
	blocking_session bigint,
	blocking_username text,
	blocking_sql_id text,
	blocking_event text
) partition by range (collected_at);

create index if not exists oracle_blocking_session_samples_collected_at_idx on %s (collected_at);
create index if not exists oracle_blocking_session_samples_sql_id_idx on %s (source_database, sql_id);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	sample_id bigint not null,
	sample_time timestamptz not null,
	inst_id bigint not null,
	session_id bigint not null,
	session_serial_number bigint,
	session_serial_number_key bigint generated always as (coalesce(session_serial_number, -1)) stored,
	session_type text,
	user_id bigint,
	sql_id text,
	sql_child_number bigint,
	sql_exec_id bigint,
	sql_exec_start timestamptz,
	top_level_sql_id text,
	session_state text,
	event text,
	wait_class text,
	wait_time_micro bigint,
	time_waited_micro bigint,
	blocking_session bigint,
	blocking_session_serial_number bigint,
	blocking_inst_id bigint,
	current_object_id bigint,
	current_file_number bigint,
	current_block_number bigint,
	program text,
	module text,
	action text,
	machine text,
	pga_allocated bigint,
	temp_space_allocated bigint,
	con_id bigint
) partition by range (sample_time);

create unique index if not exists oracle_database_activity_samples_unique_idx on %s (sample_time, source_database, inst_id, sample_id, session_id, session_serial_number_key);
create index if not exists oracle_database_activity_samples_sample_time_idx on %s (sample_time);
create index if not exists oracle_database_activity_samples_sql_id_idx on %s (source_database, sql_id, sample_time);
create index if not exists oracle_database_activity_samples_wait_class_idx on %s (source_database, wait_class, sample_time);
`, samples, samples, samples, samples,
		sqlSamples, sqlSamples, sqlSamples, sqlSamples,
		sessionSamples, sessionSamples, sessionSamples, sessionSamples,
		blockingSessions, blockingSessions, blockingSessions,
		databaseActivity, databaseActivity, databaseActivity, databaseActivity, databaseActivity)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("migrate postgresql schema: %w", err)
	}
	return nil
}

func (s *Sink) WriteSamples(ctx context.Context, samples []collector.MetricSample, performance collector.PerformanceSamples, summary collector.ScrapeSummary) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin postgresql transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.ensureWritePartitions(ctx, tx, samples, performance); err != nil {
		return err
	}

	if len(samples) > 0 {
		rows := make([][]any, 0, len(samples))
		for _, sample := range samples {
			labels, err := json.Marshal(sample.Labels)
			if err != nil {
				return fmt.Errorf("marshal labels for %s/%s: %w", sample.Context, sample.Name, err)
			}
			rows = append(rows, []any{
				sample.CollectedAt,
				sample.Database,
				sample.Context,
				sample.Name,
				sample.Help,
				sample.Type,
				sample.Value,
				string(labels),
			})
		}

		_, err = tx.CopyFrom(
			ctx,
			s.samplesTable,
			[]string{"collected_at", "source_database", "context", "metric_name", "metric_help", "metric_type", "value", "labels"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("copy metric samples: %w", err)
		}
	}

	if err := s.writeSQLSamples(ctx, tx, performance.SQL); err != nil {
		return err
	}
	if err := s.writeSessionSamples(ctx, tx, performance.Sessions); err != nil {
		return err
	}
	if err := s.writeBlockingSessionSamples(ctx, tx, performance.BlockingSessions); err != nil {
		return err
	}
	if err := s.writeDatabaseActivitySamples(ctx, tx, performance.DatabaseActivity); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit postgresql transaction: %w", err)
	}
	s.logger.Info("Wrote scrape samples to PostgreSQL",
		"samples", len(samples),
		"sql_samples", len(performance.SQL),
		"session_samples", len(performance.Sessions),
		"blocking_session_samples", len(performance.BlockingSessions),
		"database_activity_samples", len(performance.DatabaseActivity),
		"errors", summary.TotalErrors,
		"duration", summary.DurationSeconds)
	s.cleanupRetention(ctx)
	return nil
}

func (s *Sink) writeSQLSamples(ctx context.Context, tx pgx.Tx, samples []collector.SQLSample) error {
	if len(samples) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(samples))
	for _, sample := range samples {
		rows = append(rows, []any{
			sample.CollectedAt,
			sample.Database,
			sample.InstID,
			sample.SQLID,
			sample.ChildNumber,
			sample.PlanHashValue,
			sample.ParsingSchemaName,
			sample.Module,
			sample.Executions,
			sample.ElapsedTimeMicro,
			sample.CPUTimeMicro,
			sample.UserIOWaitMicro,
			sample.ApplicationWaitMicro,
			sample.ConcurrencyWaitMicro,
			sample.ClusterWaitMicro,
			sample.BufferGets,
			sample.DiskReads,
			sample.DirectWrites,
			sample.RowsProcessed,
			sample.Fetches,
			sample.Loads,
			sample.Invalidations,
			sample.ParseCalls,
			sample.LastActiveTime,
			sample.SQLText,
		})
	}

	if _, err := tx.CopyFrom(ctx, s.sqlSamplesTable, []string{
		"collected_at", "source_database", "inst_id", "sql_id", "child_number", "plan_hash_value",
		"parsing_schema_name", "module", "executions", "elapsed_time_micro", "cpu_time_micro", "user_io_wait_micro",
		"application_wait_micro", "concurrency_wait_micro", "cluster_wait_micro", "buffer_gets", "disk_reads",
		"direct_writes", "rows_processed", "fetches", "loads", "invalidations", "parse_calls", "last_active_time",
		"sql_text",
	}, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy sql samples: %w", err)
	}
	return nil
}

func (s *Sink) writeSessionSamples(ctx context.Context, tx pgx.Tx, samples []collector.SessionSample) error {
	if len(samples) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(samples))
	for _, sample := range samples {
		rows = append(rows, []any{
			sample.CollectedAt,
			sample.Database,
			sample.InstID,
			sample.SID,
			sample.SerialNumber,
			sample.Username,
			sample.Status,
			sample.SQLID,
			sample.SQLChildNumber,
			sample.PrevSQLID,
			sample.Event,
			sample.WaitClass,
			sample.State,
			sample.SecondsInWait,
			sample.BlockingInstance,
			sample.BlockingSession,
			sample.Machine,
			sample.Program,
			sample.Module,
			sample.Action,
			sample.ServiceName,
			sample.LogonTime,
		})
	}

	if _, err := tx.CopyFrom(ctx, s.sessionSamplesTable, []string{
		"collected_at", "source_database", "inst_id", "sid", "serial_number", "username", "status",
		"sql_id", "sql_child_number", "prev_sql_id", "event", "wait_class", "state", "seconds_in_wait",
		"blocking_instance", "blocking_session", "machine", "program", "module", "action", "service_name",
		"logon_time",
	}, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy session samples: %w", err)
	}
	return nil
}

func (s *Sink) writeBlockingSessionSamples(ctx context.Context, tx pgx.Tx, samples []collector.BlockingSessionSample) error {
	if len(samples) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(samples))
	for _, sample := range samples {
		rows = append(rows, []any{
			sample.CollectedAt,
			sample.Database,
			sample.InstID,
			sample.SID,
			sample.SerialNumber,
			sample.Username,
			sample.SQLID,
			sample.Event,
			sample.WaitClass,
			sample.BlockingInstance,
			sample.BlockingSession,
			sample.BlockingUsername,
			sample.BlockingSQLID,
			sample.BlockingEvent,
		})
	}

	if _, err := tx.CopyFrom(ctx, s.blockingSessionsTable, []string{
		"collected_at", "source_database", "inst_id", "sid", "serial_number", "username", "sql_id",
		"event", "wait_class", "blocking_instance", "blocking_session", "blocking_username", "blocking_sql_id",
		"blocking_event",
	}, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy blocking session samples: %w", err)
	}
	return nil
}

func (s *Sink) writeDatabaseActivitySamples(ctx context.Context, tx pgx.Tx, samples []collector.DatabaseActivitySample) error {
	if len(samples) == 0 {
		return nil
	}

	table := s.databaseActivityTable.Sanitize()
	insertSQL := fmt.Sprintf(`insert into %s (
		collected_at,
		source_database,
		sample_id,
		sample_time,
		inst_id,
		session_id,
		session_serial_number,
		session_type,
		user_id,
		sql_id,
		sql_child_number,
		sql_exec_id,
		sql_exec_start,
		top_level_sql_id,
		session_state,
		event,
		wait_class,
		wait_time_micro,
		time_waited_micro,
		blocking_session,
		blocking_session_serial_number,
		blocking_inst_id,
		current_object_id,
		current_file_number,
		current_block_number,
		program,
		module,
		action,
		machine,
		pga_allocated,
		temp_space_allocated,
		con_id
	) values (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
		$31, $32
	) on conflict do nothing`, table)

	batch := &pgx.Batch{}
	for _, sample := range samples {
		batch.Queue(insertSQL,
			sample.CollectedAt,
			sample.Database,
			sample.SampleID,
			sample.SampleTime,
			sample.InstID,
			sample.SessionID,
			sample.SessionSerialNumber,
			sample.SessionType,
			sample.UserID,
			sample.SQLID,
			sample.SQLChildNumber,
			sample.SQLExecID,
			sample.SQLExecStart,
			sample.TopLevelSQLID,
			sample.SessionState,
			sample.Event,
			sample.WaitClass,
			sample.WaitTimeMicro,
			sample.TimeWaitedMicro,
			sample.BlockingSession,
			sample.BlockingSessionSerial,
			sample.BlockingInstID,
			sample.CurrentObjectID,
			sample.CurrentFileNumber,
			sample.CurrentBlockNumber,
			sample.Program,
			sample.Module,
			sample.Action,
			sample.Machine,
			sample.PGAAllocated,
			sample.TempSpaceAllocated,
			sample.ConID,
		)
	}

	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range samples {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("insert database activity samples: %w", err)
		}
	}
	return nil
}

func (s *Sink) ensureWritePartitions(ctx context.Context, tx pgx.Tx, samples []collector.MetricSample, performance collector.PerformanceSamples) error {
	metricTimes := make([]time.Time, 0, len(samples))
	for _, sample := range samples {
		metricTimes = append(metricTimes, sample.CollectedAt)
	}
	if err := ensureDailyPartitions(ctx, tx, s.samplesTable, metricTimes); err != nil {
		return fmt.Errorf("ensure metric sample partitions: %w", err)
	}

	sqlTimes := make([]time.Time, 0, len(performance.SQL))
	for _, sample := range performance.SQL {
		sqlTimes = append(sqlTimes, sample.CollectedAt)
	}
	if err := ensureDailyPartitions(ctx, tx, s.sqlSamplesTable, sqlTimes); err != nil {
		return fmt.Errorf("ensure sql sample partitions: %w", err)
	}

	sessionTimes := make([]time.Time, 0, len(performance.Sessions))
	for _, sample := range performance.Sessions {
		sessionTimes = append(sessionTimes, sample.CollectedAt)
	}
	if err := ensureDailyPartitions(ctx, tx, s.sessionSamplesTable, sessionTimes); err != nil {
		return fmt.Errorf("ensure session sample partitions: %w", err)
	}

	blockingSessionTimes := make([]time.Time, 0, len(performance.BlockingSessions))
	for _, sample := range performance.BlockingSessions {
		blockingSessionTimes = append(blockingSessionTimes, sample.CollectedAt)
	}
	if err := ensureDailyPartitions(ctx, tx, s.blockingSessionsTable, blockingSessionTimes); err != nil {
		return fmt.Errorf("ensure blocking session sample partitions: %w", err)
	}

	activityTimes := make([]time.Time, 0, len(performance.DatabaseActivity))
	for _, sample := range performance.DatabaseActivity {
		activityTimes = append(activityTimes, sample.SampleTime)
	}
	if err := ensureDailyPartitions(ctx, tx, s.databaseActivityTable, activityTimes); err != nil {
		return fmt.Errorf("ensure database activity sample partitions: %w", err)
	}

	return nil
}

func ensureDailyPartitions(ctx context.Context, tx pgx.Tx, parent pgx.Identifier, times []time.Time) error {
	days := map[time.Time]struct{}{}
	for _, value := range times {
		if value.IsZero() {
			continue
		}
		day := dayStartUTC(value)
		days[day] = struct{}{}
	}

	for day := range days {
		nextDay := day.AddDate(0, 0, 1)
		partition := partitionIdentifier(parent, day)
		ddl := fmt.Sprintf(
			"create table if not exists %s partition of %s for values from (%s) to (%s)",
			partition.Sanitize(),
			parent.Sanitize(),
			quoteTimestamp(day),
			quoteTimestamp(nextDay),
		)
		if _, err := tx.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

func dayStartUTC(value time.Time) time.Time {
	year, month, day := value.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func partitionIdentifier(parent pgx.Identifier, day time.Time) pgx.Identifier {
	suffix := day.Format("2006_01_02")
	if len(parent) == 0 {
		return pgx.Identifier{"partition_" + suffix}
	}
	partition := append(pgx.Identifier(nil), parent...)
	partition[len(partition)-1] = partition[len(partition)-1] + "_" + suffix
	return partition
}

func quoteTimestamp(value time.Time) string {
	return "'" + value.UTC().Format("2006-01-02 15:04:05-07:00") + "'"
}

func (s *Sink) cleanupRetention(ctx context.Context) {
	if s.retention <= 0 {
		return
	}
	dropped, err := s.dropExpiredPartitions(ctx, time.Now().UTC().Add(-s.retention))
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL sample partitions", "error", err, "retention", s.retention.String())
		return
	}
	if dropped > 0 {
		s.logger.Info("Cleaned PostgreSQL sample partitions", "partitions_dropped", dropped, "retention", s.retention.String())
	}
}

func (s *Sink) dropExpiredPartitions(ctx context.Context, cutoff time.Time) (int, error) {
	total := 0
	for _, parent := range s.partitionedTables() {
		dropped, err := s.dropExpiredPartitionsForTable(ctx, parent, cutoff)
		if err != nil {
			return total, err
		}
		total += dropped
	}
	return total, nil
}

func (s *Sink) partitionedTables() []pgx.Identifier {
	return []pgx.Identifier{
		s.samplesTable,
		s.sqlSamplesTable,
		s.sessionSamplesTable,
		s.blockingSessionsTable,
		s.databaseActivityTable,
	}
}

func (s *Sink) dropExpiredPartitionsForTable(ctx context.Context, parent pgx.Identifier, cutoff time.Time) (int, error) {
	const query = `
select child_ns.nspname, child.relname
from pg_inherits
join pg_class parent on parent.oid = pg_inherits.inhparent
join pg_class child on child.oid = pg_inherits.inhrelid
join pg_namespace child_ns on child_ns.oid = child.relnamespace
where parent.oid = to_regclass($1)
`
	rows, err := s.pool.Query(ctx, query, parent.Sanitize())
	if err != nil {
		return 0, fmt.Errorf("list partitions for %s: %w", parent.Sanitize(), err)
	}
	defer rows.Close()

	var expired []pgx.Identifier
	for rows.Next() {
		var schema, partition string
		if err := rows.Scan(&schema, &partition); err != nil {
			return 0, fmt.Errorf("scan partition for %s: %w", parent.Sanitize(), err)
		}
		day, ok := partitionDay(parent, partition)
		if !ok {
			continue
		}
		if !day.AddDate(0, 0, 1).After(cutoff) {
			expired = append(expired, pgx.Identifier{schema, partition})
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read partitions for %s: %w", parent.Sanitize(), err)
	}

	for _, partition := range expired {
		ddl := fmt.Sprintf("drop table if exists %s", partition.Sanitize())
		if _, err := s.pool.Exec(ctx, ddl); err != nil {
			return 0, fmt.Errorf("drop expired partition %s: %w", partition.Sanitize(), err)
		}
	}
	return len(expired), nil
}

func partitionDay(parent pgx.Identifier, partition string) (time.Time, bool) {
	if len(parent) == 0 {
		return time.Time{}, false
	}
	prefix := parent[len(parent)-1] + "_"
	if !strings.HasPrefix(partition, prefix) {
		return time.Time{}, false
	}
	day, err := time.Parse("2006_01_02", strings.TrimPrefix(partition, prefix))
	if err != nil {
		return time.Time{}, false
	}
	return day, true
}

func (s *Sink) Close() {
	s.pool.Close()
}

func identifier(name, fallback string) pgx.Identifier {
	parts := strings.Split(name, ".")
	out := make(pgx.Identifier, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return pgx.Identifier{fallback}
	}
	return out
}
