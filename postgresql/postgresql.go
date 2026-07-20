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
	"sync"
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
	sqlTextsTable         pgx.Identifier
	sqlPlansTable         pgx.Identifier
	sessionSamplesTable   pgx.Identifier
	blockingSessionsTable pgx.Identifier
	databaseActivityTable pgx.Identifier
	retentionMu           sync.Mutex
	lastRetentionCleanup  time.Time
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

	sqlSamplesTable := identifier(cfg.SQLSamplesTable, "oracle_sql_samples")
	s := &Sink{
		pool:                  pool,
		logger:                logger,
		retention:             cfg.GetRetention(),
		samplesTable:          identifier(cfg.SamplesTable, "oracle_metric_samples"),
		sqlSamplesTable:       sqlSamplesTable,
		sqlTextsTable:         siblingIdentifier(sqlSamplesTable, "oracle_sql_texts"),
		sqlPlansTable:         siblingIdentifier(sqlSamplesTable, "oracle_sql_plans"),
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
	sqlTexts := s.sqlTextsTable.Sanitize()
	sqlPlans := s.sqlPlansTable.Sanitize()
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
	source_database text not null,
	sql_id text not null,
	sql_fulltext text not null,
	first_seen_at timestamptz not null,
	last_text_seen_at timestamptz not null,
	last_referenced_at timestamptz not null,
	primary key (source_database, sql_id)
);

create index if not exists oracle_sql_texts_last_referenced_at_idx on %s (last_referenced_at);

create table if not exists %s (
	source_database text not null,
	inst_id bigint not null,
	sql_id text not null,
	child_number bigint not null,
	plan_hash_value bigint not null,
	plan_line_id bigint not null,
	parent_id bigint,
	depth bigint,
	position bigint,
	operation text not null,
	options text,
	object_owner text,
	object_name text,
	object_type text,
	optimizer text,
	cost bigint,
	cardinality bigint,
	bytes bigint,
	cpu_cost bigint,
	io_cost bigint,
	temp_space bigint,
	partition_start text,
	partition_stop text,
	access_predicates text,
	filter_predicates text,
	first_seen_at timestamptz not null,
	last_seen_at timestamptz not null,
	last_referenced_at timestamptz not null,
	primary key (source_database, inst_id, sql_id, child_number, plan_hash_value, plan_line_id)
);

create index if not exists oracle_sql_plans_sql_id_idx on %s (source_database, sql_id, plan_hash_value);
create index if not exists oracle_sql_plans_last_referenced_at_idx on %s (last_referenced_at);

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
	last_active_time timestamptz
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
	con_id bigint,
	sample_source text not null,
	sample_duration_micro bigint not null,
	sql_plan_hash_value bigint,
	sql_full_plan_hash_value bigint,
	sql_plan_line_id bigint,
	service_hash bigint,
	service_name text,
	client_identifier text
) partition by range (sample_time);

create unique index if not exists oracle_database_activity_samples_unique_idx on %s (sample_time, source_database, inst_id, sample_id, session_id, session_serial_number_key);
create index if not exists oracle_database_activity_samples_sample_time_idx on %s (sample_time);
create index if not exists oracle_database_activity_samples_sql_id_idx on %s (source_database, sql_id, sample_time);
create index if not exists oracle_database_activity_samples_wait_class_idx on %s (source_database, wait_class, sample_time);

alter table %s add column if not exists sample_source text not null default 'LEGACY';
alter table %s add column if not exists sample_duration_micro bigint not null default 2000000;
alter table %s add column if not exists sql_plan_hash_value bigint;
alter table %s add column if not exists sql_full_plan_hash_value bigint;
alter table %s add column if not exists sql_plan_line_id bigint;
alter table %s add column if not exists service_hash bigint;
alter table %s add column if not exists service_name text;
alter table %s add column if not exists client_identifier text;
create index if not exists oracle_database_activity_samples_source_idx on %s (source_database, sample_source, sample_time);
`, samples, samples, samples, samples,
		sqlTexts, sqlTexts,
		sqlPlans, sqlPlans, sqlPlans,
		sqlSamples, sqlSamples, sqlSamples, sqlSamples,
		sessionSamples, sessionSamples, sessionSamples, sessionSamples,
		blockingSessions, blockingSessions, blockingSessions,
		databaseActivity, databaseActivity, databaseActivity, databaseActivity, databaseActivity,
		databaseActivity, databaseActivity, databaseActivity, databaseActivity, databaseActivity,
		databaseActivity, databaseActivity, databaseActivity, databaseActivity)

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

	if err := s.writeSQLTexts(ctx, tx, performance); err != nil {
		return err
	}
	if err := s.writeSQLPlans(ctx, tx, performance); err != nil {
		return err
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
	logWrite := s.logger.Info
	if len(samples) == 0 && len(performance.SQL) == 0 && len(performance.SQLPlans) == 0 &&
		len(performance.Sessions) == 0 && len(performance.BlockingSessions) == 0 {
		logWrite = s.logger.Debug
	}
	logWrite("Wrote scrape samples to PostgreSQL",
		"samples", len(samples),
		"sql_samples", len(performance.SQL),
		"sql_texts", len(performance.SQLTexts),
		"sql_plan_operations", len(performance.SQLPlans),
		"session_samples", len(performance.Sessions),
		"blocking_session_samples", len(performance.BlockingSessions),
		"database_activity_samples", len(performance.DatabaseActivity),
		"errors", summary.TotalErrors,
		"duration", summary.DurationSeconds)
	s.cleanupRetention(ctx)
	return nil
}

type sqlTextKey struct {
	database string
	sqlID    string
}

type sqlTextRecord struct {
	fullText  string
	firstSeen time.Time
	lastSeen  time.Time
}

func (s *Sink) writeSQLTexts(ctx context.Context, tx pgx.Tx, performance collector.PerformanceSamples) error {
	texts, references := collectSQLTextUpdates(performance)

	if len(texts) > 0 {
		insertSQL := fmt.Sprintf(`insert into %s as existing (
			source_database, sql_id, sql_fulltext, first_seen_at, last_text_seen_at, last_referenced_at
		) values ($1, $2, $3, $4, $5, $5)
		on conflict (source_database, sql_id) do update set
			sql_fulltext = case
				when excluded.last_text_seen_at >= existing.last_text_seen_at then excluded.sql_fulltext
				else existing.sql_fulltext
			end,
			first_seen_at = least(existing.first_seen_at, excluded.first_seen_at),
			last_text_seen_at = greatest(existing.last_text_seen_at, excluded.last_text_seen_at),
			last_referenced_at = greatest(existing.last_referenced_at, excluded.last_referenced_at)`,
			s.sqlTextsTable.Sanitize())
		batch := &pgx.Batch{}
		for key, record := range texts {
			batch.Queue(insertSQL, key.database, key.sqlID, record.fullText, record.firstSeen, record.lastSeen)
		}
		results := tx.SendBatch(ctx, batch)
		for range texts {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("upsert SQL full text: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close SQL full text upsert batch: %w", err)
		}
	}

	if len(references) == 0 {
		return nil
	}
	databases := make([]string, 0, len(references))
	sqlIDs := make([]string, 0, len(references))
	seenAt := make([]time.Time, 0, len(references))
	for key, referencedAt := range references {
		databases = append(databases, key.database)
		sqlIDs = append(sqlIDs, key.sqlID)
		seenAt = append(seenAt, referencedAt)
	}
	updateSQL := fmt.Sprintf(`update %s as texts
	set last_referenced_at = greatest(texts.last_referenced_at, refs.referenced_at)
	from unnest($1::text[], $2::text[], $3::timestamptz[])
		as refs(source_database, sql_id, referenced_at)
	where texts.source_database = refs.source_database
		and texts.sql_id = refs.sql_id`, s.sqlTextsTable.Sanitize())
	if _, err := tx.Exec(ctx, updateSQL, databases, sqlIDs, seenAt); err != nil {
		return fmt.Errorf("update SQL text references: %w", err)
	}
	return nil
}

func collectSQLTextUpdates(performance collector.PerformanceSamples) (map[sqlTextKey]sqlTextRecord, map[sqlTextKey]time.Time) {
	texts := make(map[sqlTextKey]sqlTextRecord)
	references := make(map[sqlTextKey]time.Time)
	addReference := func(database, sqlID string, seenAt time.Time) {
		if strings.TrimSpace(database) == "" || strings.TrimSpace(sqlID) == "" || seenAt.IsZero() {
			return
		}
		key := sqlTextKey{database: database, sqlID: sqlID}
		if previous, ok := references[key]; !ok || seenAt.After(previous) {
			references[key] = seenAt
		}
	}
	addOptionalReference := func(database string, sqlID *string, seenAt time.Time) {
		if sqlID != nil {
			addReference(database, *sqlID, seenAt)
		}
	}

	for _, sample := range performance.SQL {
		addReference(sample.Database, sample.SQLID, sample.CollectedAt)
		if sample.SQLFullText == nil {
			continue
		}
		key := sqlTextKey{database: sample.Database, sqlID: sample.SQLID}
		record, ok := texts[key]
		if !ok {
			texts[key] = sqlTextRecord{fullText: *sample.SQLFullText, firstSeen: sample.CollectedAt, lastSeen: sample.CollectedAt}
			continue
		}
		if sample.CollectedAt.Before(record.firstSeen) {
			record.firstSeen = sample.CollectedAt
		}
		if !sample.CollectedAt.Before(record.lastSeen) {
			record.fullText = *sample.SQLFullText
			record.lastSeen = sample.CollectedAt
		}
		texts[key] = record
	}
	for _, sample := range performance.SQLTexts {
		addReference(sample.Database, sample.SQLID, sample.CollectedAt)
		key := sqlTextKey{database: sample.Database, sqlID: sample.SQLID}
		record, ok := texts[key]
		if !ok {
			texts[key] = sqlTextRecord{fullText: sample.SQLFullText, firstSeen: sample.CollectedAt, lastSeen: sample.CollectedAt}
			continue
		}
		if sample.CollectedAt.Before(record.firstSeen) {
			record.firstSeen = sample.CollectedAt
		}
		if !sample.CollectedAt.Before(record.lastSeen) {
			record.fullText = sample.SQLFullText
			record.lastSeen = sample.CollectedAt
		}
		texts[key] = record
	}
	for _, sample := range performance.Sessions {
		addOptionalReference(sample.Database, sample.SQLID, sample.CollectedAt)
		addOptionalReference(sample.Database, sample.PrevSQLID, sample.CollectedAt)
	}
	for _, sample := range performance.BlockingSessions {
		addOptionalReference(sample.Database, sample.SQLID, sample.CollectedAt)
		addOptionalReference(sample.Database, sample.BlockingSQLID, sample.CollectedAt)
	}
	for _, sample := range performance.DatabaseActivity {
		addOptionalReference(sample.Database, sample.SQLID, sample.CollectedAt)
		addOptionalReference(sample.Database, sample.TopLevelSQLID, sample.CollectedAt)
	}
	return texts, references
}

type sqlPlanReferenceKey struct {
	database      string
	instID        int64
	sqlID         string
	childNumber   int64
	planHashValue int64
}

func (s *Sink) writeSQLPlans(ctx context.Context, tx pgx.Tx, performance collector.PerformanceSamples) error {
	if len(performance.SQLPlans) > 0 {
		insertSQL := fmt.Sprintf(`insert into %s as existing (
			source_database, inst_id, sql_id, child_number, plan_hash_value, plan_line_id,
			parent_id, depth, position, operation, options, object_owner, object_name, object_type,
			optimizer, cost, cardinality, bytes, cpu_cost, io_cost, temp_space, partition_start,
			partition_stop, access_predicates, filter_predicates, first_seen_at, last_seen_at, last_referenced_at
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $26, $26
		)
		on conflict (source_database, inst_id, sql_id, child_number, plan_hash_value, plan_line_id)
		do update set
			parent_id = excluded.parent_id,
			depth = excluded.depth,
			position = excluded.position,
			operation = excluded.operation,
			options = excluded.options,
			object_owner = excluded.object_owner,
			object_name = excluded.object_name,
			object_type = excluded.object_type,
			optimizer = excluded.optimizer,
			cost = excluded.cost,
			cardinality = excluded.cardinality,
			bytes = excluded.bytes,
			cpu_cost = excluded.cpu_cost,
			io_cost = excluded.io_cost,
			temp_space = excluded.temp_space,
			partition_start = excluded.partition_start,
			partition_stop = excluded.partition_stop,
			access_predicates = excluded.access_predicates,
			filter_predicates = excluded.filter_predicates,
			first_seen_at = least(existing.first_seen_at, excluded.first_seen_at),
			last_seen_at = greatest(existing.last_seen_at, excluded.last_seen_at),
			last_referenced_at = greatest(existing.last_referenced_at, excluded.last_referenced_at)`,
			s.sqlPlansTable.Sanitize())
		batch := &pgx.Batch{}
		for _, plan := range performance.SQLPlans {
			batch.Queue(insertSQL,
				plan.Database, plan.InstID, plan.SQLID, plan.ChildNumber, plan.PlanHashValue, plan.PlanLineID,
				plan.ParentID, plan.Depth, plan.Position, plan.Operation, plan.Options, plan.ObjectOwner,
				plan.ObjectName, plan.ObjectType, plan.Optimizer, plan.Cost, plan.Cardinality, plan.Bytes,
				plan.CPUCost, plan.IOCost, plan.TempSpace, plan.PartitionStart, plan.PartitionStop,
				plan.AccessPredicates, plan.FilterPredicates, plan.CollectedAt,
			)
		}
		results := tx.SendBatch(ctx, batch)
		for range performance.SQLPlans {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("upsert SQL plan operation: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close SQL plan upsert batch: %w", err)
		}
	}

	referenceSamples := make([]collector.SQLSample, 0, len(performance.SQL)+len(performance.SQLDetails))
	referenceSamples = append(referenceSamples, performance.SQL...)
	referenceSamples = append(referenceSamples, performance.SQLDetails...)
	references := collectSQLPlanReferences(referenceSamples)
	if len(references) == 0 {
		return nil
	}
	databases := make([]string, 0, len(references))
	instIDs := make([]int64, 0, len(references))
	sqlIDs := make([]string, 0, len(references))
	childNumbers := make([]int64, 0, len(references))
	planHashValues := make([]int64, 0, len(references))
	referencedAt := make([]time.Time, 0, len(references))
	for key, seenAt := range references {
		databases = append(databases, key.database)
		instIDs = append(instIDs, key.instID)
		sqlIDs = append(sqlIDs, key.sqlID)
		childNumbers = append(childNumbers, key.childNumber)
		planHashValues = append(planHashValues, key.planHashValue)
		referencedAt = append(referencedAt, seenAt)
	}
	updateSQL := fmt.Sprintf(`update %s as plans
	set last_referenced_at = greatest(plans.last_referenced_at, refs.referenced_at)
	from unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::timestamptz[])
		as refs(source_database, inst_id, sql_id, child_number, plan_hash_value, referenced_at)
	where plans.source_database = refs.source_database
		and plans.inst_id = refs.inst_id
		and plans.sql_id = refs.sql_id
		and plans.child_number = refs.child_number
		and plans.plan_hash_value = refs.plan_hash_value`, s.sqlPlansTable.Sanitize())
	if _, err := tx.Exec(ctx, updateSQL, databases, instIDs, sqlIDs, childNumbers, planHashValues, referencedAt); err != nil {
		return fmt.Errorf("update SQL plan references: %w", err)
	}
	return nil
}

func collectSQLPlanReferences(samples []collector.SQLSample) map[sqlPlanReferenceKey]time.Time {
	references := make(map[sqlPlanReferenceKey]time.Time)
	for _, sample := range samples {
		if sample.ChildNumber == nil || sample.PlanHashValue == nil || *sample.PlanHashValue <= 0 {
			continue
		}
		key := sqlPlanReferenceKey{
			database:      sample.Database,
			instID:        sample.InstID,
			sqlID:         sample.SQLID,
			childNumber:   *sample.ChildNumber,
			planHashValue: *sample.PlanHashValue,
		}
		if previous, ok := references[key]; !ok || sample.CollectedAt.After(previous) {
			references[key] = sample.CollectedAt
		}
	}
	return references
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
		})
	}

	if _, err := tx.CopyFrom(ctx, s.sqlSamplesTable, []string{
		"collected_at", "source_database", "inst_id", "sql_id", "child_number", "plan_hash_value",
		"parsing_schema_name", "module", "executions", "elapsed_time_micro", "cpu_time_micro", "user_io_wait_micro",
		"application_wait_micro", "concurrency_wait_micro", "cluster_wait_micro", "buffer_gets", "disk_reads",
		"direct_writes", "rows_processed", "fetches", "loads", "invalidations", "parse_calls", "last_active_time",
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
		con_id,
		sample_source,
		sample_duration_micro,
		sql_plan_hash_value,
		sql_full_plan_hash_value,
		sql_plan_line_id,
		service_hash,
		service_name,
		client_identifier
	) values (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
		$31, $32, $33, $34, $35, $36, $37, $38, $39, $40
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
			sample.SampleSource,
			sample.SampleDurationMicro,
			sample.SQLPlanHashValue,
			sample.SQLFullPlanHashValue,
			sample.SQLPlanLineID,
			sample.ServiceHash,
			sample.ServiceName,
			sample.ClientIdentifier,
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
	s.retentionMu.Lock()
	defer s.retentionMu.Unlock()
	if !s.lastRetentionCleanup.IsZero() && time.Since(s.lastRetentionCleanup) < time.Hour {
		return
	}
	s.lastRetentionCleanup = time.Now()
	cutoff := time.Now().UTC().Add(-s.retention)
	dropped, err := s.dropExpiredPartitions(ctx, cutoff)
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL sample partitions", "error", err, "retention", s.retention.String())
		return
	}
	deletedSQLTexts, err := s.deleteExpiredSQLTexts(ctx, sqlTextRetentionCutoff(cutoff))
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL SQL texts", "error", err, "retention", s.retention.String())
		return
	}
	deletedSQLPlans, err := s.deleteExpiredSQLPlans(ctx, sqlTextRetentionCutoff(cutoff))
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL SQL plans", "error", err, "retention", s.retention.String())
		return
	}
	if dropped > 0 {
		s.logger.Info("Cleaned PostgreSQL sample partitions", "partitions_dropped", dropped, "retention", s.retention.String())
	}
	if deletedSQLTexts > 0 {
		s.logger.Info("Cleaned PostgreSQL SQL texts", "sql_texts_deleted", deletedSQLTexts, "retention", s.retention.String())
	}
	if deletedSQLPlans > 0 {
		s.logger.Info("Cleaned PostgreSQL SQL plans", "sql_plan_operations_deleted", deletedSQLPlans, "retention", s.retention.String())
	}
}

func sqlTextRetentionCutoff(partitionCutoff time.Time) time.Time {
	return dayStartUTC(partitionCutoff)
}

func (s *Sink) deleteExpiredSQLTexts(ctx context.Context, cutoff time.Time) (int64, error) {
	query := fmt.Sprintf("delete from %s where last_referenced_at < $1", s.sqlTextsTable.Sanitize())
	result, err := s.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete SQL texts last referenced before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	return result.RowsAffected(), nil
}

func (s *Sink) deleteExpiredSQLPlans(ctx context.Context, cutoff time.Time) (int64, error) {
	query := fmt.Sprintf("delete from %s where last_referenced_at < $1", s.sqlPlansTable.Sanitize())
	result, err := s.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete SQL plans last referenced before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	return result.RowsAffected(), nil
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

func siblingIdentifier(identifier pgx.Identifier, table string) pgx.Identifier {
	if len(identifier) <= 1 {
		return pgx.Identifier{table}
	}
	sibling := append(pgx.Identifier(nil), identifier...)
	sibling[len(sibling)-1] = table
	return sibling
}
