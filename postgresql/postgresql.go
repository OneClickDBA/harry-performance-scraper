// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/OneClickDBA/harry-performance-scraper/collector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Sink struct {
	pool                    *pgxpool.Pool
	logger                  *slog.Logger
	retention               time.Duration
	samplesTable            pgx.Identifier
	sqlSamplesTable         pgx.Identifier
	sqlTextsTable           pgx.Identifier
	sqlPlansTable           pgx.Identifier
	sessionSamplesTable     pgx.Identifier
	blockingSessionsTable   pgx.Identifier
	databaseActivityTable   pgx.Identifier
	databaseStatusTable     pgx.Identifier
	instanceTable           pgx.Identifier
	resourceLimitTable      pgx.Identifier
	tablespaceTable         pgx.Identifier
	asmDiskgroupTable       pgx.Identifier
	systemCounterTable      pgx.Identifier
	waitClassTable          pgx.Identifier
	systemMetricTable       pgx.Identifier
	scrapeStatusTable       pgx.Identifier
	latestScrapeStatusTable pgx.Identifier
	repositoryIngestTable   pgx.Identifier
	retentionMu             sync.Mutex
	lastRetentionCleanup    time.Time
	ingestMu                sync.Mutex
	ingestFlushMu           sync.Mutex
	pendingIngest           map[repositoryIngestKey]repositoryIngestCounts
	lastIngestFlushAttempt  time.Time
	ingestFlushInterval     time.Duration
}

const repositoryIngestFlushInterval = 5 * time.Minute

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
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, configured := poolConfig.ConnConfig.RuntimeParams["application_name"]; !configured {
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "harry-scraper"
	}

	logger.Info("Initializing PostgreSQL repository",
		"auto_migrate", cfg.GetAutoMigrate(),
		"retention", cfg.GetRetention())
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgresql pool: %w", err)
	}

	sqlSamplesTable := identifier(cfg.SQLSamplesTable, "oracle_sql_samples")
	s := &Sink{
		pool:                    pool,
		logger:                  logger,
		retention:               cfg.GetRetention(),
		samplesTable:            identifier(cfg.SamplesTable, "oracle_metric_samples"),
		sqlSamplesTable:         sqlSamplesTable,
		sqlTextsTable:           siblingIdentifier(sqlSamplesTable, "oracle_sql_texts"),
		sqlPlansTable:           siblingIdentifier(sqlSamplesTable, "oracle_sql_plans"),
		sessionSamplesTable:     identifier(cfg.SessionSamplesTable, "oracle_session_samples"),
		blockingSessionsTable:   identifier(cfg.BlockingSessionsTable, "oracle_blocking_session_samples"),
		databaseActivityTable:   identifier(cfg.DatabaseActivityTable, "oracle_database_activity_samples"),
		databaseStatusTable:     siblingIdentifier(sqlSamplesTable, "oracle_database_status_samples"),
		instanceTable:           siblingIdentifier(sqlSamplesTable, "oracle_instance_samples"),
		resourceLimitTable:      siblingIdentifier(sqlSamplesTable, "oracle_resource_limit_samples"),
		tablespaceTable:         siblingIdentifier(sqlSamplesTable, "oracle_tablespace_samples"),
		asmDiskgroupTable:       siblingIdentifier(sqlSamplesTable, "oracle_asm_diskgroup_samples"),
		systemCounterTable:      siblingIdentifier(sqlSamplesTable, "oracle_system_counter_samples"),
		waitClassTable:          siblingIdentifier(sqlSamplesTable, "oracle_wait_class_samples"),
		systemMetricTable:       siblingIdentifier(sqlSamplesTable, "oracle_system_metric_samples"),
		scrapeStatusTable:       siblingIdentifier(sqlSamplesTable, "oracle_scrape_status"),
		latestScrapeStatusTable: siblingIdentifier(sqlSamplesTable, "oracle_latest_scrape_status"),
		repositoryIngestTable:   siblingIdentifier(sqlSamplesTable, "harry_repository_daily_ingest"),
		pendingIngest:           make(map[repositoryIngestKey]repositoryIngestCounts),
		ingestFlushInterval:     repositoryIngestFlushInterval,
	}

	pingStarted := time.Now()
	logger.Info("Connecting to PostgreSQL repository")
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgresql: %w", err)
	}
	logger.Info("Connected to PostgreSQL repository", "duration", time.Since(pingStarted))
	if cfg.GetAutoMigrate() {
		migrationStarted := time.Now()
		logger.Info("Starting PostgreSQL schema migration")
		if err := s.Migrate(ctx); err != nil {
			pool.Close()
			return nil, err
		}
		logger.Info("Completed PostgreSQL schema migration", "duration", time.Since(migrationStarted))
	} else {
		logger.Info("PostgreSQL automatic schema migration is disabled")
	}
	cleanupStarted := time.Now()
	logger.Info("Starting PostgreSQL retention cleanup", "retention", s.retention)
	s.cleanupRetention(ctx)
	logger.Info("Finished PostgreSQL retention cleanup", "duration", time.Since(cleanupStarted))
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

	coreStarted := time.Now()
	s.logger.Info("Applying PostgreSQL core schema DDL")
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("migrate postgresql schema: %w", err)
	}
	s.logger.Info("Applied PostgreSQL core schema DDL", "duration", time.Since(coreStarted))
	if err := s.migrateRepositoryIngestSchema(ctx); err != nil {
		return err
	}
	return s.migrateOperationalSchema(ctx)
}

func (s *Sink) migrateRepositoryIngestSchema(ctx context.Context) error {
	started := time.Now()
	s.logger.Info("Applying PostgreSQL repository ingest schema DDL")
	if _, err := s.pool.Exec(ctx, s.repositoryIngestSchemaDDL()); err != nil {
		return fmt.Errorf("migrate PostgreSQL repository ingest schema: %w", err)
	}
	s.logger.Info("Applied PostgreSQL repository ingest schema DDL", "duration", time.Since(started))
	return nil
}

func (s *Sink) repositoryIngestSchemaDDL() string {
	return fmt.Sprintf(`
create table if not exists %s (
	sample_day timestamptz not null,
	source_database text not null,
	additional_metric_rows bigint not null default 0,
	sql_sample_rows bigint not null default 0,
	sql_text_writes bigint not null default 0,
	sql_plan_operation_writes bigint not null default 0,
	session_sample_rows bigint not null default 0,
	blocking_session_sample_rows bigint not null default 0,
	database_activity_sample_rows bigint not null default 0,
	database_status_sample_rows bigint not null default 0,
	instance_sample_rows bigint not null default 0,
	resource_limit_sample_rows bigint not null default 0,
	tablespace_sample_rows bigint not null default 0,
	asm_diskgroup_sample_rows bigint not null default 0,
	system_counter_sample_rows bigint not null default 0,
	wait_class_sample_rows bigint not null default 0,
	system_metric_sample_rows bigint not null default 0,
	scrape_status_rows bigint not null default 0,
	first_sample_at timestamptz,
	last_sample_at timestamptz,
	last_sql_sample_at timestamptz,
	last_session_sample_at timestamptz,
	last_database_activity_sample_at timestamptz,
	last_flushed_at timestamptz not null,
	primary key (sample_day, source_database)
) partition by range (sample_day);
`, s.repositoryIngestTable.Sanitize())
}

func (s *Sink) migrateOperationalSchema(ctx context.Context) error {
	started := time.Now()
	s.logger.Info("Starting PostgreSQL operational schema migration")
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL operational schema migration: %w", err)
	}
	defer tx.Rollback(ctx)

	backfillLatest, selectGrants, err := s.prepareLatestScrapeStatusTable(ctx, tx)
	if err != nil {
		return err
	}
	ddl := s.operationalSchemaDDL()
	ddlStarted := time.Now()
	s.logger.Info("Applying PostgreSQL operational schema DDL")
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("migrate PostgreSQL operational schema: %w", err)
	}
	s.logger.Info("Applied PostgreSQL operational schema DDL", "duration", time.Since(ddlStarted))
	if err := s.restoreLatestScrapeStatusGrants(ctx, tx, selectGrants); err != nil {
		return err
	}
	if backfillLatest {
		if err := s.backfillLatestScrapeStatus(ctx, tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL operational schema migration: %w", err)
	}
	s.logger.Info("Completed PostgreSQL operational schema migration", "duration", time.Since(started))
	return nil
}

type relationGrant struct {
	grantee     string
	grantOption bool
}

func (s *Sink) prepareLatestScrapeStatusTable(ctx context.Context, tx pgx.Tx) (bool, []relationGrant, error) {
	var relationKind string
	err := tx.QueryRow(ctx, `
select c.relkind::text
from pg_catalog.pg_class c
where c.oid = to_regclass($1)`, s.latestScrapeStatusTable.Sanitize()).Scan(&relationKind)
	if errors.Is(err, pgx.ErrNoRows) {
		s.logger.Info("PostgreSQL latest scrape status relation does not exist; creating table",
			"relation", s.latestScrapeStatusTable.Sanitize())
		return true, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("inspect legacy latest scrape status relation: %w", err)
	}
	if relationKind == "r" || relationKind == "p" {
		s.logger.Info("PostgreSQL latest scrape status relation is already a table",
			"relation", s.latestScrapeStatusTable.Sanitize())
		return false, nil, nil
	}
	if relationKind != "v" {
		return false, nil, fmt.Errorf("cannot migrate %s: expected a view or table, found PostgreSQL relkind %q",
			s.latestScrapeStatusTable.Sanitize(), relationKind)
	}
	s.logger.Info("Converting legacy PostgreSQL latest scrape status view to a table",
		"relation", s.latestScrapeStatusTable.Sanitize())
	rows, err := tx.Query(ctx, `
select
	case when acl.grantee = 0 then 'PUBLIC' else pg_get_userbyid(acl.grantee) end,
	acl.is_grantable
from pg_catalog.pg_class c
cross join lateral aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
where c.oid = to_regclass($1)
	and acl.privilege_type = 'SELECT'`, s.latestScrapeStatusTable.Sanitize())
	if err != nil {
		return false, nil, fmt.Errorf("read legacy latest scrape status grants: %w", err)
	}
	var grants []relationGrant
	for rows.Next() {
		var grant relationGrant
		if err := rows.Scan(&grant.grantee, &grant.grantOption); err != nil {
			rows.Close()
			return false, nil, fmt.Errorf("scan legacy latest scrape status grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, nil, fmt.Errorf("read legacy latest scrape status grants: %w", err)
	}
	rows.Close()
	s.logger.Info("Dropping legacy PostgreSQL latest scrape status view",
		"relation", s.latestScrapeStatusTable.Sanitize())
	if _, err := tx.Exec(ctx, fmt.Sprintf("drop view %s", s.latestScrapeStatusTable.Sanitize())); err != nil {
		return false, nil, fmt.Errorf("drop legacy latest scrape status view: %w", err)
	}
	s.logger.Info("Dropped legacy PostgreSQL latest scrape status view",
		"relation", s.latestScrapeStatusTable.Sanitize())
	return true, grants, nil
}

func (s *Sink) restoreLatestScrapeStatusGrants(ctx context.Context, tx pgx.Tx, grants []relationGrant) error {
	for _, grant := range grants {
		grantee := pgx.Identifier{grant.grantee}.Sanitize()
		if grant.grantee == "PUBLIC" {
			grantee = "PUBLIC"
		}
		query := fmt.Sprintf("grant select on %s to %s", s.latestScrapeStatusTable.Sanitize(), grantee)
		if grant.grantOption {
			query += " with grant option"
		}
		if _, err := tx.Exec(ctx, query); err != nil {
			return fmt.Errorf("restore latest scrape status SELECT grant for %s: %w", grant.grantee, err)
		}
	}
	return nil
}

func (s *Sink) backfillLatestScrapeStatus(ctx context.Context, tx pgx.Tx) error {
	started := time.Now()
	s.logger.Info("Backfilling PostgreSQL latest scrape status table",
		"source_relation", s.scrapeStatusTable.Sanitize(),
		"target_relation", s.latestScrapeStatusTable.Sanitize())
	query := fmt.Sprintf(`
insert into %s (
	collected_at, source_database, collector, success, duration_seconds, sample_count, error_message
)
select distinct on (source_database, collector)
	collected_at, source_database, collector, success, duration_seconds, sample_count, error_message
from %s
order by source_database, collector, collected_at desc
on conflict (source_database, collector) do update set
	collected_at = excluded.collected_at,
	success = excluded.success,
	duration_seconds = excluded.duration_seconds,
	sample_count = excluded.sample_count,
	error_message = excluded.error_message
where excluded.collected_at >= %s.collected_at`,
		s.latestScrapeStatusTable.Sanitize(),
		s.scrapeStatusTable.Sanitize(),
		s.latestScrapeStatusTable.Sanitize())
	result, err := tx.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("backfill latest scrape status table: %w", err)
	}
	s.logger.Info("Backfilled PostgreSQL latest scrape status table",
		"rows", result.RowsAffected(),
		"duration", time.Since(started))
	return nil
}

func (s *Sink) operationalSchemaDDL() string {
	databaseStatus := s.databaseStatusTable.Sanitize()
	instances := s.instanceTable.Sanitize()
	resourceLimits := s.resourceLimitTable.Sanitize()
	tablespaces := s.tablespaceTable.Sanitize()
	asmDiskgroups := s.asmDiskgroupTable.Sanitize()
	systemCounters := s.systemCounterTable.Sanitize()
	waitClasses := s.waitClassTable.Sanitize()
	systemMetrics := s.systemMetricTable.Sanitize()
	scrapeStatus := s.scrapeStatusTable.Sanitize()
	latestScrapeStatus := s.latestScrapeStatusTable.Sanitize()

	return fmt.Sprintf(`
create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	instance_name text not null,
	instance_status text not null,
	database_status text not null,
	startup_time timestamptz not null,
	open_mode text not null,
	database_role text not null,
	cdb text not null,
	con_id bigint not null,
	con_name text not null,
	platform_name text not null
) partition by range (collected_at);
create index if not exists oracle_database_status_database_time_idx
	on %s (source_database, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	user_sessions bigint not null,
	active_user_sessions bigint not null,
	background_sessions bigint not null,
	process_count bigint not null,
	cpu_count bigint,
	sga_max_bytes bigint,
	pga_aggregate_limit bigint
) partition by range (collected_at);
create index if not exists oracle_instance_database_time_idx
	on %s (source_database, inst_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	resource_name text not null,
	current_value bigint not null,
	max_value bigint not null,
	initial_limit bigint,
	limit_value bigint,
	limit_unlimited boolean not null
) partition by range (collected_at);
create index if not exists oracle_resource_limit_database_time_idx
	on %s (source_database, resource_name, inst_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	tablespace_name text not null,
	contents text not null,
	used_bytes bigint not null,
	free_bytes bigint not null,
	max_bytes bigint not null,
	used_percent double precision not null
) partition by range (collected_at);
create index if not exists oracle_tablespace_database_time_idx
	on %s (source_database, tablespace_name, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	diskgroup_name text not null,
	total_bytes bigint not null,
	free_bytes bigint not null,
	usable_bytes bigint
) partition by range (collected_at);
create index if not exists oracle_asm_diskgroup_database_time_idx
	on %s (source_database, diskgroup_name, inst_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	con_id bigint not null,
	stat_name text not null,
	cumulative_value bigint not null,
	delta_value bigint,
	interval_seconds double precision,
	counter_reset boolean not null
) partition by range (collected_at);
create index if not exists oracle_system_counter_database_time_idx
	on %s (source_database, stat_name, inst_id, con_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	con_id bigint not null,
	wait_class text not null,
	cumulative_wait_micro bigint not null,
	delta_wait_micro bigint,
	interval_seconds double precision,
	counter_reset boolean not null
) partition by range (collected_at);
create index if not exists oracle_wait_class_database_time_idx
	on %s (source_database, wait_class, inst_id, con_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	inst_id bigint not null,
	con_id bigint not null,
	metric_name text not null,
	value double precision not null,
	unit text
) partition by range (collected_at);
create index if not exists oracle_system_metric_database_time_idx
	on %s (source_database, metric_name, inst_id, con_id, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	collector text not null,
	success boolean not null,
	duration_seconds double precision not null,
	sample_count bigint not null,
	error_message text
) partition by range (collected_at);
create index if not exists oracle_scrape_status_database_collector_time_idx
	on %s (source_database, collector, collected_at desc);

create table if not exists %s (
	collected_at timestamptz not null,
	source_database text not null,
	collector text not null,
	success boolean not null,
	duration_seconds double precision not null,
	sample_count bigint not null,
	error_message text,
	primary key (source_database, collector)
);

create or replace view %s as
select distinct on (source_database, tablespace_name)
	collected_at, source_database, tablespace_name, contents, used_bytes, free_bytes, max_bytes, used_percent
from %s
order by source_database, tablespace_name, collected_at desc;

create or replace view %s as
select distinct on (source_database, resource_name, inst_id)
	collected_at, source_database, inst_id, resource_name, current_value, max_value,
	initial_limit, limit_value, limit_unlimited,
	case when limit_value > 0 then current_value * 100.0 / limit_value end as used_percent
from %s
order by source_database, resource_name, inst_id, collected_at desc;

create or replace view %s as
select distinct on (source_database, diskgroup_name, inst_id)
	collected_at, source_database, inst_id, diskgroup_name, total_bytes, free_bytes, usable_bytes,
	case when total_bytes > 0 then (total_bytes - free_bytes) * 100.0 / total_bytes end as used_percent
from %s
order by source_database, diskgroup_name, inst_id, collected_at desc;

create or replace view %s as
select collected_at, source_database, inst_id, con_id, stat_name, cumulative_value, delta_value,
	interval_seconds, counter_reset,
	case when interval_seconds > 0 and not counter_reset
		then delta_value / interval_seconds
	end as value_per_second
from %s;

create or replace view %s as
select collected_at, source_database, inst_id, con_id, wait_class, cumulative_wait_micro,
	delta_wait_micro, interval_seconds, counter_reset,
	case when interval_seconds > 0 and not counter_reset
		then delta_wait_micro / interval_seconds
	end as wait_micro_per_second
from %s;
`,
		databaseStatus, databaseStatus,
		instances, instances,
		resourceLimits, resourceLimits,
		tablespaces, tablespaces,
		asmDiskgroups, asmDiskgroups,
		systemCounters, systemCounters,
		waitClasses, waitClasses,
		systemMetrics, systemMetrics,
		scrapeStatus, scrapeStatus,
		latestScrapeStatus,
		siblingIdentifier(s.tablespaceTable, "oracle_latest_tablespace_samples").Sanitize(), tablespaces,
		siblingIdentifier(s.resourceLimitTable, "oracle_latest_resource_limit_samples").Sanitize(), resourceLimits,
		siblingIdentifier(s.asmDiskgroupTable, "oracle_latest_asm_diskgroup_samples").Sanitize(), asmDiskgroups,
		siblingIdentifier(s.systemCounterTable, "oracle_system_counter_rates").Sanitize(), systemCounters,
		siblingIdentifier(s.waitClassTable, "oracle_wait_class_rates").Sanitize(), waitClasses,
	)
}

func (s *Sink) WriteSamples(ctx context.Context, batch collector.SampleBatch, summary collector.ScrapeSummary) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin postgresql transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.ensureWritePartitions(ctx, tx, batch); err != nil {
		return err
	}

	samples := batch.AdditionalMetrics
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

	if err := s.writeSQLTexts(ctx, tx, batch.Performance); err != nil {
		return err
	}
	if err := s.writeSQLPlans(ctx, tx, batch.Performance); err != nil {
		return err
	}
	if err := s.writeSQLSamples(ctx, tx, batch.Performance.SQL); err != nil {
		return err
	}
	if err := s.writeSessionSamples(ctx, tx, batch.Performance.Sessions); err != nil {
		return err
	}
	if err := s.writeBlockingSessionSamples(ctx, tx, batch.Performance.BlockingSessions); err != nil {
		return err
	}
	if err := s.writeDatabaseActivitySamples(ctx, tx, batch.Performance.DatabaseActivity); err != nil {
		return err
	}
	if err := s.writeOperationalSamples(ctx, tx, batch.Operational); err != nil {
		return err
	}
	if err := s.writeScrapeStatuses(ctx, tx, batch.ScrapeStatuses); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit postgresql transaction: %w", err)
	}
	s.recordRepositoryIngest(batch)
	if err := s.flushRepositoryIngest(ctx, false); err != nil {
		s.logger.Warn("Unable to flush PostgreSQL repository ingestion accounting", "error", err)
	}
	logWrite := s.logger.Info
	if batch.Count() == 0 {
		logWrite = s.logger.Debug
	}
	logWrite("Wrote scrape samples to PostgreSQL",
		"samples", len(samples),
		"sql_samples", len(batch.Performance.SQL),
		"sql_texts", len(batch.Performance.SQLTexts),
		"sql_plan_operations", len(batch.Performance.SQLPlans),
		"session_samples", len(batch.Performance.Sessions),
		"blocking_session_samples", len(batch.Performance.BlockingSessions),
		"database_activity_samples", len(batch.Performance.DatabaseActivity),
		"operational_samples", batch.Operational.Count(),
		"scrape_statuses", len(batch.ScrapeStatuses),
		"errors", summary.TotalErrors,
		"duration", summary.DurationSeconds)
	s.cleanupRetention(ctx)
	return nil
}

type repositoryIngestKey struct {
	day      time.Time
	database string
}

type repositoryIngestCounts struct {
	additionalMetricRows         int64
	sqlSampleRows                int64
	sqlTextWrites                int64
	sqlPlanOperationWrites       int64
	sessionSampleRows            int64
	blockingSessionSampleRows    int64
	databaseActivitySampleRows   int64
	databaseStatusSampleRows     int64
	instanceSampleRows           int64
	resourceLimitSampleRows      int64
	tablespaceSampleRows         int64
	asmDiskgroupSampleRows       int64
	systemCounterSampleRows      int64
	waitClassSampleRows          int64
	systemMetricSampleRows       int64
	scrapeStatusRows             int64
	firstSampleAt                time.Time
	lastSampleAt                 time.Time
	lastSQLSampleAt              time.Time
	lastSessionSampleAt          time.Time
	lastDatabaseActivitySampleAt time.Time
}

func (c *repositoryIngestCounts) observe(sampleTime time.Time) {
	if sampleTime.IsZero() {
		return
	}
	if c.firstSampleAt.IsZero() || sampleTime.Before(c.firstSampleAt) {
		c.firstSampleAt = sampleTime
	}
	if c.lastSampleAt.IsZero() || sampleTime.After(c.lastSampleAt) {
		c.lastSampleAt = sampleTime
	}
}

func (c *repositoryIngestCounts) add(other repositoryIngestCounts) {
	c.additionalMetricRows += other.additionalMetricRows
	c.sqlSampleRows += other.sqlSampleRows
	c.sqlTextWrites += other.sqlTextWrites
	c.sqlPlanOperationWrites += other.sqlPlanOperationWrites
	c.sessionSampleRows += other.sessionSampleRows
	c.blockingSessionSampleRows += other.blockingSessionSampleRows
	c.databaseActivitySampleRows += other.databaseActivitySampleRows
	c.databaseStatusSampleRows += other.databaseStatusSampleRows
	c.instanceSampleRows += other.instanceSampleRows
	c.resourceLimitSampleRows += other.resourceLimitSampleRows
	c.tablespaceSampleRows += other.tablespaceSampleRows
	c.asmDiskgroupSampleRows += other.asmDiskgroupSampleRows
	c.systemCounterSampleRows += other.systemCounterSampleRows
	c.waitClassSampleRows += other.waitClassSampleRows
	c.systemMetricSampleRows += other.systemMetricSampleRows
	c.scrapeStatusRows += other.scrapeStatusRows
	if c.firstSampleAt.IsZero() || (!other.firstSampleAt.IsZero() && other.firstSampleAt.Before(c.firstSampleAt)) {
		c.firstSampleAt = other.firstSampleAt
	}
	if other.lastSampleAt.After(c.lastSampleAt) {
		c.lastSampleAt = other.lastSampleAt
	}
	if other.lastSQLSampleAt.After(c.lastSQLSampleAt) {
		c.lastSQLSampleAt = other.lastSQLSampleAt
	}
	if other.lastSessionSampleAt.After(c.lastSessionSampleAt) {
		c.lastSessionSampleAt = other.lastSessionSampleAt
	}
	if other.lastDatabaseActivitySampleAt.After(c.lastDatabaseActivitySampleAt) {
		c.lastDatabaseActivitySampleAt = other.lastDatabaseActivitySampleAt
	}
}

func addRepositoryIngestValues[T any](
	counts map[repositoryIngestKey]repositoryIngestCounts,
	values []T,
	identity func(T) (time.Time, string),
	increment func(*repositoryIngestCounts, time.Time),
) {
	for _, value := range values {
		sampleTime, database := identity(value)
		if sampleTime.IsZero() || database == "" {
			continue
		}
		key := repositoryIngestKey{day: dayStartUTC(sampleTime), database: database}
		entry := counts[key]
		entry.observe(sampleTime)
		increment(&entry, sampleTime)
		counts[key] = entry
	}
}

func collectRepositoryIngest(batch collector.SampleBatch) map[repositoryIngestKey]repositoryIngestCounts {
	counts := make(map[repositoryIngestKey]repositoryIngestCounts)
	addRepositoryIngestValues(counts, batch.AdditionalMetrics,
		func(v collector.MetricSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.additionalMetricRows++ })
	addRepositoryIngestValues(counts, batch.Performance.SQL,
		func(v collector.SQLSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, at time.Time) {
			c.sqlSampleRows++
			if at.After(c.lastSQLSampleAt) {
				c.lastSQLSampleAt = at
			}
		})
	addRepositoryIngestValues(counts, batch.Performance.SQLTexts,
		func(v collector.SQLTextSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.sqlTextWrites++ })
	addRepositoryIngestValues(counts, batch.Performance.SQLPlans,
		func(v collector.SQLPlanOperation) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.sqlPlanOperationWrites++ })
	addRepositoryIngestValues(counts, batch.Performance.Sessions,
		func(v collector.SessionSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, at time.Time) {
			c.sessionSampleRows++
			if at.After(c.lastSessionSampleAt) {
				c.lastSessionSampleAt = at
			}
		})
	addRepositoryIngestValues(counts, batch.Performance.BlockingSessions,
		func(v collector.BlockingSessionSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.blockingSessionSampleRows++ })
	addRepositoryIngestValues(counts, batch.Performance.DatabaseActivity,
		func(v collector.DatabaseActivitySample) (time.Time, string) { return v.SampleTime, v.Database },
		func(c *repositoryIngestCounts, at time.Time) {
			c.databaseActivitySampleRows++
			if at.After(c.lastDatabaseActivitySampleAt) {
				c.lastDatabaseActivitySampleAt = at
			}
		})
	addRepositoryIngestValues(counts, batch.Operational.DatabaseStatus,
		func(v collector.DatabaseStatusSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.databaseStatusSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.Instances,
		func(v collector.InstanceSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.instanceSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.ResourceLimits,
		func(v collector.ResourceLimitSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.resourceLimitSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.Tablespaces,
		func(v collector.TablespaceSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.tablespaceSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.ASMDiskgroups,
		func(v collector.ASMDiskgroupSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.asmDiskgroupSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.SystemCounters,
		func(v collector.SystemCounterSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.systemCounterSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.WaitClasses,
		func(v collector.WaitClassSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.waitClassSampleRows++ })
	addRepositoryIngestValues(counts, batch.Operational.SystemMetrics,
		func(v collector.SystemMetricSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.systemMetricSampleRows++ })
	addRepositoryIngestValues(counts, batch.ScrapeStatuses,
		func(v collector.ScrapeStatusSample) (time.Time, string) { return v.CollectedAt, v.Database },
		func(c *repositoryIngestCounts, _ time.Time) { c.scrapeStatusRows++ })
	return counts
}

func (s *Sink) recordRepositoryIngest(batch collector.SampleBatch) {
	counts := collectRepositoryIngest(batch)
	if len(counts) == 0 {
		return
	}
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	for key, value := range counts {
		pending := s.pendingIngest[key]
		pending.add(value)
		s.pendingIngest[key] = pending
	}
}

func (s *Sink) flushRepositoryIngest(ctx context.Context, force bool) error {
	s.ingestFlushMu.Lock()
	defer s.ingestFlushMu.Unlock()

	now := time.Now().UTC()
	s.ingestMu.Lock()
	interval := s.ingestFlushInterval
	if interval <= 0 {
		interval = repositoryIngestFlushInterval
	}
	if !force && !s.lastIngestFlushAttempt.IsZero() && now.Sub(s.lastIngestFlushAttempt) < interval {
		s.ingestMu.Unlock()
		return nil
	}
	s.lastIngestFlushAttempt = now
	pending := s.pendingIngest
	s.pendingIngest = make(map[repositoryIngestKey]repositoryIngestCounts)
	s.ingestMu.Unlock()
	if len(pending) == 0 {
		return nil
	}

	if err := s.writeRepositoryIngest(ctx, pending, now); err != nil {
		s.ingestMu.Lock()
		for key, value := range pending {
			current := s.pendingIngest[key]
			current.add(value)
			s.pendingIngest[key] = current
		}
		s.ingestMu.Unlock()
		return err
	}
	return nil
}

func (s *Sink) writeRepositoryIngest(
	ctx context.Context,
	counts map[repositoryIngestKey]repositoryIngestCounts,
	flushedAt time.Time,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin repository ingestion transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	days := make([]time.Time, 0, len(counts))
	keys := make([]repositoryIngestKey, 0, len(counts))
	for key := range counts {
		days = append(days, key.day)
		keys = append(keys, key)
	}
	if err := ensureRepositoryIngestPartitions(ctx, tx, s.repositoryIngestTable, days); err != nil {
		return fmt.Errorf("ensure repository ingestion partitions: %w", err)
	}
	slices.SortFunc(keys, func(a, b repositoryIngestKey) int {
		if cmp := a.day.Compare(b.day); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.database, b.database)
	})

	table := s.repositoryIngestTable.Sanitize()
	query := fmt.Sprintf(`
insert into %s (
	sample_day, source_database, additional_metric_rows, sql_sample_rows,
	sql_text_writes, sql_plan_operation_writes, session_sample_rows,
	blocking_session_sample_rows, database_activity_sample_rows,
	database_status_sample_rows, instance_sample_rows, resource_limit_sample_rows,
	tablespace_sample_rows, asm_diskgroup_sample_rows, system_counter_sample_rows,
	wait_class_sample_rows, system_metric_sample_rows, scrape_status_rows,
	first_sample_at, last_sample_at, last_sql_sample_at, last_session_sample_at,
	last_database_activity_sample_at, last_flushed_at
)
values (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
)
on conflict (sample_day, source_database) do update set
	additional_metric_rows = %s.additional_metric_rows + excluded.additional_metric_rows,
	sql_sample_rows = %s.sql_sample_rows + excluded.sql_sample_rows,
	sql_text_writes = %s.sql_text_writes + excluded.sql_text_writes,
	sql_plan_operation_writes = %s.sql_plan_operation_writes + excluded.sql_plan_operation_writes,
	session_sample_rows = %s.session_sample_rows + excluded.session_sample_rows,
	blocking_session_sample_rows = %s.blocking_session_sample_rows + excluded.blocking_session_sample_rows,
	database_activity_sample_rows = %s.database_activity_sample_rows + excluded.database_activity_sample_rows,
	database_status_sample_rows = %s.database_status_sample_rows + excluded.database_status_sample_rows,
	instance_sample_rows = %s.instance_sample_rows + excluded.instance_sample_rows,
	resource_limit_sample_rows = %s.resource_limit_sample_rows + excluded.resource_limit_sample_rows,
	tablespace_sample_rows = %s.tablespace_sample_rows + excluded.tablespace_sample_rows,
	asm_diskgroup_sample_rows = %s.asm_diskgroup_sample_rows + excluded.asm_diskgroup_sample_rows,
	system_counter_sample_rows = %s.system_counter_sample_rows + excluded.system_counter_sample_rows,
	wait_class_sample_rows = %s.wait_class_sample_rows + excluded.wait_class_sample_rows,
	system_metric_sample_rows = %s.system_metric_sample_rows + excluded.system_metric_sample_rows,
	scrape_status_rows = %s.scrape_status_rows + excluded.scrape_status_rows,
	first_sample_at = least(%s.first_sample_at, excluded.first_sample_at),
	last_sample_at = greatest(%s.last_sample_at, excluded.last_sample_at),
	last_sql_sample_at = greatest(%s.last_sql_sample_at, excluded.last_sql_sample_at),
	last_session_sample_at = greatest(%s.last_session_sample_at, excluded.last_session_sample_at),
	last_database_activity_sample_at = greatest(%s.last_database_activity_sample_at, excluded.last_database_activity_sample_at),
	last_flushed_at = greatest(%s.last_flushed_at, excluded.last_flushed_at)`,
		table, table, table, table, table, table, table, table, table, table, table, table,
		table, table, table, table, table, table, table, table, table, table, table)
	batch := &pgx.Batch{}
	for _, key := range keys {
		value := counts[key]
		batch.Queue(query,
			key.day, key.database, value.additionalMetricRows, value.sqlSampleRows,
			value.sqlTextWrites, value.sqlPlanOperationWrites, value.sessionSampleRows,
			value.blockingSessionSampleRows, value.databaseActivitySampleRows,
			value.databaseStatusSampleRows, value.instanceSampleRows, value.resourceLimitSampleRows,
			value.tablespaceSampleRows, value.asmDiskgroupSampleRows, value.systemCounterSampleRows,
			value.waitClassSampleRows, value.systemMetricSampleRows, value.scrapeStatusRows,
			nullableTime(value.firstSampleAt), nullableTime(value.lastSampleAt), nullableTime(value.lastSQLSampleAt),
			nullableTime(value.lastSessionSampleAt), nullableTime(value.lastDatabaseActivitySampleAt), flushedAt)
	}
	results := tx.SendBatch(ctx, batch)
	for range keys {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("upsert repository ingestion accounting: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close repository ingestion batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit repository ingestion transaction: %w", err)
	}
	return nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
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

func copyRows(
	ctx context.Context,
	tx pgx.Tx,
	table pgx.Identifier,
	columns []string,
	rows [][]any,
	description string,
) error {
	if len(rows) == 0 {
		return nil
	}
	if _, err := tx.CopyFrom(ctx, table, columns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy %s: %w", description, err)
	}
	return nil
}

func (s *Sink) writeOperationalSamples(ctx context.Context, tx pgx.Tx, samples collector.OperationalSamples) error {
	rows := make([][]any, 0, len(samples.DatabaseStatus))
	for _, sample := range samples.DatabaseStatus {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.InstanceName,
			sample.InstanceStatus, sample.DatabaseStatus, sample.StartupTime, sample.OpenMode,
			sample.DatabaseRole, sample.CDB, sample.ConID, sample.ConName, sample.PlatformName,
		})
	}
	if err := copyRows(ctx, tx, s.databaseStatusTable, []string{
		"collected_at", "source_database", "inst_id", "instance_name", "instance_status",
		"database_status", "startup_time", "open_mode", "database_role", "cdb", "con_id",
		"con_name", "platform_name",
	}, rows, "database status samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.Instances))
	for _, sample := range samples.Instances {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.UserSessions,
			sample.ActiveUserSessions, sample.BackgroundSessions, sample.ProcessCount,
			sample.CPUCount, sample.SGAMaxBytes, sample.PGAAggregateLimit,
		})
	}
	if err := copyRows(ctx, tx, s.instanceTable, []string{
		"collected_at", "source_database", "inst_id", "user_sessions", "active_user_sessions",
		"background_sessions", "process_count", "cpu_count", "sga_max_bytes", "pga_aggregate_limit",
	}, rows, "instance samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.ResourceLimits))
	for _, sample := range samples.ResourceLimits {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.ResourceName,
			sample.CurrentValue, sample.MaxValue, sample.InitialLimit, sample.LimitValue,
			sample.LimitUnlimited,
		})
	}
	if err := copyRows(ctx, tx, s.resourceLimitTable, []string{
		"collected_at", "source_database", "inst_id", "resource_name", "current_value",
		"max_value", "initial_limit", "limit_value", "limit_unlimited",
	}, rows, "resource limit samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.Tablespaces))
	for _, sample := range samples.Tablespaces {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.Tablespace, sample.Contents,
			sample.UsedBytes, sample.FreeBytes, sample.MaxBytes, sample.UsedPercent,
		})
	}
	if err := copyRows(ctx, tx, s.tablespaceTable, []string{
		"collected_at", "source_database", "tablespace_name", "contents", "used_bytes",
		"free_bytes", "max_bytes", "used_percent",
	}, rows, "tablespace samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.ASMDiskgroups))
	for _, sample := range samples.ASMDiskgroups {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.Name,
			sample.TotalBytes, sample.FreeBytes, sample.UsableBytes,
		})
	}
	if err := copyRows(ctx, tx, s.asmDiskgroupTable, []string{
		"collected_at", "source_database", "inst_id", "diskgroup_name", "total_bytes",
		"free_bytes", "usable_bytes",
	}, rows, "ASM diskgroup samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.SystemCounters))
	for _, sample := range samples.SystemCounters {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.ConID, sample.StatName,
			sample.CumulativeValue, sample.DeltaValue, sample.IntervalSeconds, sample.CounterReset,
		})
	}
	if err := copyRows(ctx, tx, s.systemCounterTable, []string{
		"collected_at", "source_database", "inst_id", "con_id", "stat_name", "cumulative_value",
		"delta_value", "interval_seconds", "counter_reset",
	}, rows, "system counter samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.WaitClasses))
	for _, sample := range samples.WaitClasses {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.ConID, sample.WaitClass,
			sample.CumulativeWaitMicro, sample.DeltaWaitMicro, sample.IntervalSeconds, sample.CounterReset,
		})
	}
	if err := copyRows(ctx, tx, s.waitClassTable, []string{
		"collected_at", "source_database", "inst_id", "con_id", "wait_class",
		"cumulative_wait_micro", "delta_wait_micro", "interval_seconds", "counter_reset",
	}, rows, "wait class samples"); err != nil {
		return err
	}

	rows = make([][]any, 0, len(samples.SystemMetrics))
	for _, sample := range samples.SystemMetrics {
		rows = append(rows, []any{
			sample.CollectedAt, sample.Database, sample.InstID, sample.ConID,
			sample.MetricName, sample.Value, sample.Unit,
		})
	}
	return copyRows(ctx, tx, s.systemMetricTable, []string{
		"collected_at", "source_database", "inst_id", "con_id", "metric_name", "value", "unit",
	}, rows, "system metric samples")
}

func (s *Sink) writeScrapeStatuses(ctx context.Context, tx pgx.Tx, statuses []collector.ScrapeStatusSample) error {
	rows := make([][]any, 0, len(statuses))
	for _, status := range statuses {
		rows = append(rows, []any{
			status.CollectedAt, status.Database, status.Collector, status.Success,
			status.DurationSeconds, status.SampleCount, status.ErrorMessage,
		})
	}
	if err := copyRows(ctx, tx, s.scrapeStatusTable, []string{
		"collected_at", "source_database", "collector", "success", "duration_seconds",
		"sample_count", "error_message",
	}, rows, "scrape status samples"); err != nil {
		return err
	}
	if len(statuses) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
insert into %s (
	collected_at, source_database, collector, success, duration_seconds, sample_count, error_message
)
values ($1, $2, $3, $4, $5, $6, $7)
on conflict (source_database, collector) do update set
	collected_at = excluded.collected_at,
	success = excluded.success,
	duration_seconds = excluded.duration_seconds,
	sample_count = excluded.sample_count,
	error_message = excluded.error_message
where excluded.collected_at >= %s.collected_at`,
		s.latestScrapeStatusTable.Sanitize(), s.latestScrapeStatusTable.Sanitize())
	batch := &pgx.Batch{}
	for _, status := range statuses {
		batch.Queue(query,
			status.CollectedAt, status.Database, status.Collector, status.Success,
			status.DurationSeconds, status.SampleCount, status.ErrorMessage)
	}
	results := tx.SendBatch(ctx, batch)
	for range statuses {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("upsert latest scrape status: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close latest scrape status batch: %w", err)
	}
	return nil
}

func (s *Sink) ensureWritePartitions(ctx context.Context, tx pgx.Tx, batch collector.SampleBatch) error {
	samples := batch.AdditionalMetrics
	performance := batch.Performance
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

	operationalTables := []struct {
		table pgx.Identifier
		times []time.Time
	}{
		{table: s.databaseStatusTable, times: collectedTimes(batch.Operational.DatabaseStatus, func(v collector.DatabaseStatusSample) time.Time { return v.CollectedAt })},
		{table: s.instanceTable, times: collectedTimes(batch.Operational.Instances, func(v collector.InstanceSample) time.Time { return v.CollectedAt })},
		{table: s.resourceLimitTable, times: collectedTimes(batch.Operational.ResourceLimits, func(v collector.ResourceLimitSample) time.Time { return v.CollectedAt })},
		{table: s.tablespaceTable, times: collectedTimes(batch.Operational.Tablespaces, func(v collector.TablespaceSample) time.Time { return v.CollectedAt })},
		{table: s.asmDiskgroupTable, times: collectedTimes(batch.Operational.ASMDiskgroups, func(v collector.ASMDiskgroupSample) time.Time { return v.CollectedAt })},
		{table: s.systemCounterTable, times: collectedTimes(batch.Operational.SystemCounters, func(v collector.SystemCounterSample) time.Time { return v.CollectedAt })},
		{table: s.waitClassTable, times: collectedTimes(batch.Operational.WaitClasses, func(v collector.WaitClassSample) time.Time { return v.CollectedAt })},
		{table: s.systemMetricTable, times: collectedTimes(batch.Operational.SystemMetrics, func(v collector.SystemMetricSample) time.Time { return v.CollectedAt })},
		{table: s.scrapeStatusTable, times: collectedTimes(batch.ScrapeStatuses, func(v collector.ScrapeStatusSample) time.Time { return v.CollectedAt })},
	}
	for _, operationalTable := range operationalTables {
		if err := ensureDailyPartitions(ctx, tx, operationalTable.table, operationalTable.times); err != nil {
			return fmt.Errorf("ensure %s partitions: %w", operationalTable.table.Sanitize(), err)
		}
	}

	return nil
}

func collectedTimes[T any](values []T, getTime func(T) time.Time) []time.Time {
	times := make([]time.Time, 0, len(values))
	for _, value := range values {
		times = append(times, getTime(value))
	}
	return times
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

func ensureRepositoryIngestPartitions(
	ctx context.Context,
	tx pgx.Tx,
	parent pgx.Identifier,
	times []time.Time,
) error {
	days := map[time.Time]struct{}{}
	for _, value := range times {
		if !value.IsZero() {
			days[dayStartUTC(value)] = struct{}{}
		}
	}
	for day := range days {
		nextDay := day.AddDate(0, 0, 1)
		partition := partitionIdentifier(parent, day)
		ddl := fmt.Sprintf(
			"create table if not exists %s partition of %s for values from (%s) to (%s) with (fillfactor = 70)",
			partition.Sanitize(), parent.Sanitize(), quoteTimestamp(day), quoteTimestamp(nextDay),
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
	retentionCutoff := retentionDayCutoff(cutoff)
	deletedLatestStatuses, err := s.deleteExpiredLatestScrapeStatuses(ctx, retentionCutoff)
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL latest scrape statuses", "error", err, "retention", s.retention.String())
		return
	}
	deletedSQLTexts, err := s.deleteExpiredSQLTexts(ctx, retentionCutoff)
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL SQL texts", "error", err, "retention", s.retention.String())
		return
	}
	deletedSQLPlans, err := s.deleteExpiredSQLPlans(ctx, retentionCutoff)
	if err != nil {
		s.logger.Warn("Unable to clean PostgreSQL SQL plans", "error", err, "retention", s.retention.String())
		return
	}
	if dropped > 0 {
		s.logger.Info("Cleaned PostgreSQL sample partitions", "partitions_dropped", dropped, "retention", s.retention.String())
	}
	if deletedLatestStatuses > 0 {
		s.logger.Info("Cleaned PostgreSQL latest scrape statuses", "statuses_deleted", deletedLatestStatuses, "retention", s.retention.String())
	}
	if deletedSQLTexts > 0 {
		s.logger.Info("Cleaned PostgreSQL SQL texts", "sql_texts_deleted", deletedSQLTexts, "retention", s.retention.String())
	}
	if deletedSQLPlans > 0 {
		s.logger.Info("Cleaned PostgreSQL SQL plans", "sql_plan_operations_deleted", deletedSQLPlans, "retention", s.retention.String())
	}
}

func retentionDayCutoff(partitionCutoff time.Time) time.Time {
	return dayStartUTC(partitionCutoff)
}

func (s *Sink) deleteExpiredLatestScrapeStatuses(ctx context.Context, cutoff time.Time) (int64, error) {
	query := fmt.Sprintf("delete from %s where collected_at < $1", s.latestScrapeStatusTable.Sanitize())
	result, err := s.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete latest scrape statuses older than %s: %w", cutoff.Format(time.RFC3339), err)
	}
	return result.RowsAffected(), nil
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
		s.databaseStatusTable,
		s.instanceTable,
		s.resourceLimitTable,
		s.tablespaceTable,
		s.asmDiskgroupTable,
		s.systemCounterTable,
		s.waitClassTable,
		s.systemMetricTable,
		s.scrapeStatusTable,
		s.repositoryIngestTable,
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.flushRepositoryIngest(ctx, true); err != nil {
		s.logger.Warn("Unable to flush PostgreSQL repository ingestion accounting during shutdown", "error", err)
	}
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
