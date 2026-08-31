// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package postgresql

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OneClickDBA/harry-performance-scraper/collector"
	"github.com/jackc/pgx/v5"
)

func TestOperationalSchemaDDLFormatsAllIdentifiers(t *testing.T) {
	sqlSamples := pgx.Identifier{"monitoring", "oracle_sql_samples"}
	sink := &Sink{
		databaseStatusTable:     siblingIdentifier(sqlSamples, "oracle_database_status_samples"),
		instanceTable:           siblingIdentifier(sqlSamples, "oracle_instance_samples"),
		resourceLimitTable:      siblingIdentifier(sqlSamples, "oracle_resource_limit_samples"),
		tablespaceTable:         siblingIdentifier(sqlSamples, "oracle_tablespace_samples"),
		asmDiskgroupTable:       siblingIdentifier(sqlSamples, "oracle_asm_diskgroup_samples"),
		systemCounterTable:      siblingIdentifier(sqlSamples, "oracle_system_counter_samples"),
		waitClassTable:          siblingIdentifier(sqlSamples, "oracle_wait_class_samples"),
		systemMetricTable:       siblingIdentifier(sqlSamples, "oracle_system_metric_samples"),
		scrapeStatusTable:       siblingIdentifier(sqlSamples, "oracle_scrape_status"),
		latestScrapeStatusTable: siblingIdentifier(sqlSamples, "oracle_latest_scrape_status"),
	}
	ddl := sink.operationalSchemaDDL()
	if strings.Contains(ddl, "%!") {
		t.Fatalf("operational DDL contains an unresolved format directive: %s", ddl)
	}
	for _, table := range []string{
		"oracle_database_status_samples",
		"oracle_instance_samples",
		"oracle_resource_limit_samples",
		"oracle_tablespace_samples",
		"oracle_asm_diskgroup_samples",
		"oracle_system_counter_samples",
		"oracle_wait_class_samples",
		"oracle_system_metric_samples",
		"oracle_scrape_status",
		"oracle_latest_scrape_status",
	} {
		if !strings.Contains(ddl, pgx.Identifier{"monitoring", table}.Sanitize()) {
			t.Fatalf("operational DDL does not contain schema-qualified %s", table)
		}
	}
	latestTable := pgx.Identifier{"monitoring", "oracle_latest_scrape_status"}.Sanitize()
	if !strings.Contains(ddl, "create table if not exists "+latestTable) {
		t.Fatalf("operational DDL does not create latest scrape status as a table")
	}
	if strings.Contains(ddl, "create or replace view "+latestTable) {
		t.Fatalf("operational DDL still creates latest scrape status as a view")
	}
	if !strings.Contains(ddl, "primary key (source_database, collector)") {
		t.Fatalf("latest scrape status table does not define its expected primary key")
	}
}

func TestRepositoryIngestSchemaDDL(t *testing.T) {
	sink := &Sink{repositoryIngestTable: pgx.Identifier{"monitoring", "harry_repository_daily_ingest"}}
	ddl := sink.repositoryIngestSchemaDDL()
	if strings.Contains(ddl, "%!") {
		t.Fatalf("repository ingestion DDL contains an unresolved format directive: %s", ddl)
	}
	for _, expected := range []string{
		"create table if not exists \"monitoring\".\"harry_repository_daily_ingest\"",
		"partition by range (sample_day)",
		"primary key (sample_day, source_database)",
		"database_activity_sample_rows bigint not null default 0",
		"last_flushed_at timestamptz not null",
	} {
		if !strings.Contains(ddl, expected) {
			t.Fatalf("repository ingestion DDL does not contain %q", expected)
		}
	}
}

func TestCollectRepositoryIngest(t *testing.T) {
	first := time.Date(2026, 8, 27, 23, 59, 0, 0, time.UTC)
	nextDay := first.Add(2 * time.Minute)
	counts := collectRepositoryIngest(collector.SampleBatch{
		AdditionalMetrics: []collector.MetricSample{{CollectedAt: first, Database: "DB1"}},
		Performance: collector.PerformanceSamples{
			SQL:              []collector.SQLSample{{CollectedAt: first, Database: "DB1"}},
			SQLTexts:         []collector.SQLTextSample{{CollectedAt: first, Database: "DB1"}},
			SQLPlans:         []collector.SQLPlanOperation{{CollectedAt: first, Database: "DB1"}},
			Sessions:         []collector.SessionSample{{CollectedAt: nextDay, Database: "DB1"}},
			BlockingSessions: []collector.BlockingSessionSample{{CollectedAt: nextDay, Database: "DB1"}},
			DatabaseActivity: []collector.DatabaseActivitySample{{SampleTime: nextDay, Database: "DB2"}},
		},
		Operational: collector.OperationalSamples{
			DatabaseStatus: []collector.DatabaseStatusSample{{CollectedAt: nextDay, Database: "DB1"}},
			Instances:      []collector.InstanceSample{{CollectedAt: nextDay, Database: "DB1"}},
			ResourceLimits: []collector.ResourceLimitSample{{CollectedAt: nextDay, Database: "DB1"}},
			Tablespaces:    []collector.TablespaceSample{{CollectedAt: nextDay, Database: "DB1"}},
			ASMDiskgroups:  []collector.ASMDiskgroupSample{{CollectedAt: nextDay, Database: "DB1"}},
			SystemCounters: []collector.SystemCounterSample{{CollectedAt: nextDay, Database: "DB1"}},
			WaitClasses:    []collector.WaitClassSample{{CollectedAt: nextDay, Database: "DB1"}},
			SystemMetrics:  []collector.SystemMetricSample{{CollectedAt: nextDay, Database: "DB1"}},
		},
		ScrapeStatuses: []collector.ScrapeStatusSample{{CollectedAt: nextDay, Database: "DB1"}},
	})

	if len(counts) != 3 {
		t.Fatalf("accounting groups = %d, want 3", len(counts))
	}
	firstEntry := counts[repositoryIngestKey{day: dayStartUTC(first), database: "DB1"}]
	if firstEntry.additionalMetricRows != 1 || firstEntry.sqlSampleRows != 1 ||
		firstEntry.sqlTextWrites != 1 || firstEntry.sqlPlanOperationWrites != 1 {
		t.Fatalf("unexpected first-day counters: %+v", firstEntry)
	}
	if !firstEntry.lastSQLSampleAt.Equal(first) {
		t.Fatalf("last SQL sample = %s, want %s", firstEntry.lastSQLSampleAt, first)
	}

	secondEntry := counts[repositoryIngestKey{day: dayStartUTC(nextDay), database: "DB1"}]
	if secondEntry.sessionSampleRows != 1 || secondEntry.blockingSessionSampleRows != 1 ||
		secondEntry.databaseStatusSampleRows != 1 || secondEntry.instanceSampleRows != 1 ||
		secondEntry.resourceLimitSampleRows != 1 || secondEntry.tablespaceSampleRows != 1 ||
		secondEntry.asmDiskgroupSampleRows != 1 || secondEntry.systemCounterSampleRows != 1 ||
		secondEntry.waitClassSampleRows != 1 || secondEntry.systemMetricSampleRows != 1 ||
		secondEntry.scrapeStatusRows != 1 {
		t.Fatalf("unexpected second-day counters: %+v", secondEntry)
	}
	if !secondEntry.lastSessionSampleAt.Equal(nextDay) {
		t.Fatalf("last session sample = %s, want %s", secondEntry.lastSessionSampleAt, nextDay)
	}

	activityEntry := counts[repositoryIngestKey{day: dayStartUTC(nextDay), database: "DB2"}]
	if activityEntry.databaseActivitySampleRows != 1 ||
		!activityEntry.lastDatabaseActivitySampleAt.Equal(nextDay) {
		t.Fatalf("unexpected activity counters: %+v", activityEntry)
	}
}

func TestRepositoryIngestCountsAddPreservesTimestamps(t *testing.T) {
	first := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	last := first.Add(time.Hour)
	counts := repositoryIngestCounts{
		sqlSampleRows:   2,
		firstSampleAt:   last,
		lastSampleAt:    last,
		lastSQLSampleAt: last,
	}
	counts.add(repositoryIngestCounts{
		sqlSampleRows:   3,
		firstSampleAt:   first,
		lastSampleAt:    first,
		lastSQLSampleAt: first,
	})
	if counts.sqlSampleRows != 5 {
		t.Fatalf("SQL rows = %d, want 5", counts.sqlSampleRows)
	}
	if !counts.firstSampleAt.Equal(first) || !counts.lastSampleAt.Equal(last) ||
		!counts.lastSQLSampleAt.Equal(last) {
		t.Fatalf("unexpected merged timestamps: %+v", counts)
	}
}

func TestRepositoryIngestPostgreSQL(t *testing.T) {
	url := os.Getenv("HARRY_POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("HARRY_POSTGRES_TEST_URL is not set")
	}
	ctx := context.Background()
	sink, err := New(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), collector.PostgreSQLConfig{URL: url})
	if err != nil {
		t.Fatalf("create PostgreSQL sink: %v", err)
	}
	defer sink.Close()

	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	first := day.Add(time.Hour)
	last := first.Add(time.Hour)
	const database = "HARRY_REPOSITORY_INGEST_INTEGRATION_TEST"
	key := repositoryIngestKey{day: day, database: database}
	deleteQuery := "delete from " + sink.repositoryIngestTable.Sanitize() +
		" where sample_day = $1 and source_database = $2"
	if _, err := sink.pool.Exec(ctx, deleteQuery, day, database); err != nil {
		t.Fatalf("reset repository accounting test row: %v", err)
	}
	defer func() {
		if _, err := sink.pool.Exec(context.Background(), deleteQuery, day, database); err != nil {
			t.Errorf("clean up repository accounting test row: %v", err)
		}
	}()
	if err := sink.writeRepositoryIngest(ctx, map[repositoryIngestKey]repositoryIngestCounts{
		key: {sqlSampleRows: 2, firstSampleAt: first, lastSampleAt: first, lastSQLSampleAt: first},
	}, first); err != nil {
		t.Fatalf("write first accounting batch: %v", err)
	}
	if err := sink.writeRepositoryIngest(ctx, map[repositoryIngestKey]repositoryIngestCounts{
		key: {sqlSampleRows: 3, sessionSampleRows: 4, firstSampleAt: last, lastSampleAt: last, lastSessionSampleAt: last},
	}, last); err != nil {
		t.Fatalf("write second accounting batch: %v", err)
	}

	var sqlRows, sessionRows int64
	var firstSampleAt, lastSampleAt time.Time
	query := "select sql_sample_rows, session_sample_rows, first_sample_at, last_sample_at from " +
		sink.repositoryIngestTable.Sanitize() + " where sample_day = $1 and source_database = $2"
	if err := sink.pool.QueryRow(ctx, query, day, database).Scan(
		&sqlRows, &sessionRows, &firstSampleAt, &lastSampleAt,
	); err != nil {
		t.Fatalf("read repository accounting: %v", err)
	}
	if sqlRows != 5 || sessionRows != 4 {
		t.Fatalf("rows = sql:%d session:%d, want sql:5 session:4", sqlRows, sessionRows)
	}
	if !firstSampleAt.Equal(first) || !lastSampleAt.Equal(last) {
		t.Fatalf("sample range = %s to %s, want %s to %s", firstSampleAt, lastSampleAt, first, last)
	}
}

func TestPartitionDay(t *testing.T) {
	tests := []struct {
		name      string
		parent    pgx.Identifier
		partition string
		want      time.Time
		wantOK    bool
	}{
		{
			name:      "generated partition",
			parent:    pgx.Identifier{"monitoring", "oracle_metric_samples"},
			partition: "oracle_metric_samples_2026_06_26",
			want:      time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC),
			wantOK:    true,
		},
		{
			name:      "unrelated partition",
			parent:    pgx.Identifier{"oracle_metric_samples"},
			partition: "other_table_2026_06_26",
			wantOK:    false,
		},
		{
			name:      "invalid date suffix",
			parent:    pgx.Identifier{"oracle_metric_samples"},
			partition: "oracle_metric_samples_latest",
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := partitionDay(tt.parent, tt.partition)
			if ok != tt.wantOK {
				t.Fatalf("expected ok=%t, got %t", tt.wantOK, ok)
			}
			if ok && !got.Equal(tt.want) {
				t.Fatalf("expected day %s, got %s", tt.want, got)
			}
		})
	}
}

func TestRetentionDayCutoffUsesStartOfCutoffDay(t *testing.T) {
	cutoff := time.Date(2026, 7, 17, 14, 35, 12, 0, time.FixedZone("CEST", 2*60*60))
	want := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	if got := retentionDayCutoff(cutoff); !got.Equal(want) {
		t.Fatalf("retentionDayCutoff() = %s, want %s", got, want)
	}
}

func TestSiblingIdentifier(t *testing.T) {
	tests := []struct {
		name string
		base pgx.Identifier
		want pgx.Identifier
	}{
		{name: "default schema", base: pgx.Identifier{"oracle_sql_samples"}, want: pgx.Identifier{"oracle_sql_texts"}},
		{name: "configured schema", base: pgx.Identifier{"monitoring", "sql_samples"}, want: pgx.Identifier{"monitoring", "oracle_sql_texts"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := siblingIdentifier(tt.base, "oracle_sql_texts")
			if got.Sanitize() != tt.want.Sanitize() {
				t.Fatalf("siblingIdentifier() = %s, want %s", got.Sanitize(), tt.want.Sanitize())
			}
		})
	}
}

func TestSQLPlanSiblingIdentifier(t *testing.T) {
	for _, tt := range []struct {
		name string
		base pgx.Identifier
		want pgx.Identifier
	}{
		{name: "default schema", base: pgx.Identifier{"oracle_sql_samples"}, want: pgx.Identifier{"oracle_sql_plans"}},
		{name: "configured schema", base: pgx.Identifier{"monitoring", "sql_samples"}, want: pgx.Identifier{"monitoring", "oracle_sql_plans"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := siblingIdentifier(tt.base, "oracle_sql_plans")
			if got.Sanitize() != tt.want.Sanitize() {
				t.Fatalf("siblingIdentifier() = %s, want %s", got.Sanitize(), tt.want.Sanitize())
			}
		})
	}
}

func TestCollectSQLTextUpdates(t *testing.T) {
	first := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	lastReference := second.Add(time.Minute)
	oldText := "select old from dual"
	newText := "select new from dual"
	sqlID := "abc123"
	previousSQLID := "previous123"
	blockingSQLID := "blocking123"
	topLevelSQLID := "top123"

	texts, references := collectSQLTextUpdates(collector.PerformanceSamples{
		SQL: []collector.SQLSample{
			{CollectedAt: second, Database: "DB1", SQLID: sqlID, SQLFullText: &newText},
			{CollectedAt: first, Database: "DB1", SQLID: sqlID, SQLFullText: &oldText},
		},
		Sessions: []collector.SessionSample{
			{CollectedAt: lastReference, Database: "DB1", SQLID: &sqlID, PrevSQLID: &previousSQLID},
		},
		BlockingSessions: []collector.BlockingSessionSample{
			{CollectedAt: second, Database: "DB1", SQLID: &sqlID, BlockingSQLID: &blockingSQLID},
		},
		DatabaseActivity: []collector.DatabaseActivitySample{
			{CollectedAt: second, Database: "DB1", SQLID: &sqlID, TopLevelSQLID: &topLevelSQLID},
		},
	})

	key := sqlTextKey{database: "DB1", sqlID: sqlID}
	record, ok := texts[key]
	if !ok {
		t.Fatal("expected SQL text record")
	}
	if record.fullText != newText || !record.firstSeen.Equal(first) || !record.lastSeen.Equal(second) {
		t.Fatalf("unexpected SQL text record: %+v", record)
	}
	if got := references[key]; !got.Equal(lastReference) {
		t.Fatalf("latest SQL reference = %s, want %s", got, lastReference)
	}
	for _, referencedSQLID := range []string{previousSQLID, blockingSQLID, topLevelSQLID} {
		if _, ok := references[sqlTextKey{database: "DB1", sqlID: referencedSQLID}]; !ok {
			t.Fatalf("expected reference for SQL ID %s", referencedSQLID)
		}
	}
}

func TestCollectSQLPlanReferences(t *testing.T) {
	first := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	last := first.Add(time.Minute)
	child := int64(2)
	planHash := int64(12345)
	zeroPlanHash := int64(0)
	references := collectSQLPlanReferences([]collector.SQLSample{
		{CollectedAt: first, Database: "DB1", InstID: 1, SQLID: "sql1", ChildNumber: &child, PlanHashValue: &planHash},
		{CollectedAt: last, Database: "DB1", InstID: 1, SQLID: "sql1", ChildNumber: &child, PlanHashValue: &planHash},
		{CollectedAt: last, Database: "DB1", InstID: 1, SQLID: "missing-child", PlanHashValue: &planHash},
		{CollectedAt: last, Database: "DB1", InstID: 1, SQLID: "zero-plan", ChildNumber: &child, PlanHashValue: &zeroPlanHash},
	})
	if len(references) != 1 {
		t.Fatalf("references = %d, want 1", len(references))
	}
	key := sqlPlanReferenceKey{database: "DB1", instID: 1, sqlID: "sql1", childNumber: child, planHashValue: planHash}
	if got := references[key]; !got.Equal(last) {
		t.Fatalf("latest plan reference = %s, want %s", got, last)
	}
}
