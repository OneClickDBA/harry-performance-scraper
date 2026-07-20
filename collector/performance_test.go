// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestNativeActivityAndSQLQueriesAvoidASHAndSQLTextByDefault(t *testing.T) {
	if strings.Contains(strings.ToLower(sessionActivityQuery), "active_session_history") {
		t.Fatal("default session activity query must not access Oracle ASH")
	}
	if !strings.Contains(strings.ToLower(sqlPerformanceQuery), "gv$sqlstats") {
		t.Fatal("frequent SQL counter query must use GV$SQLSTATS")
	}
	if strings.Contains(strings.ToLower(sqlPerformanceQuery), "sql_fulltext") {
		t.Fatal("frequent SQL counter query must not retrieve SQL_FULLTEXT")
	}
	detailQuery, detailArgs := buildSQLDetailQuery([]string{"sql1", "sql2"})
	if !strings.Contains(strings.ToLower(detailQuery), "gv$sql") ||
		!strings.Contains(strings.ToLower(detailQuery), "where q.sql_id in (:1, :2)") ||
		!strings.Contains(strings.ToLower(detailQuery), "where child_rank = 1") {
		t.Fatalf("SQL detail query must bind selected SQL IDs and bound child cursors: %s", detailQuery)
	}
	if len(detailArgs) != 2 || detailArgs[0] != "sql1" || detailArgs[1] != "sql2" {
		t.Fatalf("unexpected SQL detail arguments: %#v", detailArgs)
	}
}

func TestNullableDurationUsesOracleValueOrConfiguredInterval(t *testing.T) {
	if got := nullableDuration(sql.NullInt64{Int64: 1_250_000, Valid: true}, 2*time.Second); got != 1_250_000 {
		t.Fatalf("duration = %d, want Oracle value", got)
	}
	if got := nullableDuration(sql.NullInt64{}, 2*time.Second); got != 2_000_000 {
		t.Fatalf("duration = %d, want configured fallback", got)
	}
}

func TestAppendPerformanceSamplesIncludesDatabaseActivity(t *testing.T) {
	var performance PerformanceSamples

	appendPerformanceSamples(&performance, PerformanceSamples{
		SQL:              []SQLSample{{Database: "first"}},
		SQLPlans:         []SQLPlanOperation{{Database: "first"}},
		Sessions:         []SessionSample{{Database: "first"}},
		BlockingSessions: []BlockingSessionSample{{Database: "first"}},
		DatabaseActivity: []DatabaseActivitySample{{Database: "first"}},
	})
	appendPerformanceSamples(&performance, PerformanceSamples{
		SQL:              []SQLSample{{Database: "second"}},
		SQLPlans:         []SQLPlanOperation{{Database: "second"}},
		Sessions:         []SessionSample{{Database: "second"}},
		BlockingSessions: []BlockingSessionSample{{Database: "second"}},
		DatabaseActivity: []DatabaseActivitySample{{Database: "second"}},
	})

	if got, want := len(performance.SQL), 2; got != want {
		t.Fatalf("len(performance.SQL) = %d, want %d", got, want)
	}
	if got, want := len(performance.SQLPlans), 2; got != want {
		t.Fatalf("len(performance.SQLPlans) = %d, want %d", got, want)
	}
	if got, want := len(performance.Sessions), 2; got != want {
		t.Fatalf("len(performance.Sessions) = %d, want %d", got, want)
	}
	if got, want := len(performance.BlockingSessions), 2; got != want {
		t.Fatalf("len(performance.BlockingSessions) = %d, want %d", got, want)
	}
	if got, want := len(performance.DatabaseActivity), 2; got != want {
		t.Fatalf("len(performance.DatabaseActivity) = %d, want %d", got, want)
	}
}

func TestSQLPlanCollectionCadence(t *testing.T) {
	interval := 2 * time.Minute
	scraper := &Scraper{
		MetricsConfiguration: &MetricsConfiguration{
			Performance: PerformanceConfig{SQLPlans: SQLPlanConfig{Interval: &interval}},
		},
	}
	first := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	if !scraper.shouldCollectSQLDetails("DB1", first) {
		t.Fatal("expected initial plan collection")
	}
	if scraper.shouldCollectSQLDetails("DB1", first.Add(time.Minute)) {
		t.Fatal("did not expect collection before interval")
	}
	if !scraper.shouldCollectSQLDetails("DB1", first.Add(interval)) {
		t.Fatal("expected collection at interval")
	}
	if !scraper.shouldCollectSQLDetails("DB2", first.Add(time.Minute)) {
		t.Fatal("expected independent cadence for another database")
	}
}

func TestTopSQLConsumersUseAccumulatedIntervalDeltas(t *testing.T) {
	scraper := &Scraper{}
	scraper.recordSQLConsumerDeltas("DB1", []SQLSample{
		testSQLCounterSample(1, "historical", 10, 10_000, 8_000, 1_000),
		testSQLCounterSample(1, "current", 20, 100, 50, 10),
		testSQLCounterSample(2, "current", 20, 200, 80, 20),
	})
	if got := scraper.takeTopSQLConsumers("DB1", 20); len(got) != 0 {
		t.Fatalf("initial counter snapshot must not create consumers, got %#v", got)
	}

	scraper.recordSQLConsumerDeltas("DB1", []SQLSample{
		testSQLCounterSample(1, "historical", 10, 10_010, 8_005, 1_001),
		testSQLCounterSample(1, "current", 20, 150, 80, 20),
		testSQLCounterSample(2, "current", 20, 240, 100, 30),
	})
	scraper.recordSQLConsumerDeltas("DB1", []SQLSample{
		testSQLCounterSample(1, "historical", 10, 10_020, 8_010, 1_002),
		testSQLCounterSample(1, "current", 20, 180, 100, 30),
		testSQLCounterSample(2, "current", 20, 280, 120, 40),
	})

	got := scraper.takeTopSQLConsumers("DB1", 2)
	want := []string{"current", "historical"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("top consumers = %#v, want %#v", got, want)
	}
	if got := scraper.takeTopSQLConsumers("DB1", 2); len(got) != 0 {
		t.Fatalf("taking consumers must clear the completed interval, got %#v", got)
	}
}

func TestSQLConsumerDeltaHandlesCounterResetAndNewCursor(t *testing.T) {
	scraper := &Scraper{}
	scraper.recordSQLConsumerDeltas("DB1", []SQLSample{
		testSQLCounterSample(1, "reset", 10, 500, 300, 100),
	})
	scraper.recordSQLConsumerDeltas("DB1", []SQLSample{
		testSQLCounterSample(1, "reset", 10, 25, 15, 5),
		testSQLCounterSample(1, "new", 20, 40, 20, 10),
	})

	got := scraper.takeTopSQLConsumers("DB1", 2)
	want := []string{"new", "reset"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("top consumers after reset = %#v, want %#v", got, want)
	}
}

func testSQLCounterSample(instID int64, sqlID string, planHash, elapsed, cpu, userIO int64) SQLSample {
	return SQLSample{
		InstID:           instID,
		SQLID:            sqlID,
		PlanHashValue:    &planHash,
		ElapsedTimeMicro: &elapsed,
		CPUTimeMicro:     &cpu,
		UserIOWaitMicro:  &userIO,
	}
}

func TestUncachedSQLPlanCandidates(t *testing.T) {
	scraper := &Scraper{}
	child := int64(1)
	planHash := int64(42)
	otherPlanHash := int64(84)
	samples := []SQLSample{
		{Database: "DB1", InstID: 1, SQLID: "sql1", ChildNumber: &child, PlanHashValue: &planHash},
		{Database: "DB1", InstID: 1, SQLID: "sql1", ChildNumber: &child, PlanHashValue: &planHash},
		{Database: "DB1", InstID: 2, SQLID: "sql2", ChildNumber: &child, PlanHashValue: &otherPlanHash},
	}

	got := scraper.uncachedSQLPlanCandidates("DB1", samples, 2)
	if len(got) != 2 {
		t.Fatalf("uncached candidates = %d, want 2", len(got))
	}
	scraper.markSQLPlansCollected("DB1", []SQLPlanOperation{{SQLPlanKey: got[0]}})
	got = scraper.uncachedSQLPlanCandidates("DB1", samples, 2)
	if len(got) != 1 || got[0] != (SQLPlanKey{InstID: 2, SQLID: "sql2", ChildNumber: child, PlanHashValue: otherPlanHash}) {
		t.Fatalf("unexpected candidates after caching: %#v", got)
	}

	got = scraper.uncachedSQLPlanCandidates("DB1", samples[1:], 1)
	if len(got) != 0 {
		t.Fatalf("expected cached active plan, got %#v", got)
	}
	got = scraper.uncachedSQLPlanCandidates("DB1", samples[2:], 1)
	if len(got) != 1 {
		t.Fatalf("expected plan leaving and returning to top N to be collected again, got %#v", got)
	}
}

func TestBuildSQLPlanQueryUsesBoundCursorKeys(t *testing.T) {
	query, args := buildSQLPlanQuery([]SQLPlanKey{
		{InstID: 1, SQLID: "sql1", ChildNumber: 0, PlanHashValue: 10},
		{InstID: 2, SQLID: "sql2", ChildNumber: 1, PlanHashValue: 20},
	})
	for _, bind := range []string{":1", ":4", ":5", ":8"} {
		if !strings.Contains(query, bind) {
			t.Fatalf("query does not contain bind %s: %s", bind, query)
		}
	}
	if len(args) != 8 {
		t.Fatalf("args = %d, want 8", len(args))
	}
}
