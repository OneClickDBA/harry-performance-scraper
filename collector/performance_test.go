// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"strings"
	"testing"
	"time"
)

func TestDatabaseActivitySamplesFromSessionsIncludesActiveSignals(t *testing.T) {
	active := "ACTIVE"
	inactive := "INACTIVE"
	sqlID := "f4hwbrtqu4v3g"
	userIO := "User I/O"
	idle := "Idle"

	samples := databaseActivitySamplesFromSessions([]SessionSample{
		{
			Database: "first",
			InstID:   1,
			SID:      10,
			Status:   &active,
		},
		{
			Database: "second",
			InstID:   1,
			SID:      20,
			Status:   &inactive,
			SQLID:    &sqlID,
		},
		{
			Database:  "third",
			InstID:    1,
			SID:       30,
			Status:    &inactive,
			WaitClass: &userIO,
		},
		{
			Database:  "idle",
			InstID:    1,
			SID:       40,
			Status:    &inactive,
			WaitClass: &idle,
		},
	}, time.Unix(100, 0))

	if got, want := len(samples), 3; got != want {
		t.Fatalf("len(samples) = %d, want %d", got, want)
	}
	if samples[1].SQLID == nil || *samples[1].SQLID != sqlID {
		t.Fatalf("samples[1].SQLID = %v, want %q", samples[1].SQLID, sqlID)
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
	if !scraper.shouldCollectSQLPlans("DB1", first) {
		t.Fatal("expected initial plan collection")
	}
	if scraper.shouldCollectSQLPlans("DB1", first.Add(time.Minute)) {
		t.Fatal("did not expect collection before interval")
	}
	if !scraper.shouldCollectSQLPlans("DB1", first.Add(interval)) {
		t.Fatal("expected collection at interval")
	}
	if !scraper.shouldCollectSQLPlans("DB2", first.Add(time.Minute)) {
		t.Fatal("expected independent cadence for another database")
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
