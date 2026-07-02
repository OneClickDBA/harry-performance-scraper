// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
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
		Sessions:         []SessionSample{{Database: "first"}},
		BlockingSessions: []BlockingSessionSample{{Database: "first"}},
		DatabaseActivity: []DatabaseActivitySample{{Database: "first"}},
	})
	appendPerformanceSamples(&performance, PerformanceSamples{
		SQL:              []SQLSample{{Database: "second"}},
		Sessions:         []SessionSample{{Database: "second"}},
		BlockingSessions: []BlockingSessionSample{{Database: "second"}},
		DatabaseActivity: []DatabaseActivitySample{{Database: "second"}},
	})

	if got, want := len(performance.SQL), 2; got != want {
		t.Fatalf("len(performance.SQL) = %d, want %d", got, want)
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
