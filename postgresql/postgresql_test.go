// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package postgresql

import (
	"testing"
	"time"

	"github.com/dodger-one/oracledb-performance-scraper/collector"
	"github.com/jackc/pgx/v5"
)

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

func TestSQLTextRetentionCutoffUsesStartOfCutoffDay(t *testing.T) {
	cutoff := time.Date(2026, 7, 17, 14, 35, 12, 0, time.FixedZone("CEST", 2*60*60))
	want := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	if got := sqlTextRetentionCutoff(cutoff); !got.Equal(want) {
		t.Fatalf("sqlTextRetentionCutoff() = %s, want %s", got, want)
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
