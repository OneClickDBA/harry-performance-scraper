// Copyright (c) 2026 Jorge Holgado.
// Licensed under the Universal Permissive License v 1.0 as shown in LICENSE.txt.

package ha

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestLockIDIsStableAndScopeSpecific(t *testing.T) {
	const scope = "production-1"
	first := lockIDForScope(scope)
	second := lockIDForScope(scope)
	const expected int64 = 6556506446078265099
	if first != expected {
		t.Fatalf("LockID(%q) = %d, want compatibility key %d", scope, first, expected)
	}
	if first != second {
		t.Fatalf("LockID(%q) is not stable: %d != %d", scope, first, second)
	}
	if first < 0 {
		t.Fatalf("LockID(%q) = %d, want a positive key", scope, first)
	}
	if first == lockIDForScope("production-2") {
		t.Fatal("different scopes produced the same advisory-lock key")
	}
}

func TestNewAcceptsMultiHostConnectionStrings(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	connectionStrings := []string{
		"host=pg-a,pg-b,pg-c port=5432 dbname=harry_monitoring user=harry_monitoring target_session_attrs=read-write connect_timeout=3",
		"postgresql://harry_monitoring:secret@pg-a:5432,pg-b:5432,pg-c:5432/harry_monitoring?target_session_attrs=read-write&connect_timeout=3",
	}
	for _, connectionString := range connectionStrings {
		if _, err := New(logger, connectionString, "default", 5*time.Second, 2*time.Second); err != nil {
			t.Fatalf("expected multi-host connection string to parse, got %v", err)
		}
	}
}

func TestNewSetsDefaultApplicationNameWithoutOverridingConfiguration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name             string
		connectionString string
		want             string
	}{
		{
			name:             "default",
			connectionString: "postgresql://harry_monitoring:secret@localhost/harry_monitoring",
			want:             "harry-scraper-ha",
		},
		{
			name:             "configured",
			connectionString: "postgresql://harry_monitoring:secret@localhost/harry_monitoring?application_name=custom-ha",
			want:             "custom-ha",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elector, err := New(logger, tt.connectionString, "default", 5*time.Second, 2*time.Second)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if got := elector.connectionConfig.RuntimeParams["application_name"]; got != tt.want {
				t.Fatalf("application_name = %q, want %q", got, tt.want)
			}
		})
	}
}
