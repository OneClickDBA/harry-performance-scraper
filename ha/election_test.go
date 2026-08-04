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
