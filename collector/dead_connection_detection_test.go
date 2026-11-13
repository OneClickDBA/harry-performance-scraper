// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type dcdRecordingSink struct {
	batches chan SampleBatch
}

func (s *dcdRecordingSink) WriteSamples(_ context.Context, batch SampleBatch, _ ScrapeSummary) error {
	s.batches <- batch
	return nil
}

func (s *dcdRecordingSink) Close() {}

func TestDeadConnectionDetectionProbesSeriallyAndKeepsFailuresQueued(t *testing.T) {
	databases := []*Database{
		{Name: "DB1", connection: DatabaseConnectionStatus{State: DatabaseConnectionQuarantined}},
		{Name: "DB2", connection: DatabaseConnectionStatus{State: DatabaseConnectionQuarantined}},
		{Name: "DB3", connection: DatabaseConnectionStatus{State: DatabaseConnectionActive}},
	}
	for _, database := range databases[:2] {
		database.quarantine(errors.New("initial connection failure"))
	}

	var current atomic.Int64
	var maximum atomic.Int64
	scraper := &Scraper{
		logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		instanceName:         "harry-test",
		databases:            databases,
		MetricsConfiguration: &MetricsConfiguration{},
	}
	scraper.reconnectDatabase = func(database *Database, timeout time.Duration) error {
		if timeout != 5*time.Second {
			t.Fatalf("probe timeout = %s, want 5s", timeout)
		}
		active := current.Add(1)
		defer current.Add(-1)
		for observed := maximum.Load(); active > observed; observed = maximum.Load() {
			if maximum.CompareAndSwap(observed, active) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		if database.Name == "DB2" {
			return errors.New("still unavailable")
		}
		return nil
	}

	sink := &dcdRecordingSink{batches: make(chan SampleBatch, 1)}
	scraper.runDeadConnectionDetection(context.Background(), sink, time.Now())
	batch := <-sink.batches

	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent DCD probes = %d, want 1", got)
	}
	if state := databases[0].connectionState(); state != DatabaseConnectionActive {
		t.Fatalf("DB1 state = %s, want ACTIVE", state)
	}
	if state := databases[1].connectionState(); state != DatabaseConnectionQuarantined {
		t.Fatalf("DB2 state = %s, want QUARANTINED", state)
	}
	if state := databases[2].connectionState(); state != DatabaseConnectionActive {
		t.Fatalf("DB3 state = %s, want ACTIVE", state)
	}
	if len(batch.ScrapeStatuses) != 2 || len(batch.ConnectionStates) != 3 {
		t.Fatalf("unexpected DCD batch: statuses=%d states=%d", len(batch.ScrapeStatuses), len(batch.ConnectionStates))
	}
	if batch.Runtime == nil || batch.Runtime.DatabasesAttempted != 2 || batch.Runtime.DatabasesSucceeded != 1 || batch.Runtime.ErrorCount != 1 {
		t.Fatalf("unexpected DCD runtime sample: %+v", batch.Runtime)
	}
}
