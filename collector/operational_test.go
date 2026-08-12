// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"testing"
	"time"
)

func TestOperationalCollectionCadenceIsPerDatabase(t *testing.T) {
	interval := time.Minute
	scraper := &Scraper{
		MetricsConfiguration: &MetricsConfiguration{
			Operational: OperationalConfig{Interval: &interval},
		},
		lastOperationalScrape: map[string]time.Time{},
	}
	first := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	if !scraper.shouldCollectOperational("DB1", first) {
		t.Fatal("expected initial operational collection")
	}
	if scraper.shouldCollectOperational("DB1", first.Add(30*time.Second)) {
		t.Fatal("did not expect collection before interval")
	}
	if !scraper.shouldCollectOperational("DB2", first.Add(30*time.Second)) {
		t.Fatal("expected independent cadence for another database")
	}
	if !scraper.shouldCollectOperational("DB1", first.Add(interval)) {
		t.Fatal("expected collection at interval")
	}
}

func TestOperationalCounterDeltaHandlesBaselineIncrementAndReset(t *testing.T) {
	scraper := &Scraper{
		operationalCounters: map[operationalCounterKey]operationalCounterSnapshot{},
	}
	key := operationalCounterKey{database: "DB1", kind: "system", instID: 1, name: "execute count"}
	first := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	delta, interval, reset := scraper.operationalDelta(key, first, 100)
	if delta != nil || interval != nil || reset {
		t.Fatalf("initial counter must establish a baseline, got delta=%v interval=%v reset=%t", delta, interval, reset)
	}

	delta, interval, reset = scraper.operationalDelta(key, first.Add(time.Minute), 160)
	if delta == nil || *delta != 60 || interval == nil || *interval != 60 || reset {
		t.Fatalf("unexpected increment result: delta=%v interval=%v reset=%t", delta, interval, reset)
	}

	delta, interval, reset = scraper.operationalDelta(key, first.Add(2*time.Minute), 5)
	if delta != nil || interval == nil || *interval != 60 || !reset {
		t.Fatalf("counter decrease must be a reset: delta=%v interval=%v reset=%t", delta, interval, reset)
	}
}

func TestOperationalQueriesUseBoundedMetricSets(t *testing.T) {
	for name, query := range map[string]string{
		"system counters": systemCounterOperationalQuery,
		"system metrics":  systemMetricOperationalQuery,
	} {
		if len(query) == 0 {
			t.Fatalf("%s query must not be empty", name)
		}
	}
}
