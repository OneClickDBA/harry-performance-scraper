// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"testing"
	"time"
)

func TestMissedScheduledIntervals(t *testing.T) {
	start := time.Date(2026, 9, 9, 8, 0, 0, 0, time.UTC)
	interval := 2 * time.Second
	for _, test := range []struct {
		name    string
		current time.Time
		want    int64
	}{
		{name: "on time", current: start.Add(interval), want: 0},
		{name: "minor delay", current: start.Add(interval + time.Millisecond), want: 0},
		{name: "one dropped tick", current: start.Add(2 * interval), want: 1},
		{name: "three dropped ticks", current: start.Add(4 * interval), want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := missedScheduledIntervals(start, test.current, interval); got != test.want {
				t.Fatalf("missed intervals = %d, want %d", got, test.want)
			}
		})
	}
}

func TestNewRuntimeSampleUsesLagAndCollectorResults(t *testing.T) {
	interval := 2 * time.Second
	timeout := 1500 * time.Millisecond
	scheduled := time.Date(2026, 9, 9, 8, 0, 0, 0, time.UTC)
	started := scheduled.Add(4500 * time.Millisecond)
	finished := started.Add(900 * time.Millisecond)
	scraper := &Scraper{
		MetricsConfiguration: &MetricsConfiguration{},
		instanceName:         "harry-a",
		databases:            []*Database{{Name: "DB1"}, {Name: "DB2"}},
	}
	statuses := []ScrapeStatusSample{
		{Database: "DB1", Collector: "activity", Success: true},
		{Database: "DB2", Collector: "activity", Success: false},
	}

	sample := scraper.newRuntimeSample(
		"activity", "ticker", scheduled, started, finished, interval, &timeout,
		0, statuses, "activity", 12, 1,
	)
	if sample.MissedIntervals != 2 {
		t.Fatalf("missed intervals = %d, want 2", sample.MissedIntervals)
	}
	if sample.DatabasesAttempted != 2 || sample.DatabasesSucceeded != 1 {
		t.Fatalf("database results = %d/%d, want 1/2", sample.DatabasesSucceeded, sample.DatabasesAttempted)
	}
	if sample.ConfiguredQueryTimeoutSeconds == nil || *sample.ConfiguredQueryTimeoutSeconds != 1.5 {
		t.Fatalf("query timeout = %v, want 1.5", sample.ConfiguredQueryTimeoutSeconds)
	}
	if sample.CollectionDurationSeconds != 0.9 || sample.SchedulingLagSeconds != 4.5 {
		t.Fatalf("unexpected durations: %+v", sample)
	}
}
