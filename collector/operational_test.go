// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"strings"
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
		"system counters":    systemCounterOperationalQuery,
		"CPU and I/O":        cpuIOCounterOperationalQuery,
		"system metrics":     systemMetricOperationalQuery,
		"database identity":  databaseIdentityOperationalQuery,
		"instance memory":    instanceMemoryOperationalQuery,
		"storage allocation": tablespaceAllocationOperationalQuery,
	} {
		if len(query) == 0 {
			t.Fatalf("%s query must not be empty", name)
		}
	}
	for _, counter := range []string{
		"DB CPU", "DB time", "physical read total IO requests",
		"physical write total IO requests", "physical read total bytes",
		"physical write total bytes",
	} {
		if !strings.Contains(cpuIOCounterOperationalQuery, "'"+counter+"'") {
			t.Errorf("CPU and I/O query does not contain %q", counter)
		}
	}
	if strings.Count(cpuIOCounterOperationalQuery, "con_id = 0") != 2 {
		t.Fatal("CPU and I/O query must restrict SYSSTAT and SYS_TIME_MODEL to instance-level counters")
	}
}

func TestOperationalEnrichmentMergesWithoutCreatingBaseSamples(t *testing.T) {
	dbid := int64(123456)
	uniqueName := "PROD_EU"
	product := "Oracle Database 19c Enterprise Edition"
	edition := "Enterprise Edition"
	version := "19.0.0.0.0"
	versionFull := "19.28.0.0.0"
	protectionMode := "MAXIMUM PERFORMANCE"
	protectionLevel := "MAXIMUM PERFORMANCE"
	databaseStatus := []DatabaseStatusSample{{InstID: 1}, {InstID: 2}}
	if updated := applyDatabaseIdentity(databaseStatus, []databaseIdentityRow{{
		instID: 1, dbid: &dbid, dbUniqueName: &uniqueName, databaseProduct: &product,
		databaseEdition: &edition, databaseVersion: &version, databaseVersionFull: &versionFull,
		protectionMode: &protectionMode, protectionLevel: &protectionLevel,
	}}); updated != 1 {
		t.Fatalf("database identity updates = %d, want 1", updated)
	}
	if databaseStatus[0].DBID == nil || *databaseStatus[0].DBID != dbid ||
		databaseStatus[0].DatabaseEdition == nil || *databaseStatus[0].DatabaseEdition != edition {
		t.Fatalf("database identity was not merged: %+v", databaseStatus[0])
	}
	if databaseStatus[1].DBID != nil {
		t.Fatalf("unmatched database-status sample was modified: %+v", databaseStatus[1])
	}

	sga, pga, pgaMax := int64(64<<30), int64(8<<30), int64(12<<30)
	instances := []InstanceSample{{InstID: 1}}
	if updated := applyInstanceMemory(instances, []instanceMemoryRow{{
		instID: 1, sgaAllocatedBytes: &sga, pgaAllocatedBytes: &pga, pgaMaxAllocatedBytes: &pgaMax,
	}}); updated != 1 {
		t.Fatalf("instance memory updates = %d, want 1", updated)
	}
	if instances[0].SGAAllocatedBytes == nil || *instances[0].SGAAllocatedBytes != sga ||
		instances[0].PGAMaxAllocatedBytes == nil || *instances[0].PGAMaxAllocatedBytes != pgaMax {
		t.Fatalf("instance memory was not merged: %+v", instances[0])
	}

	allocated := int64(1500 << 20)
	tablespaces := []TablespaceSample{{Tablespace: "APP_DATA"}, {Tablespace: "USERS"}}
	if updated := applyTablespaceAllocation(tablespaces, []tablespaceAllocationRow{{
		tablespace: "APP_DATA", allocatedBytes: &allocated,
	}}); updated != 1 {
		t.Fatalf("tablespace allocation updates = %d, want 1", updated)
	}
	if tablespaces[0].AllocatedBytes == nil || *tablespaces[0].AllocatedBytes != allocated {
		t.Fatalf("tablespace allocation was not merged: %+v", tablespaces[0])
	}
	if tablespaces[1].AllocatedBytes != nil {
		t.Fatalf("unmatched tablespace sample was modified: %+v", tablespaces[1])
	}
}
