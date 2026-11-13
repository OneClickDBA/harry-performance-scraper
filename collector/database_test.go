// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testQueryDriver struct{}

type testQueryConn struct {
	rows driver.Rows
}

type testQueryRows struct {
	read bool
}

type testQueryConnector struct {
	rows driver.Rows
}

type trackedQueryConnector struct {
	tracker *queryConcurrencyTracker
}

type failingConnector struct {
	err error
}

type trackedQueryConn struct {
	tracker *queryConcurrencyTracker
}

type queryConcurrencyTracker struct {
	current atomic.Int64
	maximum atomic.Int64
}

var testQueryDriverID atomic.Uint64

func (testQueryDriver) Open(name string) (driver.Conn, error) {
	return testQueryConn{rows: &testQueryRows{}}, nil
}

func (c testQueryConnector) Connect(context.Context) (driver.Conn, error) {
	rows := c.rows
	if rows == nil {
		rows = &testQueryRows{}
	}
	return testQueryConn{rows: rows}, nil
}

func (testQueryConnector) Driver() driver.Driver {
	return testQueryDriver{}
}

func (c trackedQueryConnector) Connect(context.Context) (driver.Conn, error) {
	return trackedQueryConn{tracker: c.tracker}, nil
}

func (trackedQueryConnector) Driver() driver.Driver {
	return testQueryDriver{}
}

func (c failingConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, c.err
}

func (failingConnector) Driver() driver.Driver {
	return testQueryDriver{}
}

func (testQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (testQueryConn) Close() error {
	return nil
}

func (testQueryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c testQueryConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

func (trackedQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (trackedQueryConn) Close() error {
	return nil
}

func (trackedQueryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c trackedQueryConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	current := c.tracker.current.Add(1)
	for maximum := c.tracker.maximum.Load(); current > maximum; maximum = c.tracker.maximum.Load() {
		if c.tracker.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	c.tracker.current.Add(-1)
	return &testQueryRows{}, nil
}

func (r *testQueryRows) Columns() []string {
	return []string{"value"}
}

func (r *testQueryRows) Close() error {
	return nil
}

func (r *testQueryRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = "1"
	return nil
}

func openTestQueryDB(t *testing.T) *sql.DB {
	return openTestQueryDBWithRows(t, nil)
}

func openTestQueryDBWithRows(t *testing.T, rows driver.Rows) *sql.DB {
	t.Helper()

	name := "collector-test-query-" + strconv.FormatUint(testQueryDriverID.Add(1), 10)
	sql.Register(name, testQueryDriver{})

	db := sql.OpenDB(testQueryConnector{rows: rows})
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestDatabaseConnectionStateTransitions(t *testing.T) {
	db := &Database{connection: DatabaseConnectionStatus{State: DatabaseConnectionActive}}
	failure := errors.New("network is unreachable")
	db.quarantine(failure)
	first := db.connectionStateSample(time.Now(), "harry-1")
	if first.State != string(DatabaseConnectionQuarantined) || first.QueuedAt == nil || first.LastFailure == nil {
		t.Fatalf("unexpected quarantined state: %+v", first)
	}

	// Repeated failures update diagnostics without duplicating or resetting the queue entry.
	queuedAt := *first.QueuedAt
	db.quarantine(errors.New("listener unavailable"))
	second := db.connectionStateSample(time.Now(), "harry-1")
	if second.QueuedAt == nil || !second.QueuedAt.Equal(queuedAt) {
		t.Fatalf("expected queue timestamp to remain stable, got %+v", second.QueuedAt)
	}

	probeStarted := time.Now()
	if !db.beginProbe(probeStarted) {
		t.Fatal("expected quarantined database to enter probing state")
	}
	if db.beginProbe(probeStarted) {
		t.Fatal("expected probing database not to be claimed twice")
	}
	db.finishProbe(probeStarted, nil)
	recovered := db.connectionStateSample(time.Now(), "harry-1")
	if recovered.State != string(DatabaseConnectionActive) || recovered.QueuedAt != nil || recovered.RecoveredAt == nil {
		t.Fatalf("unexpected recovered state: %+v", recovered)
	}
}

func TestActivitySuspensionPersistsUntilConnectivityRecovers(t *testing.T) {
	database := &Database{}

	database.suspendActivity(errors.New("network is unreachable"))
	if err := database.activityUnavailable(); err == nil || !strings.Contains(err.Error(), "network is unreachable") {
		t.Fatalf("expected activity suspension to retain the connectivity error, got %v", err)
	}

	database.resumeActivity()
	if err := database.activityUnavailable(); err != nil {
		t.Fatalf("expected activity collection to resume, got %v", err)
	}
}

func TestPingFailureQuarantinesUnknownNetworkErrors(t *testing.T) {
	networkErr := errors.New("dial tcp: network is unreachable")
	db := sql.OpenDB(failingConnector{err: networkErr})
	t.Cleanup(func() { _ = db.Close() })
	database := &Database{Session: db}
	database.connection.State = DatabaseConnectionActive
	err := database.ping(time.Minute)
	if !errors.Is(err, networkErr) {
		t.Fatalf("expected network failure, got %v", err)
	}
	if state := database.connectionState(); state != DatabaseConnectionQuarantined {
		t.Fatalf("expected failed ping to quarantine database, got %s", state)
	}
	if err := database.activityUnavailable(); err == nil || !strings.Contains(err.Error(), networkErr.Error()) {
		t.Fatalf("expected failed ping to suspend activity collection, got %v", err)
	}
}

func TestConnectionFailureClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "listener service unavailable", err: errors.New("ORA-12514: listener does not know service"), want: true},
		{name: "standby not open", err: errors.New("ORA-01033: initialization or shutdown in progress"), want: true},
		{name: "closed driver connection", err: driver.ErrBadConn, want: true},
		{name: "network unreachable", err: errors.New("dial tcp: network is unreachable"), want: true},
		{name: "query deadline", err: context.DeadlineExceeded, want: false},
		{name: "missing grant", err: errors.New("ORA-00942: table or view does not exist"), want: false},
		{name: "invalid SQL", err: errors.New("ORA-00900: invalid SQL statement"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isConnectionFailure(test.err); got != test.want {
				t.Fatalf("isConnectionFailure(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestWarmupConnectionPoolWithNilSessionSetsStartupReadyAndQuarantine(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	db := &Database{}

	err := db.WarmupConnectionPool(logger, time.Minute)
	if err == nil {
		t.Fatal("expected warmup to fail for nil session")
	}
	if !db.StartupReady() {
		t.Fatal("expected startupReady to be true after warmup attempt")
	}
	if state := db.connectionState(); state != DatabaseConnectionQuarantined {
		t.Fatalf("expected warmup failure to quarantine database, got %s", state)
	}
	if got := db.getUp(); got != 0 {
		t.Fatalf("expected database up metric to remain 0, got %v", got)
	}
}

func TestDatabaseStateAccessIsRaceSafe(t *testing.T) {
	db := &Database{
		DatabaseLabel: "database",
	}
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if (i+j)%2 == 0 {
					db.quarantine(errors.New("temporarily unavailable"))
				} else {
					db.activate(time.Now())
				}
				if (i+j)%3 == 0 {
					db.suspendActivity(errors.New("temporarily unavailable"))
				} else {
					db.resumeActivity()
				}
				db.setUp(float64((i + j) % 2))
				_ = db.connectionState()
				_ = db.activityUnavailable()
				_ = db.getUp()
			}
		}(i)
	}

	wg.Wait()
}

func TestQueryContextHoldsReadLockUntilUnlock(t *testing.T) {
	db := &Database{
		Session: openTestQueryDB(t),
	}

	rows, unlock, err := db.QueryContext(context.Background(), "select 1 from dual")
	if err != nil {
		t.Fatalf("expected query to succeed, got %v", err)
	}

	locked := make(chan struct{})
	go func() {
		db.reconnectMU.Lock()
		close(locked)
		db.reconnectMU.Unlock()
	}()

	select {
	case <-locked:
		t.Fatal("expected reconnect write lock to wait for active query reader")
	case <-time.After(100 * time.Millisecond):
	}

	if err := rows.Close(); err != nil {
		t.Fatalf("expected rows close to succeed, got %v", err)
	}
	unlock()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("expected reconnect write lock to proceed after query reader released lock")
	}
}

func TestScrapeDatabaseSkipsWhileStartupInProgress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scraper := &Scraper{
		logger:               logger,
		MetricsConfiguration: &MetricsConfiguration{},
	}
	database := &Database{
		Name:          "db1",
		DatabaseLabel: "database",
	}
	errChan := make(chan error, 1)
	sampleCh := make(chan []MetricSample, 1)
	performanceCh := make(chan PerformanceSamples, 1)
	operationalCh := make(chan OperationalSamples, 1)
	statusCh := make(chan []ScrapeStatusSample, 2)
	now := time.Now()

	scraper.scrapeDatabaseSamples(sampleCh, performanceCh, operationalCh, statusCh, errChan, database, &now)

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("expected nil error while startup is in progress, got %v", err)
		}
	default:
		t.Fatal("expected scrapeDatabase to send an error result")
	}

	select {
	case <-sampleCh:
		t.Fatal("did not expect samples while startup is in progress")
	default:
	}
	select {
	case <-performanceCh:
		t.Fatal("did not expect performance samples while startup is in progress")
	default:
	}
}

func TestScrapeDatabaseRunsAdditionalMetricsSerially(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tracker := &queryConcurrencyTracker{}
	db := sql.OpenDB(trackedQueryConnector{tracker: tracker})
	t.Cleanup(func() { _ = db.Close() })

	metrics := map[string]*Metric{}
	for _, id := range []string{"first", "second", "third"} {
		metrics[id] = &Metric{
			ID:          id,
			Context:     id,
			MetricsDesc: map[string]string{"value": "Test metric."},
			Request:     "select 1 as value from dual",
		}
	}
	database := &Database{
		Name:          "db1",
		Session:       db,
		DatabaseLabel: "database",
	}
	database.startupReady.Store(true)
	database.activate(time.Now())
	database.initCache(metrics)
	operationalEnabled := false
	scraper := &Scraper{
		logger:          logger,
		metricsToScrape: metrics,
		MetricsConfiguration: &MetricsConfiguration{
			Operational: OperationalConfig{Enabled: &operationalEnabled},
		},
	}

	errChan := make(chan error, len(metrics)+1)
	sampleCh := make(chan []MetricSample, len(metrics))
	performanceCh := make(chan PerformanceSamples, 1)
	operationalCh := make(chan OperationalSamples, 1)
	statusCh := make(chan []ScrapeStatusSample, 4)
	now := time.Now()
	scraper.scrapeDatabaseSamples(sampleCh, performanceCh, operationalCh, statusCh, errChan, database, &now)

	if maximum := tracker.maximum.Load(); maximum != 1 {
		t.Fatalf("expected at most one concurrent query per database, got %d", maximum)
	}
}

func TestRunScheduledScrapesRunsWhenDatabaseBecomesReady(t *testing.T) {
	scraper, database := newTestScheduledScraper(t, time.Hour)
	sink := &recordingSink{writes: make(chan []MetricSample, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go scraper.RunScheduledScrapes(ctx, sink)
	waitForSampleWrite(t, sink)

	if got := sink.lastSamples(); len(got) != 0 {
		t.Fatalf("did not expect samples before database startup is ready, got %d", len(got))
	}

	database.startupReady.Store(true)
	database.setUp(1)
	database.activate(time.Now())
	scraper.requestScheduledScrape()

	samples := waitForSampleWrite(t, sink)
	if len(samples) != 1 {
		t.Fatalf("expected one sample after database startup, got %d", len(samples))
	}
	if samples[0].Context != "test" || samples[0].Name != "value" {
		t.Fatalf("expected test value sample, got %#v", samples[0])
	}
}

func TestInitializeDatabasesRequestsScheduledScrapeAfterWarmup(t *testing.T) {
	scraper, database := newTestScheduledScraper(t, time.Hour)

	scraper.InitializeDatabases()

	if !database.StartupReady() {
		t.Fatal("expected database startup to be marked ready after warmup")
	}
	if got := len(scraper.scrapeRequests); got != 1 {
		t.Fatalf("expected one scheduled scrape request after warmup, got %d", got)
	}
}

func newTestScheduledScraper(t *testing.T, scrapeInterval time.Duration) (*Scraper, *Database) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metric := &Metric{
		ID:          "test_value",
		Context:     "test",
		MetricsDesc: map[string]string{"value": "Test metric."},
		MetricsType: map[string]string{"value": "gauge"},
		Request:     "select 1 as value from dual",
	}
	metricsToScrape := map[string]*Metric{
		metric.ID: metric,
	}
	maxOpenConns := 1
	database := &Database{
		Name:          "db1",
		Session:       openTestQueryDB(t),
		Config:        DatabaseConfig{ConnectConfig: ConnectConfig{MaxOpenConns: &maxOpenConns}},
		DatabaseLabel: "database",
	}
	database.initCache(metricsToScrape)

	operationalEnabled := false
	return &Scraper{
		mu:              &sync.Mutex{},
		metricsToScrape: metricsToScrape,
		scrapeRequests:  make(chan struct{}, 1),
		databases:       []*Database{database},
		logger:          logger,
		MetricsConfiguration: &MetricsConfiguration{
			Metrics: MetricsConfig{
				DatabaseLabel:  "database",
				ScrapeInterval: &scrapeInterval,
			},
			Operational: OperationalConfig{Enabled: &operationalEnabled},
		},
	}, database
}

type recordingSink struct {
	writes chan []MetricSample
	last   []MetricSample
	mu     sync.Mutex
}

func (s *recordingSink) WriteSamples(_ context.Context, batch SampleBatch, _ ScrapeSummary) error {
	samples := batch.AdditionalMetrics
	s.mu.Lock()
	s.last = append([]MetricSample(nil), samples...)
	s.mu.Unlock()
	s.writes <- samples
	return nil
}

func (s *recordingSink) Close() {}

func (s *recordingSink) lastSamples() []MetricSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]MetricSample(nil), s.last...)
}

func waitForSampleWrite(t *testing.T, sink *recordingSink) []MetricSample {
	t.Helper()

	select {
	case samples := <-sink.writes:
		return samples
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sample write")
		return nil
	}
}
