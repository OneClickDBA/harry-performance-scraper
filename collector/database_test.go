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

func TestIsValid(t *testing.T) {
	tests := []struct {
		name         string
		invalidUntil *time.Time
		wantNil      bool
	}{
		{
			name:         "Nil invalidUntil",
			invalidUntil: nil,
			wantNil:      true,
		},
		{
			name:         "Future invalidUntil",
			invalidUntil: func() *time.Time { t := time.Now().Add(time.Minute); return &t }(),
			wantNil:      false,
		},
		{
			name:         "Past invalidUntil",
			invalidUntil: func() *time.Time { t := time.Now().Add(-time.Minute); return &t }(),
			wantNil:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &Database{invalidUntil: tt.invalidUntil}
			result := db.IsValid()
			if tt.wantNil {
				if result != nil {
					t.Fatalf("expected nil retryAfter, got %v", *result)
				}
				return
			}
			if result == nil {
				t.Fatal("expected non-nil retryAfter")
			}
			if *result <= 0 {
				t.Fatalf("expected positive retryAfter, got %v", *result)
			}
		})
	}
}

func TestInvalidate(t *testing.T) {
	db := &Database{}
	backoff := time.Minute
	db.invalidate(backoff)
	if db.invalidUntil == nil {
		t.Fatal("Expected non-nil invalidUntil")
	}
	if time.Now().After(*db.invalidUntil) {
		t.Error("Expected invalidUntil in the future")
	}
}

func TestClearInvalid(t *testing.T) {
	db := &Database{}
	db.invalidate(time.Minute)
	db.clearInvalid()
	if db.invalidUntil != nil {
		t.Fatal("Expected invalidUntil to be cleared")
	}
}

func TestIsClosedDatabaseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "sql err conn done",
			err:  sql.ErrConnDone,
			want: true,
		},
		{
			name: "closed database text",
			err:  errors.New("sql: database is closed"),
			want: true,
		},
		{
			name: "other error",
			err:  errors.New("other"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isClosedDatabaseError(tt.err); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestWarmupConnectionPoolWithNilSessionSetsStartupReadyAndBackoff(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	db := &Database{}

	err := db.WarmupConnectionPool(logger, time.Minute)
	if err == nil {
		t.Fatal("expected warmup to fail for nil session")
	}
	if !db.StartupReady() {
		t.Fatal("expected startupReady to be true after warmup attempt")
	}
	if db.IsValid() == nil {
		t.Fatal("expected invalidUntil to be set after warmup failure")
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
					db.invalidate(time.Millisecond)
				} else {
					db.clearInvalid()
				}
				db.setUp(float64((i + j) % 2))
				_ = db.IsValid()
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
	now := time.Now()

	scraper.scrapeDatabaseSamples(sampleCh, performanceCh, errChan, database, &now)

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
	database.initCache(metrics)
	scraper := &Scraper{
		logger:               logger,
		metricsToScrape:      metrics,
		MetricsConfiguration: &MetricsConfiguration{},
	}

	errChan := make(chan error, len(metrics)+1)
	sampleCh := make(chan []MetricSample, len(metrics))
	performanceCh := make(chan PerformanceSamples, 1)
	now := time.Now()
	scraper.scrapeDatabaseSamples(sampleCh, performanceCh, errChan, database, &now)

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
		},
	}, database
}

type recordingSink struct {
	writes chan []MetricSample
	last   []MetricSample
	mu     sync.Mutex
}

func (s *recordingSink) WriteSamples(_ context.Context, samples []MetricSample, _ PerformanceSamples, _ ScrapeSummary) error {
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
