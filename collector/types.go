// Copyright (c) 2025, 2026, Oracle and/or its affiliates.

package collector

import (
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Scraper collects Oracle DB metrics.
type Scraper struct {
	*MetricsConfiguration
	mu                     *sync.Mutex
	metricsToScrape        map[string]*Metric
	metricDefinitionHashes map[string][]byte
	scrapeRequests         chan struct{}
	databases              []*Database
	logger                 *slog.Logger
	allConstLabels         []string
}

type Database struct {
	Name       string
	Up         float64
	Session    *sql.DB
	Config     DatabaseConfig
	connectErr error
	// MetricsCache holds computed metrics for a database, so these metrics are available on each scrape.
	// Given a metric's scrape configuration, it may not be computed on the same interval as other metrics.
	MetricsCache *MetricsCache

	invalidUntil  *time.Time
	DatabaseLabel string
	startupReady  atomic.Bool

	reconnectMU        sync.RWMutex
	reconnectAttemptMU sync.Mutex
}

type MetricsCache struct {
	// The outer map is to be initialized at startup, and when metrics are reloaded.
	// Read access is concurrent, write access is (and must) be from a single thread.
	cache map[*Metric]*MetricCacheRecord
}

// MetricCacheRecord stores metadata associated with a given Metric
type MetricCacheRecord struct {
	// LastScraped is the collector tick time when the metric was last computed.
	LastScraped *time.Time
}

type Config struct {
	ConfigFile string
}

// Metric is an object description
type Metric struct {
	ID               string
	Context          string
	Labels           []string
	MetricsDesc      map[string]string
	MetricsType      map[string]string
	MetricsBuckets   map[string]map[string]string
	FieldToAppend    string
	Request          string
	IgnoreZeroResult bool
	QueryTimeout     string
	ScrapeInterval   string
	Databases        []string
}

// Metrics is a container structure for metrics.
type Metrics struct {
	Metric []*Metric `yaml:"metrics"`
}
