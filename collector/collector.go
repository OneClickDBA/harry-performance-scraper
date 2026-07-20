// Copyright (c) 2021, 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
// Portions Copyright (c) 2016 Seth Miller <seth@sethmiller.me>

package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

func maskDsn(dsn string) string {
	parts := strings.Split(dsn, "@")
	if len(parts) > 1 {
		maskedURL := "***@" + parts[1]
		return maskedURL
	}
	return dsn
}

// NewScraper creates a new Scraper instance
func NewScraper(logger *slog.Logger, m *MetricsConfiguration) *Scraper {
	var databases []*Database

	var allConstLabels []string
	// All the metrics of the same name need to have the same set of labels
	// If a label is set for a particular database, it must be included also
	// in the same metrics collected from other databases. It will just be
	// set to a blank value.
	for _, dbconfig := range m.Databases {
		for label, _ := range dbconfig.Labels {
			if !slices.Contains(allConstLabels, label) {
				allConstLabels = append(allConstLabels, label)
			}
		}
	}

	for dbname, dbconfig := range m.Databases {
		logger.Info("Registering database", "database", dbname)
		database := NewDatabase(logger, m.DatabaseLabel(), dbname, dbconfig)
		databases = append(databases, database)
	}
	e := &Scraper{
		mu:                      &sync.Mutex{},
		metricDefinitionHashes:  map[string][]byte{},
		scrapeRequests:          make(chan struct{}, 1),
		logger:                  logger,
		MetricsConfiguration:    m,
		databases:               databases,
		allConstLabels:          allConstLabels,
		lastSQLDetailCollection: map[string]time.Time{},
		knownPlans:              map[string]map[SQLPlanKey]struct{}{},
		sqlCounterSnapshots:     map[string]map[sqlCounterKey]sqlCounterValues{},
		sqlConsumerDeltas:       map[string]map[string]sqlCounterValues{},
		activityWatermarks:      map[string]time.Time{},
	}
	metricsToScrape, err := e.loadMetricsToScrape()
	if err != nil {
		logger.Error("failed to load additional metric definitions during startup; continuing with native performance collection only", "error", err)
		metricsToScrape = map[string]*Metric{}
	}
	e.metricsToScrape = metricsToScrape
	e.initCache()
	return e
}

func (e *Scraper) InitializeDatabases() {
	for _, database := range e.databases {
		e.logger.Info("Starting database connection warmup", "database", database.Name)
		if err := database.WarmupConnectionPool(e.logger, e.MetricsConfiguration.ConnectionBackoff()); err != nil {
			e.logger.Error("Database startup warmup failed", "error", err, "database", database.Name)
		}
		e.requestScheduledScrape()
	}
}

func (e *Scraper) constLabels() map[string]string {
	// All the metrics of the same name need to have the same labels
	// If a label is set for a particular database, it must be included also
	// in the same metrics collected from other databases. It will just be
	// set to a blank value.
	labels := map[string]string{}
	for _, label := range e.allConstLabels {
		labels[label] = ""
	}
	return labels
}

// RunScheduledScrapes scrapes Oracle databases on an interval and writes each pass to the sink in bulk.
func (e *Scraper) RunScheduledScrapes(ctx context.Context, sink SampleSink) {
	interval := e.scrapeInterval()
	if interval == 0 {
		interval = 15 * time.Second
		e.logger.Info("metrics.scrapeInterval is not set; defaulting PostgreSQL export interval", "interval", interval)
	}

	e.doScrape(ctx, sink, time.Now())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case tick := <-ticker.C:
			e.doScrape(ctx, sink, tick)
		case <-e.scrapeRequests:
			e.doScrape(ctx, sink, time.Now())
		case <-ctx.Done():
			return
		}
	}
}

func (e *Scraper) doScrape(ctx context.Context, sink SampleSink, tick time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	samples, performance, summary := e.scrapeSamples(&tick)
	if err := sink.WriteSamples(ctx, samples, performance, summary); err != nil {
		e.logger.Error("failed to write samples", "error", err, "samples", len(samples))
	}
}

func (e *Scraper) requestScheduledScrape() {
	if e.scrapeRequests == nil {
		return
	}

	// Do not let database warmup block on the scheduler. If a scrape is already queued,
	// the next scheduled scrape will pick up all databases that are ready by then.
	select {
	case e.scrapeRequests <- struct{}{}:
	default:
	}
}

func (e *Scraper) scrapeInterval() time.Duration {
	if e.MetricsConfiguration == nil || e.MetricsConfiguration.Metrics.ScrapeInterval == nil {
		return 0
	}
	return e.ScrapeInterval()
}

func (e *Scraper) scrapeSamples(tick *time.Time) ([]MetricSample, PerformanceSamples, ScrapeSummary) {
	begun := time.Now()
	if e.checkIfMetricsChanged() {
		e.reloadMetrics()
	}

	errChan := make(chan error, (len(e.metricsToScrape)+1)*len(e.databases))
	sampleCh := make(chan []MetricSample, len(e.metricsToScrape)*len(e.databases))
	performanceCh := make(chan PerformanceSamples, len(e.databases))

	var wg sync.WaitGroup
	for _, db := range e.databases {
		db := db
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.scrapeDatabaseSamples(sampleCh, performanceCh, errChan, db, tick)
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
		close(sampleCh)
		close(performanceCh)
	}()

	totalErrors := 0
	for scrapeError := range errChan {
		if scrapeError != nil {
			totalErrors++
		}
	}

	var samples []MetricSample
	for metricSamples := range sampleCh {
		samples = append(samples, metricSamples...)
	}

	var performance PerformanceSamples
	for databasePerformance := range performanceCh {
		appendPerformanceSamples(&performance, databasePerformance)
	}

	finished := time.Now()
	return samples, performance, ScrapeSummary{
		StartedAt:       begun,
		FinishedAt:      finished,
		DurationSeconds: finished.Sub(begun).Seconds(),
		TotalErrors:     totalErrors,
		SampleCount:     len(samples),
	}
}

func (e *Scraper) scrapeDatabaseSamples(sampleCh chan<- []MetricSample, performanceCh chan<- PerformanceSamples, errChan chan<- error, d *Database, tick *time.Time) {
	dbScrapeStart := time.Now()
	defer func() {
		e.logger.Debug("Finished database scrape", "database", d.Name, "duration", time.Since(dbScrapeStart))
	}()

	if retryAfter := d.IsValid(); retryAfter != nil {
		e.logger.Warn("Invalid database configuration", "database", d.Name, "retry_after", retryAfter)
		errChan <- fmt.Errorf("database %s is invalid, will not be scraped", d.Name)
		return
	}
	if !d.StartupReady() {
		e.logger.Info("Database connection in progress", "database", d.Name)
		errChan <- nil
		return
	}
	if err := d.ping(e.logger, e.MetricsConfiguration.ConnectionBackoff()); err != nil {
		e.logger.Error("Error pinging database", "error", err, "database", d.Name)
		errChan <- err
		return
	}

	// Keep additional queries serial per database. When pooled connections expire
	// together, concurrent queries can otherwise cause an OCI connection stampede.
	for _, metric := range e.metricsToScrape {
		if !isScrapeMetric(e.logger, tick, metric, d) {
			errChan <- nil
			continue
		}
		scrapeStart := time.Now()
		samples, scrapeError := e.ScrapeMetricSamples(d, metric, tick)
		errChan <- scrapeError
		if scrapeError != nil {
			if shouldLogScrapeError(scrapeError, metric.IgnoreZeroResult) {
				e.logger.Error("Error scraping metric",
					"Context", metric.Context,
					"MetricsDesc", fmt.Sprint(metric.MetricsDesc),
					"duration", time.Since(scrapeStart),
					"error", scrapeError,
					"database", d.Name)
			}
			continue
		}
		d.MetricsCache.SetLastScraped(metric, tick)
		sampleCh <- samples
		e.logger.Debug("Successfully scraped metric",
			"Context", metric.Context,
			"MetricDesc", fmt.Sprint(metric.MetricsDesc),
			"duration", time.Since(scrapeStart),
			"samples", len(samples),
			"database", d.Name)
	}

	performanceSamples, err := e.ScrapePerformanceSamples(d, tick)
	errChan <- err
	if err != nil {
		e.logger.Error("Error scraping performance samples", "error", err, "database", d.Name)
	}
	if performanceSamples.Count() > 0 {
		performanceCh <- performanceSamples
	}
	e.logger.Debug("Successfully scraped performance samples",
		"sql_samples", len(performanceSamples.SQL),
		"sql_plan_operations", len(performanceSamples.SQLPlans),
		"session_samples", len(performanceSamples.Sessions),
		"blocking_session_samples", len(performanceSamples.BlockingSessions),
		"database_activity_samples", len(performanceSamples.DatabaseActivity),
		"database", d.Name)
}

func appendPerformanceSamples(performance *PerformanceSamples, databasePerformance PerformanceSamples) {
	performance.SQL = append(performance.SQL, databasePerformance.SQL...)
	performance.SQLDetails = append(performance.SQLDetails, databasePerformance.SQLDetails...)
	performance.SQLTexts = append(performance.SQLTexts, databasePerformance.SQLTexts...)
	performance.SQLPlans = append(performance.SQLPlans, databasePerformance.SQLPlans...)
	performance.Sessions = append(performance.Sessions, databasePerformance.Sessions...)
	performance.BlockingSessions = append(performance.BlockingSessions, databasePerformance.BlockingSessions...)
	performance.DatabaseActivity = append(performance.DatabaseActivity, databasePerformance.DatabaseActivity...)
}

// GetDBs is used by the log scraper to share the database connection
func (e *Scraper) GetDBs() []*Database {
	return e.databases
}

func (e *Scraper) checkIfMetricsChanged() bool {
	for _, definitionFile := range e.MetricDefinitionFiles() {
		if len(definitionFile) == 0 {
			continue
		}
		cleanPath := filepath.Clean(definitionFile)
		e.logger.Debug("Checking metric definition file for changes", "file", definitionFile)
		h := sha256.New()
		if err := hashFile(h, cleanPath); err != nil {
			e.logger.Error("Unable to get file hash; treating metrics file as changed until a reload succeeds", "error", err, "file", cleanPath)
			return true
		}
		sum := h.Sum(nil)
		// If any of files has been changed reload metrics
		if !bytes.Equal(e.metricDefinitionHashes[cleanPath], sum) {
			e.logger.Info("Metric definition file changed; reloading definitions", "file", definitionFile)
			return true
		}
	}
	return false
}

func (e *Scraper) refreshMetricDefinitionHashes() {
	hashes := map[string][]byte{}
	for _, definitionFile := range e.MetricDefinitionFiles() {
		if len(definitionFile) == 0 {
			continue
		}
		cleanPath := filepath.Clean(definitionFile)
		h := sha256.New()
		if err := hashFile(h, cleanPath); err != nil {
			e.logger.Error("Unable to refresh metric definition hash", "error", err, "file", cleanPath)
			continue
		}
		hashes[cleanPath] = h.Sum(nil)
	}
	e.metricDefinitionHashes = hashes
}

func hashFile(h hash.Hash, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	return nil
}

func (e *Scraper) ScrapeMetricSamples(d *Database, m *Metric, tick *time.Time) ([]MetricSample, error) {
	if len(m.Request) == 0 {
		e.logger.Error("Error scraping for "+fmt.Sprint(m.MetricsDesc)+". Did you forget to define request in your toml file?", "database", d.Name)
		return nil, errors.New("scrape request not found")
	}
	if len(m.MetricsDesc) == 0 {
		e.logger.Error("Error scraping for query"+fmt.Sprint(m.Request)+". Did you forget to define metricsdesc in your toml file?", "database", d.Name)
		return nil, errors.New("metricsdesc not found")
	}

	constLabels := d.constLabels(e.constLabels())
	if duplicatedLabels(constLabels, m.GetLabels()) {
		e.logger.Warn("metric has duplicated labels, skipping", "metric", m.ID, "labels", m.GetLabels(), "database", d.Name)
		return nil, nil
	}

	collectedAt := time.Now()
	if tick != nil {
		collectedAt = *tick
	}

	var samples []MetricSample
	parse := func(row map[string]string) error {
		labels := map[string]string{}
		for label, value := range constLabels {
			labels[label] = value
		}
		for _, label := range m.GetLabels() {
			labels[label] = row[label]
		}

		for metric, metricHelp := range m.MetricsDesc {
			value, ok := parseFloat(e.logger, metric, metricHelp, row)
			if !ok {
				continue
			}
			metricType := strings.ToLower(m.MetricsType[metric])
			if metricType == "" {
				metricType = "gauge"
			}
			samples = append(samples, MetricSample{
				CollectedAt: collectedAt,
				Database:    d.Name,
				Context:     m.Context,
				Name:        metricNameSuffix(row, metric, m.FieldToAppend),
				Help:        metricHelp,
				Type:        metricType,
				Value:       value,
				Labels:      copyStringMap(labels),
			})
		}
		return nil
	}

	err := e.scanRows(d, parse, m.Request, getQueryTimeout(e.logger, m, d))
	if err != nil {
		return nil, err
	}
	if !m.IgnoreZeroResult && len(samples) == 0 {
		return nil, newZeroResultError()
	}
	return samples, nil
}

// inspired by https://kylewbanks.com/blog/query-result-to-map-in-golang
// Parse SQL result and call parsing function to each row
func (e *Scraper) scanRows(d *Database, parse func(row map[string]string) error, query string, queryTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	rows, unlock, err := d.QueryContext(ctx, query)

	if ctx.Err() == context.DeadlineExceeded {
		return errors.New("Oracle query timed out")
	}

	if err != nil {
		return err
	}
	defer unlock()
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]string)
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[strings.ToLower(colName)] = fmt.Sprintf("%v", *val)
		}
		// Call function to parse row
		if err := parse(m); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyStringMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (e *Scraper) initCache() {
	for _, d := range e.databases {
		d.initCache(e.metricsToScrape)
	}
}

func duplicatedLabels(constLabels map[string]string, labels []string) bool {
	labelSet := map[string]bool{}
	for k := range constLabels {
		labelSet[k] = true
	}

	for _, label := range labels {
		if labelSet[label] {
			return true
		}
	}

	return false
}

func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.Replace(s, "*", "", -1)  // Remove asterisks
	s = strings.ToLower(s)
	return s
}

func metricNameSuffix(row map[string]string, metric, fieldToAppend string) string {
	if len(fieldToAppend) == 0 {
		return metric
	}
	return cleanName(row[fieldToAppend])
}
