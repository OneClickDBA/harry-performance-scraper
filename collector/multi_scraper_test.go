// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

const customMetricFixture = `[[metric]]
context = "custom_instances"
metricsdesc = { value = "Custom test metric." }
request = "select 1 as value from dual"
`

func TestCheckIfMetricsChangedIsTrackedPerScraper(t *testing.T) {
	customMetricsPath := writeCustomMetricsFixture(t, customMetricFixture)

	first := newTestScraperWithCustomMetrics(customMetricsPath)
	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect initial custom metrics load")
	}
	first.reloadMetrics()
	assertMetricLoaded(t, first, "custom_instances_value")

	second := newTestScraperWithCustomMetrics(customMetricsPath)
	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect initial custom metrics load independently")
	}
	second.reloadMetrics()
	assertMetricLoaded(t, second, "custom_instances_value")
}

func TestCheckIfMetricsChangedReloadsEachScraperAfterFileUpdate(t *testing.T) {
	customMetricsPath := writeCustomMetricsFixture(t, customMetricFixture)

	first := newTestScraperWithCustomMetrics(customMetricsPath)
	second := newTestScraperWithCustomMetrics(customMetricsPath)

	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect initial custom metrics load")
	}
	first.reloadMetrics()
	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect initial custom metrics load")
	}
	second.reloadMetrics()

	updatedMetrics := `[[metric]]
context = "custom_instances"
metricsdesc = { value = "Updated custom test metric." }
request = "select 2 as value from dual"
`
	if err := os.WriteFile(customMetricsPath, []byte(updatedMetrics), 0o600); err != nil {
		t.Fatalf("failed to update custom metrics fixture: %v", err)
	}

	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect updated custom metrics")
	}
	first.reloadMetrics()
	assertMetricRequest(t, first, "custom_instances_value", "select 2 as value from dual")

	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect updated custom metrics independently")
	}
	second.reloadMetrics()
	assertMetricRequest(t, second, "custom_instances_value", "select 2 as value from dual")
}

func TestReloadMetricsKeepsLastGoodMetricsOnParseError(t *testing.T) {
	customMetricsPath := writeCustomMetricsFixture(t, customMetricFixture)
	scraper := newTestScraperWithCustomMetrics(customMetricsPath)

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected initial custom metrics load to be detected")
	}
	if !scraper.reloadMetrics() {
		t.Fatal("expected initial metrics reload to succeed")
	}
	assertMetricRequest(t, scraper, "custom_instances_value", "select 1 as value from dual")

	invalidMetrics := `[[metric]]
context = "custom_instances"
metricsdesc = { value = "Broken custom test metric."
request = "select 2 as value from dual"
`
	if err := os.WriteFile(customMetricsPath, []byte(invalidMetrics), 0o600); err != nil {
		t.Fatalf("failed to write invalid custom metrics fixture: %v", err)
	}

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected invalid custom metrics update to be detected")
	}
	if scraper.reloadMetrics() {
		t.Fatal("expected metrics reload to fail for invalid custom metrics")
	}
	assertMetricRequest(t, scraper, "custom_instances_value", "select 1 as value from dual")

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected invalid custom metrics to continue appearing changed until a reload succeeds")
	}
}

func newTestScraperWithCustomMetrics(customMetricsPath string) *Scraper {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewScraper(logger, &MetricsConfiguration{
		Metrics: MetricsFilesConfig{
			Custom: []string{customMetricsPath},
		},
	})
}

func writeCustomMetricsFixture(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "custom-metrics.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write custom metrics fixture: %v", err)
	}
	return path
}

func assertMetricLoaded(t *testing.T, scraper *Scraper, metricID string) {
	t.Helper()

	metric := scraper.metricsToScrape[metricID]
	if metric == nil {
		t.Fatalf("expected metric %q to be loaded", metricID)
	}
}

func assertMetricRequest(t *testing.T, scraper *Scraper, metricID, want string) {
	t.Helper()

	metric := scraper.metricsToScrape[metricID]
	if metric == nil {
		t.Fatalf("expected metric %q to be loaded", metricID)
	}
	if metric.Request != want {
		t.Fatalf("expected metric %q request %q, got %q", metricID, want, metric.Request)
	}
}
