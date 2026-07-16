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

const metricDefinitionFixture = `[[metric]]
context = "additional_instances"
metricsdesc = { value = "Additional test metric." }
request = "select 1 as value from dual"
`

func TestCheckIfMetricsChangedIsTrackedPerScraper(t *testing.T) {
	definitionPath := writeMetricDefinitionFixture(t, metricDefinitionFixture)

	first := newTestScraperWithDefinitions(definitionPath)
	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect initial metric definition load")
	}
	first.reloadMetrics()
	assertMetricLoaded(t, first, "additional_instances_value")

	second := newTestScraperWithDefinitions(definitionPath)
	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect initial metric definition load independently")
	}
	second.reloadMetrics()
	assertMetricLoaded(t, second, "additional_instances_value")
}

func TestCheckIfMetricsChangedReloadsEachScraperAfterFileUpdate(t *testing.T) {
	definitionPath := writeMetricDefinitionFixture(t, metricDefinitionFixture)

	first := newTestScraperWithDefinitions(definitionPath)
	second := newTestScraperWithDefinitions(definitionPath)

	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect initial metric definition load")
	}
	first.reloadMetrics()
	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect initial metric definition load")
	}
	second.reloadMetrics()

	updatedMetrics := `[[metric]]
context = "additional_instances"
metricsdesc = { value = "Updated additional test metric." }
request = "select 2 as value from dual"
`
	if err := os.WriteFile(definitionPath, []byte(updatedMetrics), 0o600); err != nil {
		t.Fatalf("failed to update metric definition fixture: %v", err)
	}

	if !first.checkIfMetricsChanged() {
		t.Fatal("expected first scraper to detect updated metric definitions")
	}
	first.reloadMetrics()
	assertMetricRequest(t, first, "additional_instances_value", "select 2 as value from dual")

	if !second.checkIfMetricsChanged() {
		t.Fatal("expected second scraper to detect updated metric definitions independently")
	}
	second.reloadMetrics()
	assertMetricRequest(t, second, "additional_instances_value", "select 2 as value from dual")
}

func TestReloadMetricsKeepsLastGoodMetricsOnParseError(t *testing.T) {
	definitionPath := writeMetricDefinitionFixture(t, metricDefinitionFixture)
	scraper := newTestScraperWithDefinitions(definitionPath)

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected initial metric definition load to be detected")
	}
	if !scraper.reloadMetrics() {
		t.Fatal("expected initial metrics reload to succeed")
	}
	assertMetricRequest(t, scraper, "additional_instances_value", "select 1 as value from dual")

	invalidMetrics := `[[metric]]
context = "additional_instances"
metricsdesc = { value = "Broken additional test metric."
request = "select 2 as value from dual"
`
	if err := os.WriteFile(definitionPath, []byte(invalidMetrics), 0o600); err != nil {
		t.Fatalf("failed to write invalid metric definition fixture: %v", err)
	}

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected invalid metric definition update to be detected")
	}
	if scraper.reloadMetrics() {
		t.Fatal("expected metrics reload to fail for invalid definitions")
	}
	assertMetricRequest(t, scraper, "additional_instances_value", "select 1 as value from dual")

	if !scraper.checkIfMetricsChanged() {
		t.Fatal("expected invalid definitions to continue appearing changed until a reload succeeds")
	}
}

func newTestScraperWithDefinitions(definitionPath string) *Scraper {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewScraper(logger, &MetricsConfiguration{
		Metrics: MetricsConfig{
			Definitions: []string{definitionPath},
		},
	})
}

func writeMetricDefinitionFixture(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "additional-metrics.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write metric definition fixture: %v", err)
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
