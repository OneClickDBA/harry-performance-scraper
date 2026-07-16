// Copyright (c) 2025, 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"go.yaml.in/yaml/v2"
)

func (e *Scraper) reloadMetrics() bool {
	metricsToScrape, err := e.loadMetricsToScrape()
	if err != nil {
		e.logger.Error("failed to reload metrics; continuing with last known good metrics", "error", err)
		return false
	}

	e.metricsToScrape = metricsToScrape
	e.refreshMetricDefinitionHashes()
	e.initCache()
	return true
}

func (e *Scraper) loadMetricsToScrape() (map[string]*Metric, error) {
	metricsToScrape := map[string]*Metric{}

	if len(e.MetricDefinitionFiles()) == 0 {
		e.logger.Debug("No additional metric definitions configured")
		return metricsToScrape, nil
	}

	for _, definitionFile := range e.MetricDefinitionFiles() {
		if strings.TrimSpace(definitionFile) == "" {
			continue
		}
		metrics := &Metrics{}

		if err := loadMetricsConfig(definitionFile, metrics); err != nil {
			return nil, fmt.Errorf("failed to load metric definitions %s: %w", definitionFile, err)
		}

		e.logger.Info("Successfully loaded additional metric definitions", "file", definitionFile)
		mergeMetrics(metricsToScrape, metrics)
	}

	return metricsToScrape, nil
}

func (e *Scraper) merge(metrics *Metrics) {
	for _, metric := range metrics.Metric {
		e.metricsToScrape[metric.ID] = metric
	}
}

func mergeMetrics(dst map[string]*Metric, metrics *Metrics) {
	for _, metric := range metrics.Metric {
		dst[metric.ID] = metric
	}
}

func loadYamlMetricsConfig(definitionFile string, metrics *Metrics) error {
	yamlBytes, err := os.ReadFile(definitionFile)
	if err != nil {
		return fmt.Errorf("cannot read the metrics config %s: %w", definitionFile, err)
	}
	if err := yaml.Unmarshal(yamlBytes, metrics); err != nil {
		return fmt.Errorf("cannot unmarshal the metrics config %s: %w", definitionFile, err)
	}
	return nil
}

func loadTomlMetricsConfig(definitionFile string, metrics *Metrics) error {
	if _, err := toml.DecodeFile(definitionFile, metrics); err != nil {
		return fmt.Errorf("cannot read the metrics config %s: %w", definitionFile, err)
	}
	return nil
}

func loadMetricsConfig(definitionFile string, metrics *Metrics) error {
	if strings.HasSuffix(definitionFile, "toml") {
		if err := loadTomlMetricsConfig(definitionFile, metrics); err != nil {
			return fmt.Errorf("cannot load toml based metrics: %w", err)
		}
	} else {
		if err := loadYamlMetricsConfig(definitionFile, metrics); err != nil {
			return fmt.Errorf("cannot load yaml based metrics: %w", err)
		}
	}
	metrics.normalizeIdentifiers()
	return nil
}
