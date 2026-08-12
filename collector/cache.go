// Copyright (c) 2025, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import "time"

func NewMetricsCache(metrics map[string]*Metric) *MetricsCache {
	c := map[*Metric]*MetricCacheRecord{}

	for _, metric := range metrics {
		c[metric] = &MetricCacheRecord{
			LastScraped: nil,
		}
	}
	return &MetricsCache{
		cache: c,
	}
}

func (c *MetricsCache) SetLastScraped(m *Metric, tick *time.Time) {
	c.cache[m].LastScraped = tick
}

func (c *MetricsCache) GetLastScraped(m *Metric) *time.Time {
	return c.cache[m].LastScraped
}
