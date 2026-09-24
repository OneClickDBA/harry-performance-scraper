// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import "time"

func missedScheduledIntervals(previous, current time.Time, interval time.Duration) int64 {
	if previous.IsZero() || interval <= 0 || !current.After(previous) {
		return 0
	}
	periods := int64(current.Sub(previous) / interval)
	if periods <= 1 {
		return 0
	}
	return periods - 1
}

func (e *Scraper) newRuntimeSample(
	scheduler string,
	trigger string,
	scheduledAt time.Time,
	startedAt time.Time,
	collectionFinishedAt time.Time,
	interval time.Duration,
	queryTimeout *time.Duration,
	missed int64,
	statuses []ScrapeStatusSample,
	completionCollector string,
	sampleCount int,
	errorCount int,
) *RuntimeSample {
	lag := startedAt.Sub(scheduledAt)
	if lag < 0 {
		lag = 0
	}
	if interval > 0 {
		if delayedIntervals := int64(lag / interval); delayedIntervals > missed {
			missed = delayedIntervals
		}
	}

	succeeded := 0
	for _, status := range statuses {
		if status.Collector == completionCollector && status.Success {
			succeeded++
		}
	}
	if unsuccessful := len(e.databases) - succeeded; unsuccessful > errorCount {
		errorCount = unsuccessful
	}

	var timeoutSeconds *float64
	if queryTimeout != nil {
		seconds := queryTimeout.Seconds()
		timeoutSeconds = &seconds
	}

	return &RuntimeSample{
		Scheduler:                     scheduler,
		Trigger:                       trigger,
		ScraperInstance:               e.instanceName,
		HAScope:                       e.HighAvailability.GetScope(),
		ScheduledAt:                   scheduledAt,
		StartedAt:                     startedAt,
		CollectionFinishedAt:          collectionFinishedAt,
		ConfiguredIntervalSeconds:     interval.Seconds(),
		ConfiguredQueryTimeoutSeconds: timeoutSeconds,
		SchedulingLagSeconds:          lag.Seconds(),
		CollectionDurationSeconds:     collectionFinishedAt.Sub(startedAt).Seconds(),
		MissedIntervals:               missed,
		DatabasesAttempted:            len(e.databases),
		DatabasesSucceeded:            succeeded,
		SampleCount:                   sampleCount,
		ErrorCount:                    errorCount,
	}
}
