// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"time"
)

// RunDeadConnectionDetection serially probes quarantined databases. It is the
// sole runtime owner of connection reconstruction and pool warmup.
func (e *Scraper) RunDeadConnectionDetection(ctx context.Context, sink SampleSink) {
	config := e.Metrics.DeadConnectionDetection
	if !config.GetEnabled() {
		e.logger.Info("Dead connection detection is disabled")
		return
	}

	interval := config.GetInterval()
	e.logger.Info("Starting dead connection detection",
		"interval", interval,
		"probe_timeout", config.GetProbeTimeout())
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case scheduledAt := <-timer.C:
			e.runDeadConnectionDetection(ctx, sink, scheduledAt)
			if ctx.Err() != nil {
				return
			}
			// Schedule from completion so a slow serialized pass cannot create a
			// catch-up burst of connection attempts.
			timer.Reset(interval)
		case <-ctx.Done():
			return
		}
	}
}

func (e *Scraper) runDeadConnectionDetection(ctx context.Context, sink SampleSink, scheduledAt time.Time) {
	startedAt := time.Now()
	statuses := make([]ScrapeStatusSample, 0)
	attempted := 0
	recovered := 0
	errorsCount := 0

	for _, database := range e.databases {
		if err := ctx.Err(); err != nil {
			return
		}
		probeStarted := time.Now()
		if !database.beginProbe(probeStarted) {
			continue
		}
		attempted++
		e.logger.Info("DCD probing quarantined database",
			"database", database.Name,
			"attempt", database.connectionStateSample(probeStarted, e.instanceName).ProbeAttempts)

		err := e.recoverDatabase(database)
		database.finishProbe(probeStarted, err)
		statuses = append(statuses, newScrapeStatus(scheduledAt, database.Name,
			"dead_connection_detection", probeStarted, 0, err))
		if err != nil {
			errorsCount++
			e.logger.Warn("DCD database probe failed",
				"database", database.Name,
				"duration", time.Since(probeStarted),
				"error", err)
			continue
		}
		recovered++
		e.logger.Info("DCD returned database to active collection",
			"database", database.Name,
			"duration", time.Since(probeStarted))
	}

	finishedAt := time.Now()
	missed := int64(finishedAt.Sub(startedAt) / e.Metrics.DeadConnectionDetection.GetInterval())
	queryTimeoutSeconds := e.Metrics.DeadConnectionDetection.GetProbeTimeout().Seconds()
	batch := SampleBatch{
		ScrapeStatuses:           statuses,
		ConnectionStates:         e.connectionStateSamples(finishedAt),
		ConnectionStatesComplete: true,
		Runtime: &RuntimeSample{
			Scheduler:                     "dead_connection_detection",
			Trigger:                       "ticker",
			ScraperInstance:               e.instanceName,
			HAScope:                       e.HighAvailability.GetScope(),
			ScheduledAt:                   scheduledAt,
			StartedAt:                     startedAt,
			CollectionFinishedAt:          finishedAt,
			ConfiguredIntervalSeconds:     e.Metrics.DeadConnectionDetection.GetInterval().Seconds(),
			ConfiguredQueryTimeoutSeconds: &queryTimeoutSeconds,
			SchedulingLagSeconds:          nonNegativeDuration(startedAt.Sub(scheduledAt)).Seconds(),
			CollectionDurationSeconds:     finishedAt.Sub(startedAt).Seconds(),
			MissedIntervals:               missed,
			DatabasesAttempted:            attempted,
			DatabasesSucceeded:            recovered,
			SampleCount:                   len(statuses),
			ErrorCount:                    errorsCount,
		},
	}
	summary := ScrapeSummary{
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		DurationSeconds: finishedAt.Sub(startedAt).Seconds(),
		TotalErrors:     errorsCount,
		SampleCount:     len(statuses),
	}
	if err := sink.WriteSamples(ctx, batch, summary); err != nil {
		e.logger.Error("Failed to write dead connection detection status", "error", err)
	}
}

func (e *Scraper) recoverDatabase(database *Database) error {
	probeTimeout := e.Metrics.DeadConnectionDetection.GetProbeTimeout()
	if e.reconnectDatabase != nil {
		return e.reconnectDatabase(database, probeTimeout)
	}
	return database.reconnect(e.logger, probeTimeout)
}

func nonNegativeDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}
