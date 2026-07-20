// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

const sessionActivityQuery = `
select
	s.inst_id,
	s.sid,
	s.serial#,
	s.user#,
	s.sql_id,
	s.sql_child_number,
	s.sql_exec_id,
	s.sql_exec_start,
	case when s.state = 'WAITING' then 'WAITING' else 'ON CPU' end,
	case when s.state = 'WAITING' then s.event end,
	case when s.state = 'WAITING' then s.wait_class end,
	case when s.state = 'WAITING' then s.wait_time_micro end,
	case when s.state = 'WAITING' then s.wait_time_micro end,
	s.blocking_session,
	s.blocking_instance,
	s.row_wait_obj#,
	s.row_wait_file#,
	s.row_wait_block#,
	s.program,
	s.module,
	s.action,
	s.machine,
	s.con_id,
	s.service_name,
	s.client_identifier
from gv$session s
where s.type = 'USER'
	and s.status = 'ACTIVE'
	and (s.state <> 'WAITING' or s.wait_class <> 'Idle')
	and s.audsid <> to_number(sys_context('USERENV', 'SESSIONID'))`

const ashActivityQuery = `
select
	ash.sample_id,
	ash.sample_time,
	ash.inst_id,
	ash.session_id,
	ash.session_serial#,
	ash.session_type,
	ash.user_id,
	ash.sql_id,
	ash.sql_child_number,
	ash.sql_exec_id,
	ash.sql_exec_start,
	ash.top_level_sql_id,
	ash.session_state,
	ash.event,
	ash.wait_class,
	ash.wait_time,
	ash.time_waited,
	ash.blocking_session,
	ash.blocking_session_serial#,
	ash.blocking_inst_id,
	ash.current_obj#,
	ash.current_file#,
	ash.current_block#,
	ash.program,
	ash.module,
	ash.action,
	ash.machine,
	ash.pga_allocated,
	ash.temp_space_allocated,
	ash.con_id,
	ash.usecs_per_row,
	ash.sql_plan_hash_value,
	ash.sql_full_plan_hash_value,
	ash.sql_plan_line_id,
	ash.service_hash,
	ash.client_id
from gv$active_session_history ash
where ash.sample_time > :1
	and ash.session_type = 'FOREGROUND'
order by ash.sample_time`

// RunActivitySampling collects only active-session observations on a short,
// independent cadence. Slower SQL and plan collectors cannot delay this loop.
func (e *Scraper) RunActivitySampling(ctx context.Context, sink SampleSink) {
	interval := e.Performance.Activity.GetInterval()
	e.logger.Info("Starting database activity sampling",
		"source", e.Performance.Activity.GetSource(),
		"interval", interval,
		"query_timeout", e.Performance.Activity.GetQueryTimeout())

	e.sampleActivity(ctx, sink, time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case tick := <-ticker.C:
			e.sampleActivity(ctx, sink, tick)
		case <-ctx.Done():
			return
		}
	}
}

func (e *Scraper) sampleActivity(ctx context.Context, sink SampleSink, sampledAt time.Time) {
	type result struct {
		database string
		samples  []DatabaseActivitySample
		err      error
	}

	results := make(chan result, len(e.databases))
	var wg sync.WaitGroup
	for _, database := range e.databases {
		database := database
		if database.IsValid() != nil || !database.StartupReady() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			samples, err := e.scrapeActivitySamples(database, sampledAt)
			results <- result{database: database.Name, samples: samples, err: err}
		}()
	}
	wg.Wait()
	close(results)

	performance := PerformanceSamples{}
	latestASH := map[string]time.Time{}
	totalErrors := 0
	for result := range results {
		if result.err != nil {
			totalErrors++
			e.logger.Error("Error scraping database activity",
				"database", result.database,
				"source", e.Performance.Activity.GetSource(),
				"error", result.err)
			continue
		}
		performance.DatabaseActivity = append(performance.DatabaseActivity, result.samples...)
		for _, sample := range result.samples {
			if sample.SampleSource == "ASH" && sample.SampleTime.After(latestASH[result.database]) {
				latestASH[result.database] = sample.SampleTime
			}
		}
	}
	if len(performance.DatabaseActivity) == 0 {
		return
	}

	startedAt := time.Now()
	summary := ScrapeSummary{StartedAt: startedAt, TotalErrors: totalErrors}
	if err := sink.WriteSamples(ctx, nil, performance, summary); err != nil {
		e.logger.Error("Failed to write database activity samples", "error", err)
		return
	}
	if len(latestASH) > 0 {
		e.activityWatermarkMu.Lock()
		for database, watermark := range latestASH {
			e.activityWatermarks[database] = watermark
		}
		e.activityWatermarkMu.Unlock()
	}
}

func (e *Scraper) scrapeActivitySamples(d *Database, sampledAt time.Time) ([]DatabaseActivitySample, error) {
	switch e.Performance.Activity.GetSource() {
	case "ash":
		return e.scrapeASHSamples(d, sampledAt)
	default:
		return e.scrapeSessionActivitySamples(d, sampledAt)
	}
}

func (e *Scraper) scrapeSessionActivitySamples(d *Database, sampledAt time.Time) ([]DatabaseActivitySample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.Performance.Activity.GetQueryTimeout())
	defer cancel()
	rows, unlock, err := d.QueryContext(ctx, sessionActivityQuery)
	if err != nil {
		return nil, activityQueryError(ctx, err)
	}
	defer unlock()
	defer rows.Close()

	sampleID := sampledAt.UnixNano()
	durationMicro := e.Performance.Activity.GetInterval().Microseconds()
	var samples []DatabaseActivitySample
	for rows.Next() {
		var sample DatabaseActivitySample
		var serialNumber, userID, sqlChildNumber, sqlExecID sql.NullInt64
		var waitTime, timeWaited, blockingSession, blockingInstID sql.NullInt64
		var objectID, fileNumber, blockNumber, conID sql.NullInt64
		var sqlID, sessionState, event, waitClass sql.NullString
		var program, module, action, machine, serviceName, clientID sql.NullString
		var sqlExecStart sql.NullTime
		if err := rows.Scan(
			&sample.InstID, &sample.SessionID, &serialNumber, &userID,
			&sqlID, &sqlChildNumber, &sqlExecID, &sqlExecStart,
			&sessionState, &event, &waitClass, &waitTime, &timeWaited,
			&blockingSession, &blockingInstID, &objectID, &fileNumber, &blockNumber,
			&program, &module, &action, &machine, &conID, &serviceName, &clientID,
		); err != nil {
			return nil, err
		}
		sample.CollectedAt = sampledAt
		sample.Database = d.Name
		sample.SampleID = sampleID
		sample.SampleTime = sampledAt
		sample.SessionSerialNumber = int64Ptr(serialNumber)
		sample.SessionType = ptrTo("FOREGROUND")
		sample.UserID = int64Ptr(userID)
		sample.SQLID = stringPtr(sqlID)
		sample.SQLChildNumber = int64Ptr(sqlChildNumber)
		sample.SQLExecID = int64Ptr(sqlExecID)
		sample.SQLExecStart = timePtr(sqlExecStart)
		sample.SessionState = stringPtr(sessionState)
		sample.Event = stringPtr(event)
		sample.WaitClass = stringPtr(waitClass)
		sample.WaitTimeMicro = int64Ptr(waitTime)
		sample.TimeWaitedMicro = int64Ptr(timeWaited)
		sample.BlockingSession = int64Ptr(blockingSession)
		sample.BlockingInstID = int64Ptr(blockingInstID)
		sample.CurrentObjectID = int64Ptr(objectID)
		sample.CurrentFileNumber = int64Ptr(fileNumber)
		sample.CurrentBlockNumber = int64Ptr(blockNumber)
		sample.Program = stringPtr(program)
		sample.Module = stringPtr(module)
		sample.Action = stringPtr(action)
		sample.Machine = stringPtr(machine)
		sample.ConID = int64Ptr(conID)
		sample.ServiceName = stringPtr(serviceName)
		sample.ClientIdentifier = stringPtr(clientID)
		sample.SampleSource = "SESSION"
		sample.SampleDurationMicro = durationMicro
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func (e *Scraper) scrapeASHSamples(d *Database, sampledAt time.Time) ([]DatabaseActivitySample, error) {
	e.activityWatermarkMu.Lock()
	since, ok := e.activityWatermarks[d.Name]
	e.activityWatermarkMu.Unlock()
	if !ok {
		since = sampledAt.Add(-2 * time.Minute)
	} else {
		since = since.Add(-e.Performance.Activity.GetInterval())
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.Performance.Activity.GetQueryTimeout())
	defer cancel()
	rows, unlock, err := d.QueryContext(ctx, ashActivityQuery, since)
	if err != nil {
		return nil, activityQueryError(ctx, err)
	}
	defer unlock()
	defer rows.Close()

	var samples []DatabaseActivitySample
	for rows.Next() {
		var sample DatabaseActivitySample
		var serialNumber, userID, sqlChildNumber, sqlExecID sql.NullInt64
		var waitTime, timeWaited, blockingSession, blockingSerial, blockingInstID sql.NullInt64
		var objectID, fileNumber, blockNumber, pga, temp, conID, usecs sql.NullInt64
		var planHash, fullPlanHash, planLineID, serviceHash sql.NullInt64
		var sessionType, sqlID, topSQLID, sessionState, event, waitClass sql.NullString
		var program, module, action, machine, clientID sql.NullString
		var sqlExecStart sql.NullTime
		if err := rows.Scan(
			&sample.SampleID, &sample.SampleTime, &sample.InstID, &sample.SessionID,
			&serialNumber, &sessionType, &userID, &sqlID, &sqlChildNumber,
			&sqlExecID, &sqlExecStart, &topSQLID, &sessionState, &event, &waitClass,
			&waitTime, &timeWaited, &blockingSession, &blockingSerial, &blockingInstID,
			&objectID, &fileNumber, &blockNumber, &program, &module, &action, &machine,
			&pga, &temp, &conID, &usecs, &planHash, &fullPlanHash, &planLineID,
			&serviceHash, &clientID,
		); err != nil {
			return nil, err
		}
		sample.CollectedAt = sampledAt
		sample.Database = d.Name
		sample.SessionSerialNumber = int64Ptr(serialNumber)
		sample.SessionType = stringPtr(sessionType)
		sample.UserID = int64Ptr(userID)
		sample.SQLID = stringPtr(sqlID)
		sample.SQLChildNumber = int64Ptr(sqlChildNumber)
		sample.SQLExecID = int64Ptr(sqlExecID)
		sample.SQLExecStart = timePtr(sqlExecStart)
		sample.TopLevelSQLID = stringPtr(topSQLID)
		sample.SessionState = stringPtr(sessionState)
		sample.Event = stringPtr(event)
		sample.WaitClass = stringPtr(waitClass)
		sample.WaitTimeMicro = int64Ptr(waitTime)
		sample.TimeWaitedMicro = int64Ptr(timeWaited)
		sample.BlockingSession = int64Ptr(blockingSession)
		sample.BlockingSessionSerial = int64Ptr(blockingSerial)
		sample.BlockingInstID = int64Ptr(blockingInstID)
		sample.CurrentObjectID = int64Ptr(objectID)
		sample.CurrentFileNumber = int64Ptr(fileNumber)
		sample.CurrentBlockNumber = int64Ptr(blockNumber)
		sample.Program = stringPtr(program)
		sample.Module = stringPtr(module)
		sample.Action = stringPtr(action)
		sample.Machine = stringPtr(machine)
		sample.PGAAllocated = int64Ptr(pga)
		sample.TempSpaceAllocated = int64Ptr(temp)
		sample.ConID = int64Ptr(conID)
		sample.SampleDurationMicro = nullableDuration(usecs, e.Performance.Activity.GetInterval())
		sample.SQLPlanHashValue = int64Ptr(planHash)
		sample.SQLFullPlanHashValue = int64Ptr(fullPlanHash)
		sample.SQLPlanLineID = int64Ptr(planLineID)
		sample.ServiceHash = int64Ptr(serviceHash)
		sample.ClientIdentifier = stringPtr(clientID)
		sample.SampleSource = "ASH"
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func activityQueryError(ctx context.Context, err error) error {
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("Oracle activity query timed out: %w", ctx.Err())
	}
	return err
}

func nullableDuration(value sql.NullInt64, fallback time.Duration) int64 {
	if value.Valid && value.Int64 > 0 {
		return value.Int64
	}
	return fallback.Microseconds()
}

func ptrTo(value string) *string {
	return &value
}
