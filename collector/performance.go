// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const topSQLLimit = 100

const sqlPerformanceQuery = `
select
	q.inst_id,
	q.sql_id,
	q.child_number,
	q.plan_hash_value,
	q.parsing_schema_name,
	q.module,
	q.executions,
	q.elapsed_time,
	q.cpu_time,
	q.user_io_wait_time,
	q.application_wait_time,
	q.concurrency_wait_time,
	q.cluster_wait_time,
	q.buffer_gets,
	q.disk_reads,
	q.direct_writes,
	q.rows_processed,
	q.fetches,
	q.loads,
	q.invalidations,
	q.parse_calls,
	q.last_active_time,
	q.sql_text
from gv$sql q
where q.sql_id is not null
order by q.elapsed_time desc
fetch first 100 rows only`

const sessionPerformanceQuery = `
select
	s.inst_id,
	s.sid,
	s.serial# as serial_number,
	s.username,
	s.status,
	s.sql_id,
	s.sql_child_number,
	s.prev_sql_id,
	s.event,
	s.wait_class,
	s.state,
	s.seconds_in_wait,
	s.blocking_instance,
	s.blocking_session,
	s.machine,
	s.program,
	s.module,
	s.action,
	s.service_name,
	s.logon_time
from gv$session s
where s.type = 'USER'`

const blockingSessionPerformanceQuery = `
select
	s.inst_id,
	s.sid,
	s.serial# as serial_number,
	s.username,
	s.sql_id,
	s.event,
	s.wait_class,
	s.blocking_instance,
	s.blocking_session,
	bs.username as blocking_username,
	bs.sql_id as blocking_sql_id,
	bs.event as blocking_event
from gv$session s
left join gv$session bs
	on bs.inst_id = s.blocking_instance
	and bs.sid = s.blocking_session
where s.blocking_session is not null`

const databaseActivityHistoryQuery = `
select
	ash.sample_id,
	ash.sample_time,
	ash.inst_id,
	ash.session_id,
	ash.session_serial# as session_serial_number,
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
	ash.blocking_session_serial# as blocking_session_serial_number,
	ash.blocking_inst_id,
	ash.current_obj# as current_object_id,
	ash.current_file# as current_file_number,
	ash.current_block# as current_block_number,
	ash.program,
	ash.module,
	ash.action,
	ash.machine,
	ash.pga_allocated,
	ash.temp_space_allocated,
	ash.con_id
from gv$active_session_history ash
where ash.sample_time >= systimestamp - interval '2' minute
	and ash.session_type = 'FOREGROUND'
order by ash.sample_time`

func (e *Scraper) ScrapePerformanceSamples(d *Database, tick *time.Time) (PerformanceSamples, error) {
	collectedAt := time.Now()
	if tick != nil {
		collectedAt = *tick
	}

	timeout := time.Duration(d.Config.GetQueryTimeout()) * time.Second
	sqlSamples, err := e.scrapeSQLSamples(d, collectedAt, timeout)
	if err != nil {
		err = fmt.Errorf("scrape sql performance: %w", err)
	}
	sessionSamples, sessionErr := e.scrapeSessionSamples(d, collectedAt, timeout)
	if sessionErr != nil {
		sessionErr = fmt.Errorf("scrape session performance: %w", sessionErr)
	}
	blockingSamples, blockingErr := e.scrapeBlockingSessionSamples(d, collectedAt, timeout)
	if blockingErr != nil {
		blockingErr = fmt.Errorf("scrape blocking sessions: %w", blockingErr)
	}
	databaseActivitySamples, databaseActivityErr := e.scrapeDatabaseActivitySamples(d, collectedAt, timeout)
	if databaseActivityErr != nil {
		databaseActivityErr = fmt.Errorf("scrape database activity history: %w", databaseActivityErr)
	}
	ashSamples := len(databaseActivitySamples)
	sessionDerivedActivitySamples := 0
	if len(databaseActivitySamples) == 0 && len(sessionSamples) > 0 {
		databaseActivitySamples = databaseActivitySamplesFromSessions(sessionSamples, collectedAt)
		sessionDerivedActivitySamples = len(databaseActivitySamples)
	}
	e.logger.Info("Scraped database activity samples",
		"database", d.Name,
		"ash_samples", ashSamples,
		"session_samples", len(sessionSamples),
		"session_derived_database_activity_samples", sessionDerivedActivitySamples,
		"database_activity_samples", len(databaseActivitySamples),
	)
	return PerformanceSamples{
		SQL:              sqlSamples,
		Sessions:         sessionSamples,
		BlockingSessions: blockingSamples,
		DatabaseActivity: databaseActivitySamples,
	}, errors.Join(err, sessionErr, blockingErr, databaseActivityErr)
}

func (e *Scraper) scrapeSQLSamples(d *Database, collectedAt time.Time, timeout time.Duration) ([]SQLSample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, unlock, err := d.QueryContext(ctx, sqlPerformanceQuery)
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("Oracle query timed out")
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var samples []SQLSample
	for rows.Next() {
		var sample SQLSample
		var childNumber, planHashValue, executions, elapsedTime, cpuTime sql.NullInt64
		var userIOWait, applicationWait, concurrencyWait, clusterWait sql.NullInt64
		var bufferGets, diskReads, directWrites, rowsProcessed, fetches sql.NullInt64
		var loads, invalidations, parseCalls sql.NullInt64
		var parsingSchema, module, sqlText sql.NullString
		var lastActiveTime sql.NullTime

		if err := rows.Scan(
			&sample.InstID,
			&sample.SQLID,
			&childNumber,
			&planHashValue,
			&parsingSchema,
			&module,
			&executions,
			&elapsedTime,
			&cpuTime,
			&userIOWait,
			&applicationWait,
			&concurrencyWait,
			&clusterWait,
			&bufferGets,
			&diskReads,
			&directWrites,
			&rowsProcessed,
			&fetches,
			&loads,
			&invalidations,
			&parseCalls,
			&lastActiveTime,
			&sqlText,
		); err != nil {
			return nil, err
		}

		sample.CollectedAt = collectedAt
		sample.Database = d.Name
		sample.ChildNumber = int64Ptr(childNumber)
		sample.PlanHashValue = int64Ptr(planHashValue)
		sample.ParsingSchemaName = stringPtr(parsingSchema)
		sample.Module = stringPtr(module)
		sample.Executions = int64Ptr(executions)
		sample.ElapsedTimeMicro = int64Ptr(elapsedTime)
		sample.CPUTimeMicro = int64Ptr(cpuTime)
		sample.UserIOWaitMicro = int64Ptr(userIOWait)
		sample.ApplicationWaitMicro = int64Ptr(applicationWait)
		sample.ConcurrencyWaitMicro = int64Ptr(concurrencyWait)
		sample.ClusterWaitMicro = int64Ptr(clusterWait)
		sample.BufferGets = int64Ptr(bufferGets)
		sample.DiskReads = int64Ptr(diskReads)
		sample.DirectWrites = int64Ptr(directWrites)
		sample.RowsProcessed = int64Ptr(rowsProcessed)
		sample.Fetches = int64Ptr(fetches)
		sample.Loads = int64Ptr(loads)
		sample.Invalidations = int64Ptr(invalidations)
		sample.ParseCalls = int64Ptr(parseCalls)
		sample.LastActiveTime = timePtr(lastActiveTime)
		sample.SQLText = stringPtr(sqlText)
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func (e *Scraper) scrapeSessionSamples(d *Database, collectedAt time.Time, timeout time.Duration) ([]SessionSample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, unlock, err := d.QueryContext(ctx, sessionPerformanceQuery)
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("Oracle query timed out")
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var samples []SessionSample
	for rows.Next() {
		var sample SessionSample
		var serialNumber, sqlChildNumber, secondsInWait, blockingInstance, blockingSession sql.NullInt64
		var username, status, sqlID, prevSQLID, event, waitClass, state sql.NullString
		var machine, program, module, action, serviceName sql.NullString
		var logonTime sql.NullTime

		if err := rows.Scan(
			&sample.InstID,
			&sample.SID,
			&serialNumber,
			&username,
			&status,
			&sqlID,
			&sqlChildNumber,
			&prevSQLID,
			&event,
			&waitClass,
			&state,
			&secondsInWait,
			&blockingInstance,
			&blockingSession,
			&machine,
			&program,
			&module,
			&action,
			&serviceName,
			&logonTime,
		); err != nil {
			return nil, err
		}

		sample.CollectedAt = collectedAt
		sample.Database = d.Name
		sample.SerialNumber = int64Ptr(serialNumber)
		sample.Username = stringPtr(username)
		sample.Status = stringPtr(status)
		sample.SQLID = stringPtr(sqlID)
		sample.SQLChildNumber = int64Ptr(sqlChildNumber)
		sample.PrevSQLID = stringPtr(prevSQLID)
		sample.Event = stringPtr(event)
		sample.WaitClass = stringPtr(waitClass)
		sample.State = stringPtr(state)
		sample.SecondsInWait = int64Ptr(secondsInWait)
		sample.BlockingInstance = int64Ptr(blockingInstance)
		sample.BlockingSession = int64Ptr(blockingSession)
		sample.Machine = stringPtr(machine)
		sample.Program = stringPtr(program)
		sample.Module = stringPtr(module)
		sample.Action = stringPtr(action)
		sample.ServiceName = stringPtr(serviceName)
		sample.LogonTime = timePtr(logonTime)
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func (e *Scraper) scrapeBlockingSessionSamples(d *Database, collectedAt time.Time, timeout time.Duration) ([]BlockingSessionSample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, unlock, err := d.QueryContext(ctx, blockingSessionPerformanceQuery)
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("Oracle query timed out")
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var samples []BlockingSessionSample
	for rows.Next() {
		var sample BlockingSessionSample
		var serialNumber, blockingInstance, blockingSession sql.NullInt64
		var username, sqlID, event, waitClass, blockingUsername, blockingSQLID, blockingEvent sql.NullString

		if err := rows.Scan(
			&sample.InstID,
			&sample.SID,
			&serialNumber,
			&username,
			&sqlID,
			&event,
			&waitClass,
			&blockingInstance,
			&blockingSession,
			&blockingUsername,
			&blockingSQLID,
			&blockingEvent,
		); err != nil {
			return nil, err
		}

		sample.CollectedAt = collectedAt
		sample.Database = d.Name
		sample.SerialNumber = int64Ptr(serialNumber)
		sample.Username = stringPtr(username)
		sample.SQLID = stringPtr(sqlID)
		sample.Event = stringPtr(event)
		sample.WaitClass = stringPtr(waitClass)
		sample.BlockingInstance = int64Ptr(blockingInstance)
		sample.BlockingSession = int64Ptr(blockingSession)
		sample.BlockingUsername = stringPtr(blockingUsername)
		sample.BlockingSQLID = stringPtr(blockingSQLID)
		sample.BlockingEvent = stringPtr(blockingEvent)
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func (e *Scraper) scrapeDatabaseActivitySamples(d *Database, collectedAt time.Time, timeout time.Duration) ([]DatabaseActivitySample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, unlock, err := d.QueryContext(ctx, databaseActivityHistoryQuery)
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("Oracle query timed out")
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var samples []DatabaseActivitySample
	for rows.Next() {
		var sample DatabaseActivitySample
		var sessionSerialNumber, userID, sqlChildNumber, sqlExecID sql.NullInt64
		var waitTimeMicro, timeWaitedMicro, blockingSession, blockingSessionSerial sql.NullInt64
		var blockingInstID, currentObjectID, currentFileNumber, currentBlockNumber sql.NullInt64
		var pgaAllocated, tempSpaceAllocated, conID sql.NullInt64
		var sessionType, sqlID, topLevelSQLID, sessionState, event, waitClass sql.NullString
		var program, module, action, machine sql.NullString
		var sqlExecStart sql.NullTime

		if err := rows.Scan(
			&sample.SampleID,
			&sample.SampleTime,
			&sample.InstID,
			&sample.SessionID,
			&sessionSerialNumber,
			&sessionType,
			&userID,
			&sqlID,
			&sqlChildNumber,
			&sqlExecID,
			&sqlExecStart,
			&topLevelSQLID,
			&sessionState,
			&event,
			&waitClass,
			&waitTimeMicro,
			&timeWaitedMicro,
			&blockingSession,
			&blockingSessionSerial,
			&blockingInstID,
			&currentObjectID,
			&currentFileNumber,
			&currentBlockNumber,
			&program,
			&module,
			&action,
			&machine,
			&pgaAllocated,
			&tempSpaceAllocated,
			&conID,
		); err != nil {
			return nil, err
		}

		sample.CollectedAt = collectedAt
		sample.Database = d.Name
		sample.SessionSerialNumber = int64Ptr(sessionSerialNumber)
		sample.SessionType = stringPtr(sessionType)
		sample.UserID = int64Ptr(userID)
		sample.SQLID = stringPtr(sqlID)
		sample.SQLChildNumber = int64Ptr(sqlChildNumber)
		sample.SQLExecID = int64Ptr(sqlExecID)
		sample.SQLExecStart = timePtr(sqlExecStart)
		sample.TopLevelSQLID = stringPtr(topLevelSQLID)
		sample.SessionState = stringPtr(sessionState)
		sample.Event = stringPtr(event)
		sample.WaitClass = stringPtr(waitClass)
		sample.WaitTimeMicro = int64Ptr(waitTimeMicro)
		sample.TimeWaitedMicro = int64Ptr(timeWaitedMicro)
		sample.BlockingSession = int64Ptr(blockingSession)
		sample.BlockingSessionSerial = int64Ptr(blockingSessionSerial)
		sample.BlockingInstID = int64Ptr(blockingInstID)
		sample.CurrentObjectID = int64Ptr(currentObjectID)
		sample.CurrentFileNumber = int64Ptr(currentFileNumber)
		sample.CurrentBlockNumber = int64Ptr(currentBlockNumber)
		sample.Program = stringPtr(program)
		sample.Module = stringPtr(module)
		sample.Action = stringPtr(action)
		sample.Machine = stringPtr(machine)
		sample.PGAAllocated = int64Ptr(pgaAllocated)
		sample.TempSpaceAllocated = int64Ptr(tempSpaceAllocated)
		sample.ConID = int64Ptr(conID)
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func databaseActivitySamplesFromSessions(sessionSamples []SessionSample, collectedAt time.Time) []DatabaseActivitySample {
	samples := make([]DatabaseActivitySample, 0, len(sessionSamples))
	sampleID := collectedAt.UnixNano()
	for _, session := range sessionSamples {
		if !sessionLooksActiveForDatabaseActivity(session) {
			continue
		}
		sample := DatabaseActivitySample{
			CollectedAt:         collectedAt,
			Database:            session.Database,
			SampleID:            sampleID,
			SampleTime:          collectedAt,
			InstID:              session.InstID,
			SessionID:           session.SID,
			SessionSerialNumber: session.SerialNumber,
			SQLID:               session.SQLID,
			SQLChildNumber:      session.SQLChildNumber,
			SessionState:        sessionStateFromSession(session),
			Event:               session.Event,
			WaitClass:           session.WaitClass,
			BlockingSession:     session.BlockingSession,
			BlockingInstID:      session.BlockingInstance,
			Program:             session.Program,
			Module:              session.Module,
			Action:              session.Action,
			Machine:             session.Machine,
		}
		samples = append(samples, sample)
	}
	return samples
}

func sessionLooksActiveForDatabaseActivity(session SessionSample) bool {
	if stringPtrEqual(session.Status, "ACTIVE") {
		return true
	}
	if session.SQLID != nil && *session.SQLID != "" {
		return true
	}
	if session.WaitClass != nil && *session.WaitClass != "" && *session.WaitClass != "Idle" {
		return true
	}
	if session.BlockingSession != nil {
		return true
	}
	return false
}

func sessionStateFromSession(session SessionSample) *string {
	if session.WaitClass == nil || *session.WaitClass == "" || *session.WaitClass == "Idle" {
		state := "ON CPU"
		return &state
	}
	state := "WAITING"
	return &state
}

func stringPtrEqual(value *string, expected string) bool {
	return value != nil && *value == expected
}

func int64Ptr(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func stringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func timePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}
