// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
	q.sql_fulltext
from gv$sql q
where q.sql_id is not null
order by q.elapsed_time desc
fetch first 100 rows only`

const sqlPlanQueryPrefix = `
select
	p.inst_id,
	p.sql_id,
	p.child_number,
	p.plan_hash_value,
	p.id,
	p.parent_id,
	p.depth,
	p.position,
	p.operation,
	p.options,
	p.object_owner,
	p.object_name,
	p.object_type,
	p.optimizer,
	p.cost,
	p.cardinality,
	p.bytes,
	p.cpu_cost,
	p.io_cost,
	p.temp_space,
	p.partition_start,
	p.partition_stop,
	p.access_predicates,
	p.filter_predicates
from gv$sql_plan p
where `

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
	var sqlPlans []SQLPlanOperation
	var sqlPlanErr error
	if len(sqlSamples) > 0 && e.shouldCollectSQLPlans(d.Name, collectedAt) {
		candidates := e.uncachedSQLPlanCandidates(d.Name, sqlSamples, e.Performance.SQLPlans.GetTopN())
		if len(candidates) > 0 {
			sqlPlans, sqlPlanErr = e.scrapeSQLPlans(d, collectedAt, candidates, e.Performance.SQLPlans.GetQueryTimeout())
			if sqlPlanErr != nil {
				sqlPlanErr = fmt.Errorf("scrape SQL execution plans: %w", sqlPlanErr)
			} else {
				e.markSQLPlansCollected(d.Name, sqlPlans)
				e.logger.Debug("Scraped SQL execution plans",
					"database", d.Name,
					"cursor_plans_requested", len(candidates),
					"plan_operations", len(sqlPlans),
				)
			}
		}
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
		SQLPlans:         sqlPlans,
		Sessions:         sessionSamples,
		BlockingSessions: blockingSamples,
		DatabaseActivity: databaseActivitySamples,
	}, errors.Join(err, sqlPlanErr, sessionErr, blockingErr, databaseActivityErr)
}

func (e *Scraper) shouldCollectSQLPlans(database string, collectedAt time.Time) bool {
	if e.MetricsConfiguration == nil || !e.Performance.SQLPlans.GetEnabled() {
		return false
	}

	e.planCacheMu.Lock()
	defer e.planCacheMu.Unlock()
	if e.lastPlanCollection == nil {
		e.lastPlanCollection = map[string]time.Time{}
	}
	last, ok := e.lastPlanCollection[database]
	if ok && collectedAt.Before(last.Add(e.Performance.SQLPlans.GetInterval())) {
		return false
	}
	e.lastPlanCollection[database] = collectedAt
	return true
}

func (e *Scraper) uncachedSQLPlanCandidates(database string, samples []SQLSample, limit int) []SQLPlanKey {
	allCandidates := make([]SQLPlanKey, 0, limit)
	seen := make(map[SQLPlanKey]struct{}, limit)
	for _, sample := range samples {
		if len(allCandidates) >= limit {
			break
		}
		if sample.ChildNumber == nil || sample.PlanHashValue == nil || *sample.PlanHashValue <= 0 {
			continue
		}
		key := SQLPlanKey{
			InstID:        sample.InstID,
			SQLID:         sample.SQLID,
			ChildNumber:   *sample.ChildNumber,
			PlanHashValue: *sample.PlanHashValue,
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		allCandidates = append(allCandidates, key)
	}

	e.planCacheMu.Lock()
	defer e.planCacheMu.Unlock()
	if e.knownPlans == nil {
		e.knownPlans = map[string]map[SQLPlanKey]struct{}{}
	}
	known := e.knownPlans[database]
	activeKnown := make(map[SQLPlanKey]struct{}, len(allCandidates))
	uncached := make([]SQLPlanKey, 0, len(allCandidates))
	for _, candidate := range allCandidates {
		if _, ok := known[candidate]; ok {
			activeKnown[candidate] = struct{}{}
			continue
		}
		uncached = append(uncached, candidate)
	}
	e.knownPlans[database] = activeKnown
	return uncached
}

func (e *Scraper) markSQLPlansCollected(database string, plans []SQLPlanOperation) {
	e.planCacheMu.Lock()
	defer e.planCacheMu.Unlock()
	if e.knownPlans == nil {
		e.knownPlans = map[string]map[SQLPlanKey]struct{}{}
	}
	if e.knownPlans[database] == nil {
		e.knownPlans[database] = map[SQLPlanKey]struct{}{}
	}
	for _, plan := range plans {
		e.knownPlans[database][plan.SQLPlanKey] = struct{}{}
	}
}

func buildSQLPlanQuery(candidates []SQLPlanKey) (string, []any) {
	conditions := make([]string, 0, len(candidates))
	args := make([]any, 0, len(candidates)*4)
	for i, candidate := range candidates {
		bind := i*4 + 1
		conditions = append(conditions, fmt.Sprintf(
			"(p.inst_id = :%d and p.sql_id = :%d and p.child_number = :%d and p.plan_hash_value = :%d)",
			bind, bind+1, bind+2, bind+3,
		))
		args = append(args, candidate.InstID, candidate.SQLID, candidate.ChildNumber, candidate.PlanHashValue)
	}
	return sqlPlanQueryPrefix + strings.Join(conditions, " or ") +
		" order by p.inst_id, p.sql_id, p.child_number, p.plan_hash_value, p.id", args
}

func (e *Scraper) scrapeSQLPlans(d *Database, collectedAt time.Time, candidates []SQLPlanKey, timeout time.Duration) ([]SQLPlanOperation, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	query, args := buildSQLPlanQuery(candidates)
	rows, unlock, err := d.QueryContext(ctx, query, args...)
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("Oracle query timed out")
	}
	if err != nil {
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var plans []SQLPlanOperation
	for rows.Next() {
		var plan SQLPlanOperation
		var parentID, depth, position, cost, cardinality, bytesValue sql.NullInt64
		var cpuCost, ioCost, tempSpace sql.NullInt64
		var operation, options, objectOwner, objectName, objectType, optimizer sql.NullString
		var partitionStart, partitionStop, accessPredicates, filterPredicates sql.NullString
		if err := rows.Scan(
			&plan.InstID,
			&plan.SQLID,
			&plan.ChildNumber,
			&plan.PlanHashValue,
			&plan.PlanLineID,
			&parentID,
			&depth,
			&position,
			&operation,
			&options,
			&objectOwner,
			&objectName,
			&objectType,
			&optimizer,
			&cost,
			&cardinality,
			&bytesValue,
			&cpuCost,
			&ioCost,
			&tempSpace,
			&partitionStart,
			&partitionStop,
			&accessPredicates,
			&filterPredicates,
		); err != nil {
			return nil, err
		}
		plan.CollectedAt = collectedAt
		plan.Database = d.Name
		plan.Operation = operation.String
		plan.ParentID = int64Ptr(parentID)
		plan.Depth = int64Ptr(depth)
		plan.Position = int64Ptr(position)
		plan.Options = stringPtr(options)
		plan.ObjectOwner = stringPtr(objectOwner)
		plan.ObjectName = stringPtr(objectName)
		plan.ObjectType = stringPtr(objectType)
		plan.Optimizer = stringPtr(optimizer)
		plan.Cost = int64Ptr(cost)
		plan.Cardinality = int64Ptr(cardinality)
		plan.Bytes = int64Ptr(bytesValue)
		plan.CPUCost = int64Ptr(cpuCost)
		plan.IOCost = int64Ptr(ioCost)
		plan.TempSpace = int64Ptr(tempSpace)
		plan.PartitionStart = stringPtr(partitionStart)
		plan.PartitionStop = stringPtr(partitionStop)
		plan.AccessPredicates = stringPtr(accessPredicates)
		plan.FilterPredicates = stringPtr(filterPredicates)
		plans = append(plans, plan)
	}
	return plans, rows.Err()
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
		var parsingSchema, module, sqlFullText sql.NullString
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
			&sqlFullText,
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
		sample.SQLFullText = stringPtr(sqlFullText)
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
