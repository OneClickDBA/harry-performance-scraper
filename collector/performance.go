// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const topSQLLimit = 100

const sqlPerformanceQuery = `
select
	q.inst_id,
	q.sql_id,
	cast(null as number) as child_number,
	q.plan_hash_value,
	cast(null as varchar2(128)) as parsing_schema_name,
	cast(null as varchar2(64)) as module,
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
	q.last_active_time
from gv$sqlstats q
where q.sql_id is not null
	and q.last_active_time >= sysdate - (:1 / 86400)
order by q.last_active_time desc`

const sqlDetailQueryPrefix = `
select inst_id, sql_id, child_number, plan_hash_value, sql_fulltext
from (
	select q.inst_id, q.sql_id, q.child_number, q.plan_hash_value, q.sql_fulltext,
		row_number() over (
			partition by q.inst_id, q.sql_id, q.plan_hash_value
			order by q.elapsed_time desc, q.child_number
		) as child_rank
	from gv$sql q
	where q.sql_id in (`

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

func (e *Scraper) ScrapePerformanceSamples(d *Database, tick *time.Time) (PerformanceSamples, error) {
	collectedAt := time.Now()
	if tick != nil {
		collectedAt = *tick
	}

	timeout := time.Duration(d.Config.GetQueryTimeout()) * time.Second
	sqlSamples, err := e.scrapeSQLSamples(d, collectedAt, timeout)
	if err != nil {
		err = fmt.Errorf("scrape sql performance: %w", err)
	} else {
		e.recordSQLConsumerDeltas(d.Name, sqlSamples)
	}
	sessionSamples, sessionErr := e.scrapeSessionSamples(d, collectedAt, timeout)
	if sessionErr != nil {
		sessionErr = fmt.Errorf("scrape session performance: %w", sessionErr)
	}
	blockingSamples, blockingErr := e.scrapeBlockingSessionSamples(d, collectedAt, timeout)
	if blockingErr != nil {
		blockingErr = fmt.Errorf("scrape blocking sessions: %w", blockingErr)
	}
	var sqlPlans []SQLPlanOperation
	var sqlTexts []SQLTextSample
	var sqlDetails []SQLSample
	var sqlPlanErr error
	if e.shouldCollectSQLDetails(d.Name, collectedAt) {
		consumerSQLIDs := e.takeTopSQLConsumers(d.Name, e.Performance.SQLPlans.GetTopN())
		detailSamples, detailErr := e.scrapeSQLDetails(d, collectedAt, consumerSQLIDs, e.Performance.SQLPlans.GetQueryTimeout())
		sqlDetails = detailSamples
		if detailErr != nil {
			sqlPlanErr = fmt.Errorf("scrape SQL text and plan candidates: %w", detailErr)
		}
		for _, detail := range detailSamples {
			if detail.SQLFullText != nil && strings.TrimSpace(*detail.SQLFullText) != "" {
				sqlTexts = append(sqlTexts, SQLTextSample{
					CollectedAt: detail.CollectedAt,
					Database:    detail.Database,
					SQLID:       detail.SQLID,
					SQLFullText: *detail.SQLFullText,
				})
			}
		}
		candidates := e.uncachedSQLPlanCandidates(d.Name, detailSamples, topSQLLimit)
		if len(candidates) > 0 {
			var planErr error
			sqlPlans, planErr = e.scrapeSQLPlans(d, collectedAt, candidates, e.Performance.SQLPlans.GetQueryTimeout())
			if planErr != nil {
				sqlPlanErr = errors.Join(sqlPlanErr, fmt.Errorf("scrape SQL execution plans: %w", planErr))
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
	return PerformanceSamples{
		SQL:              sqlSamples,
		SQLDetails:       sqlDetails,
		SQLTexts:         sqlTexts,
		SQLPlans:         sqlPlans,
		Sessions:         sessionSamples,
		BlockingSessions: blockingSamples,
	}, errors.Join(err, sqlPlanErr, sessionErr, blockingErr)
}

func (e *Scraper) shouldCollectSQLDetails(database string, collectedAt time.Time) bool {
	if e.MetricsConfiguration == nil || !e.Performance.SQLPlans.GetEnabled() {
		return false
	}

	e.planCacheMu.Lock()
	defer e.planCacheMu.Unlock()
	if e.lastSQLDetailCollection == nil {
		e.lastSQLDetailCollection = map[string]time.Time{}
	}
	last, ok := e.lastSQLDetailCollection[database]
	if ok && collectedAt.Before(last.Add(e.Performance.SQLPlans.GetInterval())) {
		return false
	}
	e.lastSQLDetailCollection[database] = collectedAt
	return true
}

type sqlCounterKey struct {
	InstID        int64
	SQLID         string
	PlanHashValue int64
}

type sqlCounterValues struct {
	ElapsedTimeMicro     int64
	CPUTimeMicro         int64
	UserIOWaitMicro      int64
	ApplicationWaitMicro int64
	ConcurrencyWaitMicro int64
	ClusterWaitMicro     int64
	Executions           int64
	BufferGets           int64
	DiskReads            int64
}

func (e *Scraper) recordSQLConsumerDeltas(database string, samples []SQLSample) {
	e.sqlConsumerMu.Lock()
	defer e.sqlConsumerMu.Unlock()

	if e.sqlCounterSnapshots == nil {
		e.sqlCounterSnapshots = map[string]map[sqlCounterKey]sqlCounterValues{}
	}
	if e.sqlConsumerDeltas == nil {
		e.sqlConsumerDeltas = map[string]map[string]sqlCounterValues{}
	}
	previous, initialized := e.sqlCounterSnapshots[database]
	current := make(map[sqlCounterKey]sqlCounterValues, len(samples))
	if e.sqlConsumerDeltas[database] == nil {
		e.sqlConsumerDeltas[database] = map[string]sqlCounterValues{}
	}

	for _, sample := range samples {
		if sample.SQLID == "" {
			continue
		}
		key := sqlCounterKey{InstID: sample.InstID, SQLID: sample.SQLID}
		if sample.PlanHashValue != nil {
			key.PlanHashValue = *sample.PlanHashValue
		}
		values := sqlCounterValuesFromSample(sample)
		current[key] = values
		if !initialized {
			continue
		}
		previousValues, hadPrevious := previous[key]
		delta := sqlCounterDelta(values, previousValues, hadPrevious)
		accumulated := e.sqlConsumerDeltas[database][sample.SQLID]
		accumulated.add(delta)
		e.sqlConsumerDeltas[database][sample.SQLID] = accumulated
	}
	e.sqlCounterSnapshots[database] = current
}

func sqlCounterValuesFromSample(sample SQLSample) sqlCounterValues {
	return sqlCounterValues{
		ElapsedTimeMicro:     int64Value(sample.ElapsedTimeMicro),
		CPUTimeMicro:         int64Value(sample.CPUTimeMicro),
		UserIOWaitMicro:      int64Value(sample.UserIOWaitMicro),
		ApplicationWaitMicro: int64Value(sample.ApplicationWaitMicro),
		ConcurrencyWaitMicro: int64Value(sample.ConcurrencyWaitMicro),
		ClusterWaitMicro:     int64Value(sample.ClusterWaitMicro),
		Executions:           int64Value(sample.Executions),
		BufferGets:           int64Value(sample.BufferGets),
		DiskReads:            int64Value(sample.DiskReads),
	}
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func sqlCounterDelta(current, previous sqlCounterValues, hadPrevious bool) sqlCounterValues {
	if !hadPrevious {
		return current
	}
	return sqlCounterValues{
		ElapsedTimeMicro:     monotonicDelta(current.ElapsedTimeMicro, previous.ElapsedTimeMicro),
		CPUTimeMicro:         monotonicDelta(current.CPUTimeMicro, previous.CPUTimeMicro),
		UserIOWaitMicro:      monotonicDelta(current.UserIOWaitMicro, previous.UserIOWaitMicro),
		ApplicationWaitMicro: monotonicDelta(current.ApplicationWaitMicro, previous.ApplicationWaitMicro),
		ConcurrencyWaitMicro: monotonicDelta(current.ConcurrencyWaitMicro, previous.ConcurrencyWaitMicro),
		ClusterWaitMicro:     monotonicDelta(current.ClusterWaitMicro, previous.ClusterWaitMicro),
		Executions:           monotonicDelta(current.Executions, previous.Executions),
		BufferGets:           monotonicDelta(current.BufferGets, previous.BufferGets),
		DiskReads:            monotonicDelta(current.DiskReads, previous.DiskReads),
	}
}

func monotonicDelta(current, previous int64) int64 {
	if current >= previous {
		return current - previous
	}
	return current
}

func (v *sqlCounterValues) add(other sqlCounterValues) {
	v.ElapsedTimeMicro += other.ElapsedTimeMicro
	v.CPUTimeMicro += other.CPUTimeMicro
	v.UserIOWaitMicro += other.UserIOWaitMicro
	v.ApplicationWaitMicro += other.ApplicationWaitMicro
	v.ConcurrencyWaitMicro += other.ConcurrencyWaitMicro
	v.ClusterWaitMicro += other.ClusterWaitMicro
	v.Executions += other.Executions
	v.BufferGets += other.BufferGets
	v.DiskReads += other.DiskReads
}

func (e *Scraper) takeTopSQLConsumers(database string, limit int) []string {
	e.sqlConsumerMu.Lock()
	defer e.sqlConsumerMu.Unlock()

	deltas := e.sqlConsumerDeltas[database]
	type rankedSQL struct {
		SQLID string
		sqlCounterValues
	}
	ranked := make([]rankedSQL, 0, len(deltas))
	for sqlID, values := range deltas {
		if values.ElapsedTimeMicro <= 0 {
			continue
		}
		ranked = append(ranked, rankedSQL{SQLID: sqlID, sqlCounterValues: values})
	}
	slices.SortFunc(ranked, func(a, b rankedSQL) int {
		if a.ElapsedTimeMicro != b.ElapsedTimeMicro {
			return -compareInt64(a.ElapsedTimeMicro, b.ElapsedTimeMicro)
		}
		if a.CPUTimeMicro != b.CPUTimeMicro {
			return -compareInt64(a.CPUTimeMicro, b.CPUTimeMicro)
		}
		if a.UserIOWaitMicro != b.UserIOWaitMicro {
			return -compareInt64(a.UserIOWaitMicro, b.UserIOWaitMicro)
		}
		return strings.Compare(a.SQLID, b.SQLID)
	})
	if limit > len(ranked) {
		limit = len(ranked)
	}
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		result[i] = ranked[i].SQLID
	}
	delete(e.sqlConsumerDeltas, database)
	return result
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
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

func buildSQLDetailQuery(sqlIDs []string) (string, []any) {
	binds := make([]string, len(sqlIDs))
	args := make([]any, len(sqlIDs))
	for i, sqlID := range sqlIDs {
		binds[i] = fmt.Sprintf(":%d", i+1)
		args[i] = sqlID
	}
	query := sqlDetailQueryPrefix + strings.Join(binds, ", ") + `)
)
where child_rank = 1
order by sql_id, inst_id, plan_hash_value, child_number`
	return query, args
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

	lookback := 2 * e.scrapeInterval()
	if lookback < time.Minute {
		lookback = time.Minute
	}
	rows, unlock, err := d.QueryContext(ctx, sqlPerformanceQuery, lookback.Seconds())
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
		var parsingSchema, module sql.NullString
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
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

func (e *Scraper) scrapeSQLDetails(d *Database, collectedAt time.Time, sqlIDs []string, timeout time.Duration) ([]SQLSample, error) {
	if len(sqlIDs) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	query, args := buildSQLDetailQuery(sqlIDs)
	rows, unlock, err := d.QueryContext(ctx, query, args...)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("Oracle query timed out")
		}
		return nil, err
	}
	defer unlock()
	defer rows.Close()

	var samples []SQLSample
	for rows.Next() {
		var sample SQLSample
		var childNumber, planHashValue sql.NullInt64
		var sqlFullText sql.NullString
		if err := rows.Scan(&sample.InstID, &sample.SQLID, &childNumber, &planHashValue, &sqlFullText); err != nil {
			return nil, err
		}
		sample.CollectedAt = collectedAt
		sample.Database = d.Name
		sample.ChildNumber = int64Ptr(childNumber)
		sample.PlanHashValue = int64Ptr(planHashValue)
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
