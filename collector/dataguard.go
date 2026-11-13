// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const dataGuardDiscoveryQuery = `
select
	database_role,
	case
		when database_role <> 'PRIMARY' then 1
		when exists (
			select 1
			from v$archive_dest
			where target = 'STANDBY'
				and (destination is not null or db_unique_name is not null)
		) then 1
		else 0
	end
from v$database`

const dataGuardDestinationQuery = `
select
	d.inst_id,
	d.dest_id,
	d.dest_name,
	d.target,
	d.destination,
	d.status,
	d.transmit_mode,
	d.affirm,
	d.valid_now,
	d.db_unique_name,
	s.type,
	s.database_mode,
	s.recovery_mode,
	s.protection_mode,
	s.synchronized,
	s.synchronization_status,
	s.gap_status,
	s.archived_thread#,
	s.archived_seq#,
	s.applied_thread#,
	s.applied_seq#,
	d.applied_scn,
	d.error
from gv$archive_dest d
left join gv$archive_dest_status s
	on s.inst_id = d.inst_id
	and s.dest_id = d.dest_id

where (
	d.target = 'STANDBY'
	or upper(coalesce(s.type, 'LOCAL')) not in ('LOCAL', 'CROSS-INSTANCE')
)
	and (d.destination is not null or d.db_unique_name is not null)`

const dataGuardLagQuery = `
select name, value, time_computed, datum_time
from v$dataguard_stats
where name in ('transport lag', 'apply lag', 'apply finish time', 'estimated startup time')`

const dataGuardProcessQuery = `
select
	i.inst_id,
	sum(case when m.name = 'MRP0' or lower(m.role) = 'managed recovery' then 1 else 0 end),
	sum(case when m.name = 'RFS' then 1 else 0 end),
	max(case when m.name = 'MRP0' or lower(m.role) = 'managed recovery' then m.action end),
	max(case when lower(m.role) in ('managed recovery', 'recovery logmerger') then m.thread# end),
	max(case when lower(m.role) in ('managed recovery', 'recovery logmerger') then m.sequence# end)
from gv$instance i
left join gv$dataguard_process m on m.inst_id = i.inst_id
group by i.inst_id`

var dataGuardIntervalPattern = regexp.MustCompile(`^([+-]?)([0-9]+) ([0-9]{2}):([0-9]{2}):([0-9]{2}(?:\.[0-9]+)?)$`)

func parseDataGuardInterval(value string) (*float64, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "UNKNOWN") {
		return nil, nil
	}
	match := dataGuardIntervalPattern.FindStringSubmatch(value)
	if match == nil {
		seconds, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("parse Data Guard interval %q: %w", value, err)
		}
		return &seconds, nil
	}
	days, _ := strconv.ParseFloat(match[2], 64)
	hours, _ := strconv.ParseFloat(match[3], 64)
	minutes, _ := strconv.ParseFloat(match[4], 64)
	seconds, _ := strconv.ParseFloat(match[5], 64)
	total := days*86400 + hours*3600 + minutes*60 + seconds
	if match[1] == "-" {
		total = -total
	}
	return &total, nil
}

func parseDataGuardTimestamp(value string) *time.Time {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"01/02/2006 15:04:05", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return &parsed
		}
	}
	return nil
}

func (e *Scraper) scrapeDataGuard(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) (int, error) {
	var role string
	detected := false
	err := queryOperationalRows(ctx, d, dataGuardDiscoveryQuery, func(rows *sql.Rows) error {
		var enabled int64
		if err := rows.Scan(&role, &enabled); err != nil {
			return err
		}
		detected = enabled == 1
		return nil
	})
	if err != nil || !detected {
		return 0, err
	}

	before := out.Count()
	var joined error
	if err := e.scrapeDataGuardDestinations(ctx, d, collectedAt, out); err != nil {
		joined = errors.Join(joined, fmt.Errorf("destinations: %w", err))
	}
	if !strings.EqualFold(role, "PRIMARY") {
		if err := e.scrapeDataGuardLag(ctx, d, collectedAt, role, out); err != nil {
			joined = errors.Join(joined, fmt.Errorf("lag: %w", err))
		}
	}
	if strings.EqualFold(role, "PHYSICAL STANDBY") {
		if err := e.scrapeDataGuardProcesses(ctx, d, collectedAt, role, out); err != nil {
			joined = errors.Join(joined, fmt.Errorf("processes: %w", err))
		}
	}
	return out.Count() - before, joined
}

func (e *Scraper) scrapeDataGuardDestinations(ctx context.Context, d *Database, collectedAt time.Time, out *OperationalSamples) error {
	return queryOperationalRows(ctx, d, dataGuardDestinationQuery, func(rows *sql.Rows) error {
		var sample DataGuardDestinationSample
		var destinationName, target, destination, status, transmitMode, affirm, validNow sql.NullString
		var uniqueName, destinationType, databaseMode, recoveryMode, protectionMode sql.NullString
		var synchronized, synchronizationStatus, gapStatus, destinationError sql.NullString
		var archivedThread, archivedSequence, appliedThread, appliedSequence, appliedSCN sql.NullInt64
		if err := rows.Scan(
			&sample.InstID, &sample.DestID, &destinationName, &target, &destination, &status,
			&transmitMode, &affirm, &validNow, &uniqueName, &destinationType, &databaseMode,
			&recoveryMode, &protectionMode, &synchronized, &synchronizationStatus, &gapStatus,
			&archivedThread, &archivedSequence, &appliedThread, &appliedSequence, &appliedSCN,
			&destinationError,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database = collectedAt, d.Name
		sample.DestinationName, sample.Target = stringPtr(destinationName), stringPtr(target)
		sample.Destination, sample.Status = stringPtr(destination), stringPtr(status)
		sample.TransmitMode, sample.Affirm, sample.ValidNow = stringPtr(transmitMode), stringPtr(affirm), stringPtr(validNow)
		sample.DBUniqueName, sample.DestinationType = stringPtr(uniqueName), stringPtr(destinationType)
		sample.DatabaseMode, sample.RecoveryMode = stringPtr(databaseMode), stringPtr(recoveryMode)
		sample.ProtectionMode, sample.Synchronized = stringPtr(protectionMode), stringPtr(synchronized)
		sample.SynchronizationStatus, sample.GapStatus = stringPtr(synchronizationStatus), stringPtr(gapStatus)
		sample.ArchivedThread, sample.ArchivedSequence = int64Ptr(archivedThread), int64Ptr(archivedSequence)
		sample.AppliedThread, sample.AppliedSequence = int64Ptr(appliedThread), int64Ptr(appliedSequence)
		sample.AppliedSCN, sample.Error = int64Ptr(appliedSCN), stringPtr(destinationError)
		out.DataGuardDestinations = append(out.DataGuardDestinations, sample)
		return nil
	})
}

func (e *Scraper) scrapeDataGuardLag(ctx context.Context, d *Database, collectedAt time.Time, role string, out *OperationalSamples) error {
	sample := DataGuardLagSample{CollectedAt: collectedAt, Database: d.Name, DatabaseRole: role}
	rowsSeen := 0
	err := queryOperationalRows(ctx, d, dataGuardLagQuery, func(rows *sql.Rows) error {
		var name string
		var value, timeComputed, datumTime sql.NullString
		if err := rows.Scan(&name, &value, &timeComputed, &datumTime); err != nil {
			return err
		}
		seconds, err := parseDataGuardInterval(value.String)
		if err != nil {
			return err
		}
		switch strings.ToLower(name) {
		case "transport lag":
			sample.TransportLagSeconds = seconds
		case "apply lag":
			sample.ApplyLagSeconds = seconds
		case "apply finish time":
			sample.ApplyFinishTimeSeconds = seconds
		case "estimated startup time":
			sample.EstimatedStartupSeconds = seconds
		}
		if timeComputed.Valid {
			sample.TimeComputed = stringPtr(timeComputed)
		}
		if datumTime.Valid {
			sample.DatumTime = stringPtr(datumTime)
		}
		if computed, datum := parseDataGuardTimestamp(timeComputed.String), parseDataGuardTimestamp(datumTime.String); computed != nil && datum != nil {
			age := computed.Sub(*datum).Seconds()
			if sample.DatumAgeSeconds == nil || age > *sample.DatumAgeSeconds {
				sample.DatumAgeSeconds = &age
			}
		}
		rowsSeen++
		return nil
	})
	if err == nil && rowsSeen > 0 {
		out.DataGuardLag = append(out.DataGuardLag, sample)
	}
	return err
}

func (e *Scraper) scrapeDataGuardProcesses(ctx context.Context, d *Database, collectedAt time.Time, role string, out *OperationalSamples) error {
	return queryOperationalRows(ctx, d, dataGuardProcessQuery, func(rows *sql.Rows) error {
		var sample DataGuardProcessSample
		var applyStatus sql.NullString
		var thread, sequence sql.NullInt64
		if err := rows.Scan(
			&sample.InstID, &sample.ApplyProcesses, &sample.TransportProcesses, &applyStatus,
			&thread, &sequence,
		); err != nil {
			return err
		}
		sample.CollectedAt, sample.Database, sample.DatabaseRole = collectedAt, d.Name, role
		sample.ApplyStatus = stringPtr(applyStatus)
		sample.ThreadNumber, sample.SequenceNumber = int64Ptr(thread), int64Ptr(sequence)
		out.DataGuardProcesses = append(out.DataGuardProcesses, sample)
		return nil
	})
}
