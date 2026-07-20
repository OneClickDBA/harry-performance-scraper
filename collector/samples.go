// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"time"
)

// MetricSample is the backend-neutral representation of one collected metric value.
type MetricSample struct {
	CollectedAt time.Time
	Database    string
	Context     string
	Name        string
	Help        string
	Type        string
	Value       float64
	Labels      map[string]string
}

type ScrapeSummary struct {
	StartedAt       time.Time
	FinishedAt      time.Time
	DurationSeconds float64
	TotalErrors     int
	SampleCount     int
}

type SQLSample struct {
	CollectedAt          time.Time
	Database             string
	InstID               int64
	SQLID                string
	ChildNumber          *int64
	PlanHashValue        *int64
	ParsingSchemaName    *string
	Module               *string
	Executions           *int64
	ElapsedTimeMicro     *int64
	CPUTimeMicro         *int64
	UserIOWaitMicro      *int64
	ApplicationWaitMicro *int64
	ConcurrencyWaitMicro *int64
	ClusterWaitMicro     *int64
	BufferGets           *int64
	DiskReads            *int64
	DirectWrites         *int64
	RowsProcessed        *int64
	Fetches              *int64
	Loads                *int64
	Invalidations        *int64
	ParseCalls           *int64
	LastActiveTime       *time.Time
	SQLFullText          *string
}

type SQLTextSample struct {
	CollectedAt time.Time
	Database    string
	SQLID       string
	SQLFullText string
}

type SQLPlanKey struct {
	InstID        int64
	SQLID         string
	ChildNumber   int64
	PlanHashValue int64
}

type SQLPlanOperation struct {
	CollectedAt time.Time
	Database    string
	SQLPlanKey
	PlanLineID       int64
	ParentID         *int64
	Depth            *int64
	Position         *int64
	Operation        string
	Options          *string
	ObjectOwner      *string
	ObjectName       *string
	ObjectType       *string
	Optimizer        *string
	Cost             *int64
	Cardinality      *int64
	Bytes            *int64
	CPUCost          *int64
	IOCost           *int64
	TempSpace        *int64
	PartitionStart   *string
	PartitionStop    *string
	AccessPredicates *string
	FilterPredicates *string
}

type SessionSample struct {
	CollectedAt      time.Time
	Database         string
	InstID           int64
	SID              int64
	SerialNumber     *int64
	Username         *string
	Status           *string
	SQLID            *string
	SQLChildNumber   *int64
	PrevSQLID        *string
	Event            *string
	WaitClass        *string
	State            *string
	SecondsInWait    *int64
	BlockingInstance *int64
	BlockingSession  *int64
	Machine          *string
	Program          *string
	Module           *string
	Action           *string
	ServiceName      *string
	LogonTime        *time.Time
}

type BlockingSessionSample struct {
	CollectedAt      time.Time
	Database         string
	InstID           int64
	SID              int64
	SerialNumber     *int64
	Username         *string
	SQLID            *string
	Event            *string
	WaitClass        *string
	BlockingInstance *int64
	BlockingSession  *int64
	BlockingUsername *string
	BlockingSQLID    *string
	BlockingEvent    *string
}

type DatabaseActivitySample struct {
	CollectedAt           time.Time
	Database              string
	SampleID              int64
	SampleTime            time.Time
	InstID                int64
	SessionID             int64
	SessionSerialNumber   *int64
	SessionType           *string
	UserID                *int64
	SQLID                 *string
	SQLChildNumber        *int64
	SQLExecID             *int64
	SQLExecStart          *time.Time
	TopLevelSQLID         *string
	SessionState          *string
	Event                 *string
	WaitClass             *string
	WaitTimeMicro         *int64
	TimeWaitedMicro       *int64
	BlockingSession       *int64
	BlockingSessionSerial *int64
	BlockingInstID        *int64
	CurrentObjectID       *int64
	CurrentFileNumber     *int64
	CurrentBlockNumber    *int64
	Program               *string
	Module                *string
	Action                *string
	Machine               *string
	PGAAllocated          *int64
	TempSpaceAllocated    *int64
	ConID                 *int64
	SampleSource          string
	SampleDurationMicro   int64
	SQLPlanHashValue      *int64
	SQLFullPlanHashValue  *int64
	SQLPlanLineID         *int64
	ServiceHash           *int64
	ServiceName           *string
	ClientIdentifier      *string
}

type PerformanceSamples struct {
	SQL              []SQLSample
	SQLDetails       []SQLSample
	SQLTexts         []SQLTextSample
	SQLPlans         []SQLPlanOperation
	Sessions         []SessionSample
	BlockingSessions []BlockingSessionSample
	DatabaseActivity []DatabaseActivitySample
}

func (p PerformanceSamples) Count() int {
	return len(p.SQL) + len(p.SQLTexts) + len(p.SQLPlans) + len(p.Sessions) + len(p.BlockingSessions) + len(p.DatabaseActivity)
}

type SampleSink interface {
	WriteSamples(ctx context.Context, samples []MetricSample, performance PerformanceSamples, summary ScrapeSummary) error
	Close()
}
