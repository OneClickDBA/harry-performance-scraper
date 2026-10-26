// Copyright (c) 2025, 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

var errDatabaseSessionNotInitialized = errors.New("database session is not initialized")

func (d *Database) constLabels(labels map[string]string) map[string]string {
	labels[d.DatabaseLabel] = d.Name

	// configured per-database labels added to constLabels
	for label, value := range d.Config.Labels {
		labels[label] = value
	}
	return labels
}

func NewDatabase(logger *slog.Logger, dblabel, dbname string, dbconfig DatabaseConfig) *Database {
	db, err := connect(logger, dbname, dbconfig)
	if err != nil {
		logger.Error("Failed to initialize database session", "error", err, "database", dbname)
	}
	return &Database{
		Name:          dbname,
		Up:            0,
		Session:       db,
		Config:        dbconfig,
		connectErr:    err,
		DatabaseLabel: dblabel,
		reconnectMU:   sync.RWMutex{},
		connection: DatabaseConnectionStatus{
			State: DatabaseConnectionStarting,
		},
	}
}

func (d *Database) StartupReady() bool {
	return d.startupReady.Load()
}

// initCache resets the metrics cached. Used on startup and when metrics are reloaded.
func (d *Database) initCache(metrics map[string]*Metric) {
	d.MetricsCache = NewMetricsCache(metrics)
}

// WarmupConnectionPool serially acquires connections to warm the pool. The
// godror connector also gates physical OCI connection creation process-wide so
// runtime pool recycling cannot create connections concurrently across pools.
func (d *Database) WarmupConnectionPool(logger *slog.Logger, probeTimeout time.Duration) error {
	defer d.startupReady.Store(true)

	d.reconnectMU.RLock()
	session := d.Session
	connectErr := d.connectErr
	d.reconnectMU.RUnlock()

	if session == nil {
		d.setUp(0)
		d.quarantine(connectErr)
		d.suspendActivity(connectErr)
		if connectErr != nil {
			return connectErr
		}
		return errDatabaseSessionNotInitialized
	}

	if err := d.warmupSession(logger, session, probeTimeout); err != nil {
		d.setUp(0)
		d.quarantine(err)
		d.suspendActivity(err)
		return err
	}

	d.setUp(1)
	d.activate(time.Now())
	d.resumeActivity()
	return nil
}

func (d *Database) warmupSession(logger *slog.Logger, session *sql.DB, probeTimeout time.Duration) error {
	if session == nil {
		return errDatabaseSessionNotInitialized
	}

	// Establish basic connectivity before running initialization SQL. Without
	// this preflight, an unreachable target can consume both initdb timeouts and
	// a pool-warmup timeout before startup advances to the next database.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), probeTimeout)
	err := session.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		return err
	}

	var connections []*sql.Conn
	poolSize := d.Config.GetMaxOpenConns()
	if poolSize < 1 {
		poolSize = d.Config.GetPoolMaxConnections()
	}
	if poolSize > 100 { // defensively cap poolsize
		poolSize = 100
	}
	warmup := func(i int) error {
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()

		conn, err := session.Conn(ctx)
		if err != nil {
			return err
		}
		connections = append(connections, conn)
		return nil
	}

	initdb(logger, d.Name, d.Config, session)

	for i := 0; i < poolSize; i++ {
		// short circuit warmup for inaccessible databases
		if err := warmup(i + 1); err != nil {
			logger.Debug("Failed warmup database connection pool", "conn", i, "error", err, "database", d.Name)
			return err
		}
	}

	logger.Debug("Warmed connection pool", "total", len(connections), "database", d.Name)
	for i, conn := range connections {
		if err := conn.Close(); err != nil {
			logger.Debug("Failed to return database connection to pool on warmup", "conn", i+1, "error", err, "database", d.Name)
		}
	}
	return nil
}

func (d *Database) reconnect(logger *slog.Logger, probeTimeout time.Duration) error {
	d.reconnectAttemptMU.Lock()
	defer d.reconnectAttemptMU.Unlock()

	logger.Info("Reconnecting database session", "database", d.Name)

	session, err := connect(logger, d.Name, d.Config)
	if err != nil {
		d.reconnectMU.Lock()
		d.connectErr = err
		d.Up = 0
		d.reconnectMU.Unlock()
		d.suspendActivity(err)
		return err
	}
	if err := d.warmupSession(logger, session, probeTimeout); err != nil {
		if session != nil {
			_ = session.Close()
		}
		d.reconnectMU.Lock()
		d.connectErr = err
		d.Up = 0
		d.reconnectMU.Unlock()
		d.suspendActivity(err)
		return err
	}

	d.reconnectMU.Lock()
	oldSession := d.Session
	d.Session = session
	d.connectErr = nil
	d.Up = 1
	if oldSession != nil && oldSession != session {
		_ = oldSession.Close()
	}
	d.reconnectMU.Unlock()
	d.resumeActivity()
	return nil
}

// ping verifies an active target. It never reconnects; DCD exclusively owns
// pool reconstruction for quarantined databases.
func (d *Database) ping(probeTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	err := d.PingContext(ctx)
	if err != nil {
		d.setUp(0)
		d.quarantine(err)
		d.suspendActivity(err)
		return err
	}
	d.setUp(1)
	d.resumeActivity()
	return nil
}

func (d *Database) suspendActivity(err error) {
	d.activityMU.Lock()
	defer d.activityMU.Unlock()
	if err == nil {
		d.activityError = "database connection is unavailable"
		return
	}
	d.activityError = err.Error()
}

func (d *Database) resumeActivity() {
	d.activityMU.Lock()
	d.activityError = ""
	d.activityMU.Unlock()
}

func (d *Database) activityUnavailable() error {
	d.activityMU.RLock()
	defer d.activityMU.RUnlock()
	if d.activityError == "" {
		return nil
	}
	return fmt.Errorf("activity collection suspended until connectivity recovers: %s", d.activityError)
}

func (d *Database) connectionState() DatabaseConnectionState {
	d.reconnectMU.RLock()
	defer d.reconnectMU.RUnlock()
	return d.connection.State
}

func (d *Database) isActive() bool {
	return d.connectionState() == DatabaseConnectionActive
}

// IsActive reports whether normal collectors may use the database.
func (d *Database) IsActive() bool {
	return d.isActive()
}

func (d *Database) quarantine(err error) {
	d.reconnectMU.Lock()
	defer d.reconnectMU.Unlock()
	d.quarantineLocked(err, time.Now())
}

func (d *Database) quarantineConnectionFailure(err error) bool {
	if !isConnectionFailure(err) {
		return false
	}
	d.setUp(0)
	d.quarantine(err)
	d.suspendActivity(err)
	return true
}

func isConnectionFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, errDatabaseSessionNotInitialized) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && !errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToUpper(err.Error())
	for _, marker := range []string{
		"ORA-01033", "ORA-01034", "ORA-01089",
		"ORA-03113", "ORA-03114", "ORA-03135",
		"ORA-12154", "ORA-12170", "ORA-125",
		"DPI-1010", "DPI-1080",
		"BROKEN PIPE", "CONNECTION REFUSED", "CONNECTION RESET",
		"DATABASE IS CLOSED", "NETWORK IS UNREACHABLE", "NO ROUTE TO HOST",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (d *Database) quarantineLocked(err error, now time.Time) {
	if d.connection.QueuedAt == nil ||
		(d.connection.State != DatabaseConnectionQuarantined && d.connection.State != DatabaseConnectionProbing) {
		queuedAt := now
		d.connection.QueuedAt = &queuedAt
	}
	d.connection.State = DatabaseConnectionQuarantined
	d.connection.LastFailureAt = timePointer(now)
	if err == nil {
		d.connection.LastFailure = "database connection is unavailable"
	} else {
		d.connection.LastFailure = err.Error()
	}
}

func (d *Database) beginProbe(now time.Time) bool {
	d.reconnectMU.Lock()
	defer d.reconnectMU.Unlock()
	if d.connection.State != DatabaseConnectionQuarantined {
		return false
	}
	d.connection.State = DatabaseConnectionProbing
	d.connection.LastProbeAt = timePointer(now)
	d.connection.LastProbeDuration = nil
	d.connection.LastProbeError = ""
	d.connection.ProbeAttempts++
	return true
}

func (d *Database) finishProbe(started time.Time, err error) {
	now := time.Now()
	d.reconnectMU.Lock()
	defer d.reconnectMU.Unlock()
	duration := now.Sub(started)
	d.connection.LastProbeDuration = &duration
	if err != nil {
		d.connection.LastProbeError = err.Error()
		d.quarantineLocked(err, now)
		return
	}
	d.connection.State = DatabaseConnectionActive
	d.connection.QueuedAt = nil
	d.connection.LastProbeError = ""
	d.connection.RecoveredAt = timePointer(now)
}

func (d *Database) activate(now time.Time) {
	d.reconnectMU.Lock()
	defer d.reconnectMU.Unlock()
	d.connection.State = DatabaseConnectionActive
	d.connection.QueuedAt = nil
	d.connection.LastProbeError = ""
}

func (d *Database) connectionStateSample(observedAt time.Time, scraperInstance string) DatabaseConnectionStateSample {
	d.reconnectMU.RLock()
	defer d.reconnectMU.RUnlock()
	status := d.connection
	sample := DatabaseConnectionStateSample{
		ObservedAt:      observedAt,
		Database:        d.Name,
		State:           string(status.State),
		QueuedAt:        cloneTimePointer(status.QueuedAt),
		LastFailureAt:   cloneTimePointer(status.LastFailureAt),
		LastProbeAt:     cloneTimePointer(status.LastProbeAt),
		ProbeAttempts:   status.ProbeAttempts,
		RecoveredAt:     cloneTimePointer(status.RecoveredAt),
		ScraperInstance: scraperInstance,
	}
	if status.LastFailure != "" {
		value := status.LastFailure
		sample.LastFailure = &value
	}
	if status.LastProbeError != "" {
		value := status.LastProbeError
		sample.LastProbeError = &value
	}
	if status.LastProbeDuration != nil {
		seconds := status.LastProbeDuration.Seconds()
		sample.LastProbeDurationSeconds = &seconds
	}
	return sample
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (d *Database) getUp() float64 {
	d.reconnectMU.RLock()
	defer d.reconnectMU.RUnlock()
	return d.Up
}

func (d *Database) setUp(up float64) {
	d.reconnectMU.Lock()
	defer d.reconnectMU.Unlock()
	d.Up = up
}

func (d *Database) Query(query string, args ...interface{}) (*sql.Rows, func(), error) {
	d.reconnectMU.RLock()
	if d.Session == nil {
		err := d.connectErr
		d.reconnectMU.RUnlock()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, errDatabaseSessionNotInitialized
	}

	rows, err := d.Session.Query(query, args...)
	if err != nil {
		d.reconnectMU.RUnlock()
		return nil, nil, err
	}
	return rows, d.reconnectMU.RUnlock, nil
}

func (d *Database) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, func(), error) {
	d.reconnectMU.RLock()
	if d.Session == nil {
		err := d.connectErr
		d.reconnectMU.RUnlock()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, errDatabaseSessionNotInitialized
	}

	rows, err := d.Session.QueryContext(ctx, query, args...)
	if err != nil {
		d.reconnectMU.RUnlock()
		return nil, nil, err
	}
	return rows, d.reconnectMU.RUnlock, nil
}

func (d *Database) PingContext(ctx context.Context) error {
	d.reconnectMU.RLock()
	defer d.reconnectMU.RUnlock()
	if d.Session == nil {
		return errDatabaseSessionNotInitialized
	}
	return d.Session.PingContext(ctx)
}

func initdb(logger *slog.Logger, dbname string, dbconfig DatabaseConfig, db *sql.DB) {
	logger.Debug(fmt.Sprintf("set max idle connections to %d", dbconfig.MaxIdleConns), "database", dbname)
	db.SetMaxIdleConns(dbconfig.GetMaxIdleConns())
	logger.Debug(fmt.Sprintf("set max open connections to %d", dbconfig.MaxOpenConns), "database", dbname)
	db.SetMaxOpenConns(dbconfig.GetMaxOpenConns())
	logger.Debug(fmt.Sprintf("set connection max lifetime to %s", dbconfig.GetConnMaxLifetime()), "database", dbname)
	db.SetConnMaxLifetime(dbconfig.GetConnMaxLifetime())
	logger.Debug(fmt.Sprintf("set connection max idle time to %s", dbconfig.GetConnMaxIdleTime()), "database", dbname)
	db.SetConnMaxIdleTime(dbconfig.GetConnMaxIdleTime())
	logger.Debug(fmt.Sprintf("Successfully configured connection to %s", maskDsn(dbconfig.URL)), "database", dbname)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
			begin
			dbms_application_info.set_client_info('harry-scraper');
			end;`); err != nil {
		logger.Info("Could not set CLIENT_INFO.", "database", dbname)
	}

	var sysdba string
	if err := db.QueryRowContext(ctx, "select sys_context('USERENV', 'ISDBA') from dual").Scan(&sysdba); err != nil {
		logger.Error("error checking my database role", "error", err, "database", dbname)
	}
	logger.Info("Connected as SYSDBA? "+sysdba, "database", dbname)
}
