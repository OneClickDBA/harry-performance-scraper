// Copyright (c) 2026 Jorge Holgado.
// Licensed under the Universal Permissive License v 1.0 as shown in LICENSE.txt.

package ha

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const lockNamespace = "harry-performance-scraper/ha/v1:"

const acquireQuery = `
select case
	when not pg_is_in_recovery()
	 and current_setting('transaction_read_only') = 'off'
	then pg_try_advisory_lock($1)
	else false
end`

const writablePrimaryQuery = `
select not pg_is_in_recovery()
	and current_setting('transaction_read_only') = 'off'`

// Elector holds a session-level PostgreSQL advisory lock on a dedicated
// connection. The connection must never be returned to a pool.
type Elector struct {
	logger             *slog.Logger
	connectionConfig   *pgx.ConnConfig
	scope              string
	lockID             int64
	retryInterval      time.Duration
	validationInterval time.Duration
	mu                 sync.Mutex
	conn               *pgx.Conn
}

func New(logger *slog.Logger, url, scope string, retryInterval, validationInterval time.Duration) (*Elector, error) {
	connectionConfig, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL URL for HA: %w", err)
	}
	if connectionConfig.RuntimeParams == nil {
		connectionConfig.RuntimeParams = make(map[string]string)
	}
	if _, configured := connectionConfig.RuntimeParams["application_name"]; !configured {
		connectionConfig.RuntimeParams["application_name"] = "harry-scraper-ha"
	}
	return &Elector{
		logger:             logger,
		connectionConfig:   connectionConfig,
		scope:              scope,
		lockID:             lockIDForScope(scope),
		retryInterval:      retryInterval,
		validationInterval: validationInterval,
	}, nil
}

// lockIDForScope maps a human-readable scope to a stable positive advisory-lock key.
// The namespace and hash algorithm form part of the HA compatibility contract.
func lockIDForScope(scope string) int64 {
	digest := sha256.Sum256([]byte(lockNamespace + scope))
	return int64(binary.BigEndian.Uint64(digest[:8]) & math.MaxInt64)
}

// Acquire waits until this process connects to a writable primary and becomes
// the sole advisory-lock owner for its scope.
func (e *Elector) Acquire(ctx context.Context) error {
	standbyLogged := false
	for {
		conn, err := pgx.ConnectConfig(ctx, e.connectionConfig.Copy())
		if err == nil {
			var acquired bool
			queryCtx, cancel := context.WithTimeout(ctx, e.retryInterval)
			err = conn.QueryRow(queryCtx, acquireQuery, e.lockID).Scan(&acquired)
			cancel()
			if err == nil && acquired {
				e.mu.Lock()
				e.conn = conn
				e.mu.Unlock()
				e.logger.Info("Acquired PostgreSQL HA leadership", "scope", e.scope)
				return nil
			}
			_ = conn.Close(context.Background())
			if err == nil && !standbyLogged {
				e.logger.Info("PostgreSQL HA leadership is held by another instance", "scope", e.scope)
				standbyLogged = true
			} else if err == nil {
				e.logger.Debug("PostgreSQL HA leadership is still held by another instance", "scope", e.scope)
			} else {
				e.logger.Warn("Unable to attempt PostgreSQL HA leadership", "scope", e.scope, "error", err)
			}
		} else if !errors.Is(err, context.Canceled) {
			e.logger.Warn("Unable to connect to PostgreSQL for HA leadership", "scope", e.scope, "error", err)
		}

		timer := time.NewTimer(e.retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Monitor verifies that the dedicated lock connection still reaches a
// writable PostgreSQL primary. A successful advisory-lock call is deliberately
// not repeated because PostgreSQL session locks stack.
func (e *Elector) Monitor(ctx context.Context) error {
	e.mu.Lock()
	if e.conn == nil {
		e.mu.Unlock()
		return errors.New("PostgreSQL HA leadership has not been acquired")
	}
	e.mu.Unlock()
	ticker := time.NewTicker(e.validationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var writablePrimary bool
			queryCtx, cancel := context.WithTimeout(ctx, e.validationInterval)
			e.mu.Lock()
			if e.conn == nil {
				e.mu.Unlock()
				cancel()
				return errors.New("PostgreSQL HA leadership connection is closed")
			}
			err := e.conn.QueryRow(queryCtx, writablePrimaryQuery).Scan(&writablePrimary)
			e.mu.Unlock()
			cancel()
			if err != nil {
				return fmt.Errorf("validate PostgreSQL HA leadership connection: %w", err)
			}
			if !writablePrimary {
				return errors.New("PostgreSQL HA leadership connection is no longer on a writable primary")
			}
		}
	}
}

func (e *Elector) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		return nil
	}
	err := e.conn.Close(ctx)
	e.conn = nil
	return err
}
