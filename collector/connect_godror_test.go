// Copyright (c) 2026 Jorge Holgado.
// Licensed under the Universal Permissive License v 1.0 as shown in LICENSE.txt.

//go:build !goora

package collector

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type trackingConnector struct {
	current atomic.Int64
	maximum atomic.Int64
	calls   atomic.Int64
}

func (c *trackingConnector) Connect(context.Context) (driver.Conn, error) {
	c.calls.Add(1)
	current := c.current.Add(1)
	for maximum := c.maximum.Load(); current > maximum; maximum = c.maximum.Load() {
		if c.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	time.Sleep(10 * time.Millisecond)
	c.current.Add(-1)
	return nil, nil
}

func (*trackingConnector) Driver() driver.Driver { return nil }

func TestGatedConnectorSerializesPhysicalConnections(t *testing.T) {
	gate := newOracleConnectionGate(0)
	tracker := &trackingConnector{}
	connector := gatedConnector{Connector: tracker, gate: gate}

	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := connector.Connect(context.Background()); err != nil {
				t.Errorf("Connect() returned an error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := tracker.maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent physical connections = %d, want 1", got)
	}
	if got := tracker.calls.Load(); got != 12 {
		t.Fatalf("physical connection attempts = %d, want 12", got)
	}
}

func TestGatedConnectorHonorsContextWhileWaiting(t *testing.T) {
	gate := newOracleConnectionGate(0)
	gate.semaphore <- struct{}{}
	tracker := &trackingConnector{}
	connector := gatedConnector{Connector: tracker, gate: gate}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := connector.Connect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect() error = %v, want context.Canceled", err)
	}
	if got := tracker.calls.Load(); got != 0 {
		t.Fatalf("physical connection attempts = %d, want 0", got)
	}
}

func TestGatedConnectorSpacesPhysicalConnections(t *testing.T) {
	const minimumInterval = 20 * time.Millisecond
	gate := newOracleConnectionGate(minimumInterval)
	tracker := &trackingConnector{}
	connector := gatedConnector{Connector: tracker, gate: gate}

	if _, err := connector.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect() returned an error: %v", err)
	}
	started := time.Now()
	if _, err := connector.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect() returned an error: %v", err)
	}
	if elapsed := time.Since(started); elapsed < minimumInterval {
		t.Fatalf("second connection waited %s, want at least %s", elapsed, minimumInterval)
	}
}
