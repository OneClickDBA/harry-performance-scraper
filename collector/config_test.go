// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dodger-one/oracledb-performance-scraper/ocivault"
)

func TestConnectConfigGetConnMaxLifetime(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg := ConnectConfig{}

		if got := cfg.GetConnMaxLifetime(); got != 30*time.Minute {
			t.Fatalf("expected default connection max lifetime of 30m, got %s", got)
		}
	})

	t.Run("configured", func(t *testing.T) {
		lifetime := 10 * time.Minute
		cfg := ConnectConfig{ConnMaxLifetime: &lifetime}

		if got := cfg.GetConnMaxLifetime(); got != lifetime {
			t.Fatalf("expected configured connection max lifetime of %s, got %s", lifetime, got)
		}
	})
}

func TestConnectConfigGetConnMaxIdleTime(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cfg := ConnectConfig{}

		if got := cfg.GetConnMaxIdleTime(); got != 5*time.Minute {
			t.Fatalf("expected default connection max idle time of 5m, got %s", got)
		}
	})

	t.Run("configured", func(t *testing.T) {
		idleTime := 2 * time.Minute
		cfg := ConnectConfig{ConnMaxIdleTime: &idleTime}

		if got := cfg.GetConnMaxIdleTime(); got != idleTime {
			t.Fatalf("expected configured connection max idle time of %s, got %s", idleTime, got)
		}
	})
}

func TestDatabaseConfigGetPasswordReturnsPasswordFileError(t *testing.T) {
	cfg := DatabaseConfig{
		PasswordFile: filepath.Join(t.TempDir(), "missing-password"),
	}

	_, err := cfg.GetPassword()
	if err == nil {
		t.Fatal("expected missing password file to return an error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing file error, got %v", err)
	}
}

func TestDatabaseConfigPassesOCIVaultAuthMode(t *testing.T) {
	original := getOCIVaultSecret
	var calls []string
	getOCIVaultSecret = func(vaultID, secretName string, authMode ocivault.AuthMode) (string, error) {
		calls = append(calls, fmt.Sprintf("%s/%s/%s", vaultID, secretName, authMode))
		return "secret-value", nil
	}
	t.Cleanup(func() {
		getOCIVaultSecret = original
	})

	cfg := DatabaseConfig{
		Vault: &VaultConfig{
			OCI: &OCIVault{
				ID:             "vault-1",
				Auth:           "instance_principal",
				UsernameSecret: "db-username",
				PasswordSecret: "db-password",
			},
		},
	}

	if got, err := cfg.GetUsername(); err != nil || got != "secret-value" {
		t.Fatalf("expected username from OCI Vault, got %q, %v", got, err)
	}
	if got, err := cfg.GetPassword(); err != nil || got != "secret-value" {
		t.Fatalf("expected password from OCI Vault, got %q, %v", got, err)
	}

	want := []string{
		"vault-1/db-username/instance_principal",
		"vault-1/db-password/instance_principal",
	}
	if strings.Join(calls, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected OCI Vault calls: got %#v want %#v", calls, want)
	}
}

func TestMetricsConfigurationValidateOCIVaultAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	authModes := []ocivault.AuthMode{"", "config_file", "instance_principal", "resource_principal", "workload_identity"}

	for _, authMode := range authModes {
		t.Run("valid "+string(authMode), func(t *testing.T) {
			cfg := &MetricsConfiguration{
				Databases: map[string]DatabaseConfig{
					"db1": {
						Vault: &VaultConfig{
							OCI: &OCIVault{
								ID:             "vault-1",
								Auth:           authMode,
								PasswordSecret: "db-password",
							},
						},
					},
				},
			}

			if err := cfg.validate(logger); err != nil {
				t.Fatalf("expected auth mode %q to validate, got %v", authMode, err)
			}
		})
	}
}

func TestLoadMetricsConfigurationAppliesConfigFileDefaults(t *testing.T) {
	configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
`)

	cfg, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("expected config to load, got %v", err)
	}

	if len(cfg.Metrics.Definitions) != 0 {
		t.Fatalf("expected no additional metric definitions by default, got %#v", cfg.Metrics.Definitions)
	}
	if cfg.Logging.Level != "info" {
		t.Fatalf("expected default log level, got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "logfmt" {
		t.Fatalf("expected default log format, got %q", cfg.Logging.Format)
	}
	if cfg.LogDestination() != "/log/alert.log" {
		t.Fatalf("expected default log destination, got %q", cfg.LogDestination())
	}
	if cfg.LogInterval() != 15*time.Second {
		t.Fatalf("expected default log interval, got %s", cfg.LogInterval())
	}
	if got := *cfg.Web.ListenAddresses; len(got) != 1 || got[0] != ":9161" {
		t.Fatalf("expected default web listen address, got %#v", got)
	}
	if cfg.Output.PostgreSQL.SamplesTable != "oracle_metric_samples" {
		t.Fatalf("expected default metric samples table, got %q", cfg.Output.PostgreSQL.SamplesTable)
	}
	if cfg.Output.PostgreSQL.SQLSamplesTable != "oracle_sql_samples" {
		t.Fatalf("expected default sql samples table, got %q", cfg.Output.PostgreSQL.SQLSamplesTable)
	}
	if cfg.Output.PostgreSQL.SessionSamplesTable != "oracle_session_samples" {
		t.Fatalf("expected default session samples table, got %q", cfg.Output.PostgreSQL.SessionSamplesTable)
	}
	if cfg.Output.PostgreSQL.BlockingSessionsTable != "oracle_blocking_session_samples" {
		t.Fatalf("expected default blocking sessions table, got %q", cfg.Output.PostgreSQL.BlockingSessionsTable)
	}
	if cfg.Output.PostgreSQL.DatabaseActivityTable != "oracle_database_activity_samples" {
		t.Fatalf("expected default database activity table, got %q", cfg.Output.PostgreSQL.DatabaseActivityTable)
	}
	if cfg.Output.PostgreSQL.GetRetention() != 0 {
		t.Fatalf("expected default retention to be disabled, got %s", cfg.Output.PostgreSQL.GetRetention())
	}
	if !cfg.Performance.SQLPlans.GetEnabled() {
		t.Fatal("expected SQL plan collection to be enabled by default")
	}
	if got := cfg.Performance.SQLPlans.GetInterval(); got != 2*time.Minute {
		t.Fatalf("expected default SQL plan interval of 2m, got %s", got)
	}
	if got := cfg.Performance.SQLPlans.GetTopN(); got != 20 {
		t.Fatalf("expected default SQL plan topN of 20, got %d", got)
	}
	if got := cfg.Performance.SQLPlans.GetQueryTimeout(); got != 10*time.Second {
		t.Fatalf("expected default SQL plan query timeout of 10s, got %s", got)
	}
}

func TestLoadMetricsConfigurationAcceptsSQLPlanSettings(t *testing.T) {
	configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
performance:
  sqlPlans:
    enabled: true
    interval: 5m
    topN: 12
    queryTimeout: 20s
`)

	cfg, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("expected config to load, got %v", err)
	}
	if got := cfg.Performance.SQLPlans.GetInterval(); got != 5*time.Minute {
		t.Fatalf("SQL plan interval = %s, want 5m", got)
	}
	if got := cfg.Performance.SQLPlans.GetTopN(); got != 12 {
		t.Fatalf("SQL plan topN = %d, want 12", got)
	}
	if got := cfg.Performance.SQLPlans.GetQueryTimeout(); got != 20*time.Second {
		t.Fatalf("SQL plan query timeout = %s, want 20s", got)
	}
}

func TestLoadMetricsConfigurationRejectsInvalidSQLPlanSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		settings string
		wantErr  string
	}{
		{name: "interval", settings: "    interval: 0s\n", wantErr: "interval must be greater than zero"},
		{name: "topN zero", settings: "    topN: 0\n", wantErr: "topN must be between"},
		{name: "topN too large", settings: "    topN: 101\n", wantErr: "topN must be between"},
		{name: "timeout", settings: "    queryTimeout: 0s\n", wantErr: "queryTimeout must be greater than zero"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
performance:
  sqlPlans:
`+tt.settings)
			_, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestLoadMetricsConfigurationAcceptsMetricDefinitions(t *testing.T) {
	configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
metrics:
  definitions:
    - /etc/oracledb-monitor/oracle-operational-metrics.toml
    - /etc/oracledb-monitor/application-metrics.toml
`)

	cfg, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("expected config to load, got %v", err)
	}
	want := []string{
		"/etc/oracledb-monitor/oracle-operational-metrics.toml",
		"/etc/oracledb-monitor/application-metrics.toml",
	}
	if strings.Join(cfg.Metrics.Definitions, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected metric definitions: got %#v want %#v", cfg.Metrics.Definitions, want)
	}
}

func TestLoadMetricsConfigurationRejectsLegacyMetricFileKeys(t *testing.T) {
	tests := []struct {
		name        string
		metricsYAML string
		wantErr     string
	}{
		{name: "default", metricsYAML: "  default: default-metrics.toml\n", wantErr: "field default not found"},
		{name: "custom", metricsYAML: "  custom: []\n", wantErr: "field custom not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
metrics:
`+tt.metricsYAML)

			_, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestLoadMetricsConfigurationAcceptsPostgreSQLOutputSettings(t *testing.T) {
	configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
output:
  postgresql:
    url: postgres://monitoring:secret@localhost:5432/monitoring?sslmode=disable
    samplesTable: monitoring.oracle_metric_samples
    sqlSamplesTable: monitoring.oracle_sql_samples
    sessionSamplesTable: monitoring.oracle_session_samples
    blockingSessionsTable: monitoring.oracle_blocking_session_samples
    databaseActivityTable: monitoring.oracle_database_activity_samples
    retention: 720h
    autoMigrate: true
    maxConns: 8
`)

	cfg, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("expected config to load, got %v", err)
	}
	if cfg.Output.PostgreSQL.SQLSamplesTable != "monitoring.oracle_sql_samples" {
		t.Fatalf("expected configured sql samples table, got %q", cfg.Output.PostgreSQL.SQLSamplesTable)
	}
	if got := cfg.Output.PostgreSQL.GetRetention(); got != 720*time.Hour {
		t.Fatalf("expected configured retention of 720h, got %s", got)
	}
}

func TestLoadMetricsConfigurationAcceptsLogLevelAndFormat(t *testing.T) {
	configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
log:
  level: debug
  format: json
`)

	cfg, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
	if err != nil {
		t.Fatalf("expected config to load, got %v", err)
	}
	if cfg.Logging.Level != "debug" {
		t.Fatalf("expected configured log level, got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Fatalf("expected configured log format, got %q", cfg.Logging.Format)
	}
}

func TestLoadMetricsConfigurationRejectsInvalidLogLevelAndFormat(t *testing.T) {
	tests := []struct {
		name    string
		logYAML string
		wantErr string
	}{
		{
			name: "invalid level",
			logYAML: `
log:
  level: trace
`,
			wantErr: "invalid log.level",
		},
		{
			name: "invalid format",
			logYAML: `
log:
  format: text
`,
			wantErr: "invalid log.format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := writeScraperConfig(t, `
databases:
  default:
    username: scott
    password: tiger
    url: localhost:1521/freepdb1
`+tt.logYAML)

			_, err := LoadMetricsConfiguration(testLogger(), &Config{ConfigFile: configPath})
			if err == nil {
				t.Fatal("expected invalid logging config to fail")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestMetricsConfigurationValidateRejectsInvalidOCIVaultAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &MetricsConfiguration{
		Databases: map[string]DatabaseConfig{
			"db1": {
				Vault: &VaultConfig{
					OCI: &OCIVault{
						ID:             "vault-1",
						Auth:           "api_key",
						PasswordSecret: "db-password",
					},
				},
			},
		},
	}

	err := cfg.validate(logger)
	if err == nil {
		t.Fatal("expected invalid OCI Vault auth mode to fail validation")
	}
	if !strings.Contains(err.Error(), "database \"db1\"") || !strings.Contains(err.Error(), "accepted values") {
		t.Fatalf("expected validation error to include database and accepted values, got %v", err)
	}
}

func TestLoadMetricsConfigurationRequiresConfigFile(t *testing.T) {
	_, err := LoadMetricsConfiguration(testLogger(), &Config{})
	if err == nil {
		t.Fatal("expected missing config file to fail")
	}
	if !strings.Contains(err.Error(), "config file is required") {
		t.Fatalf("expected required config file error, got %v", err)
	}
}

func writeScraperConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatalf("failed to write config fixture: %v", err)
	}
	return path
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
