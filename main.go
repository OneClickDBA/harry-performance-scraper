// Copyright (c) 2021, 2025, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
// Portions Copyright (c) 2016 Seth Miller <seth@sethmiller.me>

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	// Required for debugging
	// _ "net/http/pprof"

	"github.com/dodger-one/oracledb-performance-scraper/alertlog"
	"github.com/dodger-one/oracledb-performance-scraper/collector"
	"github.com/dodger-one/oracledb-performance-scraper/postgresql"
)

// Version will be set at build time.
var Version = "0.0.0.dev"

func parseConfigFile(args []string, getenv func(string) string, output io.Writer) (string, error) {
	flags := flag.NewFlagSet("oracledb_performance_scraper", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	flags.SetOutput(&flagOutput)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage of oracledb_performance_scraper:\n")
		fmt.Fprintf(flags.Output(), "  --config.file string\n")
		fmt.Fprintf(flags.Output(), "        File with metrics scraper configuration. (env: CONFIG_FILE)\n")
	}

	configFile := flags.String("config.file", getenv("CONFIG_FILE"), "File with metrics scraper configuration. (env: CONFIG_FILE)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) && output != nil {
			_, _ = output.Write(flagOutput.Bytes())
		}
		return "", err
	}
	if flags.NArg() > 0 {
		return "", fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(*configFile) == "" {
		return "", errors.New("config file is required; set --config.file or CONFIG_FILE")
	}
	return *configFile, nil
}

func newLogger(levelValue, formatValue string, output io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(levelValue) {
	case "", "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid log level %q", levelValue)
	}
	opts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(formatValue) {
	case "", "logfmt":
		return slog.New(slog.NewTextHandler(output, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(output, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q", formatValue)
	}
}

func landingPageHTML(healthPath string) string {
	escapedVersion := html.EscapeString(Version)
	escapedHealthPath := html.EscapeString(healthPath)
	return "<html><head><title>Oracle DB Performance Scraper " + escapedVersion + "</title></head><body><h1>Oracle DB Performance Scraper " + escapedVersion + "</h1><p>Writing Oracle metrics to PostgreSQL.</p><p><a href='" + escapedHealthPath + "'>Health</a></p></body></html>"
}

func landingPageHandler(healthPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(landingPageHTML(healthPath)))
	}
}

func listenAddress(addresses *[]string) string {
	if addresses == nil || len(*addresses) == 0 || strings.TrimSpace((*addresses)[0]) == "" {
		return ":9161"
	}
	return (*addresses)[0]
}

func main() {
	configFile, err := parseConfigFile(os.Args[1:], os.Getenv, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	bootstrapLogger, _ := newLogger("info", "logfmt", os.Stderr)
	config := &collector.Config{ConfigFile: configFile}
	m, err := collector.LoadMetricsConfiguration(bootstrapLogger, config)
	if err != nil {
		bootstrapLogger.Error("unable to load metrics configuration file", "error", err)
		os.Exit(1)
	}

	logger, err := newLogger(m.Logging.Level, m.Logging.Format, os.Stderr)
	if err != nil {
		bootstrapLogger.Error("invalid logging configuration", "error", err)
		os.Exit(1)
	}

	freeOSMemInterval, enableFree := os.LookupEnv("FREE_INTERVAL")
	if enableFree {
		logger.Info("FREE_INTERVAL env var is present, so will attempt to release OS memory", "free_interval", freeOSMemInterval)
	} else {
		logger.Info("FREE_INTERVAL end var is not present, will not periodically attempt to release memory")
	}

	restartInterval, enableRestart := os.LookupEnv("RESTART_INTERVAL")
	if enableRestart {
		logger.Info("RESTART_INTERVAL env var is present, so will restart my own process periodically", "restart_interval", restartInterval)
	} else {
		logger.Info("RESTART_INTERVAL env var is not present, so will not restart myself periodically")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scraper := collector.NewScraper(logger, m)
	sink, err := postgresql.New(ctx, logger, m.Output.PostgreSQL)
	if err != nil {
		logger.Error("unable to initialize PostgreSQL output", "error", err)
		os.Exit(1)
	}
	defer sink.Close()

	logger.Info("Starting oracledb_performance_scraper", "version", Version)
	logger.Info("Writing metrics to PostgreSQL",
		"samples_table", m.Output.PostgreSQL.SamplesTable,
		"sql_samples_table", m.Output.PostgreSQL.SQLSamplesTable,
		"session_samples_table", m.Output.PostgreSQL.SessionSamplesTable,
		"blocking_sessions_table", m.Output.PostgreSQL.BlockingSessionsTable,
		"database_activity_table", m.Output.PostgreSQL.DatabaseActivityTable)
	logger.Info("SQL execution plan collection configuration",
		"enabled", m.Performance.SQLPlans.GetEnabled(),
		"interval", m.Performance.SQLPlans.GetInterval(),
		"top_n", m.Performance.SQLPlans.GetTopN(),
		"query_timeout", m.Performance.SQLPlans.GetQueryTimeout())
	logger.Info("Database activity collection configuration",
		"source", m.Performance.Activity.GetSource(),
		"interval", m.Performance.Activity.GetInterval(),
		"query_timeout", m.Performance.Activity.GetQueryTimeout())
	logger.Info("Operational collection configuration",
		"enabled", m.Operational.GetEnabled(),
		"interval", m.Operational.GetInterval(),
		"query_timeout", m.Operational.GetQueryTimeout())
	if m.Performance.Activity.GetSource() == "ash" {
		logger.Warn("Oracle ASH collection is enabled; the operator is responsible for verifying Oracle Diagnostics Pack licensing")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", landingPageHandler("/healthz"))

	// start a ticker to cause rebirth
	if enableRestart {
		duration, err := time.ParseDuration(restartInterval)
		if err != nil {
			logger.Info("Could not parse RESTART_INTERVAL, so ignoring it")
		} else if duration <= 0 {
			logger.Info("RESTART_INTERVAL must be greater than zero, so ignoring it")
		} else {
			ticker := time.NewTicker(duration)
			defer ticker.Stop()

			go func() {
				<-ticker.C
				logger.Info("Restarting the process...")
				executable, _ := os.Executable()
				execErr := syscall.Exec(executable, os.Args, os.Environ())
				if execErr != nil {
					panic(execErr)
				}
			}()
		}
	}

	// start a ticker to free OS memory
	if enableFree {
		duration, err := time.ParseDuration(freeOSMemInterval)
		if err != nil {
			logger.Info("Could not parse FREE_INTERVAL, so ignoring it")
		} else if duration <= 0 {
			logger.Info("FREE_INTERVAL must be greater than zero, so ignoring it")
		} else {
			memTicker := time.NewTicker(duration)
			defer memTicker.Stop()

			go func() {
				for {
					<-memTicker.C
					logger.Info("attempting to free OS memory")
					debug.FreeOSMemory()
				}
			}()
		}

	}

	// start the log scraper
	if m.LogDisable() == 1 {
		logger.Info("log.disable set to 1, so will not export the alert logs")
	} else {
		if m.LogPerDatabaseFiles() {
			logger.Info(fmt.Sprintf("Writing an alert log file per database to %s", filepath.Dir(m.LogDestination())))
		} else {
			logger.Info(fmt.Sprintf("Writing alert logs to %s", m.LogDestination()))
		}
		logTicker := time.NewTicker(m.LogInterval())
		defer logTicker.Stop()

		go func() {
			for {
				<-logTicker.C
				logger.Debug("updating alert log")
				for _, db := range scraper.GetDBs() {
					alertlog.UpdateLog(m.LogDestination(), m.LogPerDatabaseFiles(), logger, db)
				}

			}
		}()
	}

	server := &http.Server{
		Addr:              listenAddress(m.Web.ListenAddresses),
		ReadHeaderTimeout: m.Web.GetReadHeaderTimeout(),
		ReadTimeout:       m.Web.GetReadTimeout(),
		IdleTimeout:       m.Web.GetIdleTimeout(),
		Handler:           mux,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("Starting health server", "address", server.Addr)
		serverErr <- server.ListenAndServe()
	}()

	go scraper.InitializeDatabases()
	go scraper.RunScheduledScrapes(ctx, sink)
	go scraper.RunActivitySampling(ctx, sink)

	select {
	case <-ctx.Done():
		logger.Info("Shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown error", "error", err)
		}
	case err := <-serverErr:
		if err == http.ErrServerClosed {
			return
		}
		logger.Error("Listening error", "error", err)
		os.Exit(1)
	}
}
