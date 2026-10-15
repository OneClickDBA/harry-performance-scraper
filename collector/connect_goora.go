// Copyright (c) 2025, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

//go:build goora

package collector

import (
	"database/sql"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"log/slog"
	"net/url"
)

func connect(logger *slog.Logger, dbname string, dbconfig DatabaseConfig) (*sql.DB, error) {
	logger.Debug("Launching connection to "+maskDsn(dbconfig.URL), "database", dbname)

	password, err := dbconfig.GetPassword()
	if err != nil {
		return nil, err
	}
	username, err := dbconfig.GetUsername()
	if err != nil {
		return nil, err
	}
	dbconfig.ExternalAuth = password == ""

	logger.Debug(fmt.Sprintf("external authentication set to %t", dbconfig.ExternalAuth), "database", dbname)

	msg := "Using Username/Password Authentication."
	if dbconfig.ExternalAuth {
		msg = "Database Password not specified; will attempt to use external authentication (ignoring user input)."
		dbconfig.Username = ""
	}
	logger.Info(msg, "database", dbname)

	// Build connection string for go-ora
	var dsn string
	if dbconfig.ExternalAuth {
		// go-ora doesn't directly support external authentication
		// So we rely on OS authentication (set Oracle wallet/env)
		dsn = fmt.Sprintf("oracle://@%s", dbconfig.URL)
	} else if username != "" {
		// url.UserPassword properly percent-encodes special characters in credentials,
		// preventing malformed DSNs when passwords contain @, ?, #, /, % etc.
		userInfo := url.UserPassword(username, password)
		dsn = fmt.Sprintf("oracle://%s@%s", userInfo.String(), dbconfig.URL)
	} else {
		dsn = fmt.Sprintf("oracle://%s", dbconfig.URL)
	}

	// open connection (lazy until first use)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		logger.Error("Failed to create DB handle", "error", err, "database", dbname)
		return nil, err
	}

	// Configure connection pool (sql.DB handles pooling)
	setConnectionPool(logger, dbname, dbconfig, db)
	return db, nil
}

func setConnectionPool(logger *slog.Logger, dbname string, dbconfig DatabaseConfig, db *sql.DB) {
	if dbconfig.GetPoolMaxConnections() > 0 {
		logger.Debug(fmt.Sprintf("set pool max connections to %d", dbconfig.PoolMaxConnections), "database", dbname)
		db.SetMaxOpenConns(dbconfig.GetPoolMaxConnections())
	} else {
		db.SetMaxOpenConns(dbconfig.GetMaxOpenConns())
	}
	if dbconfig.GetPoolMinConnections() > 0 {
		logger.Debug(fmt.Sprintf("set pool min connections to %d", dbconfig.PoolMinConnections), "database", dbname)
		db.SetMaxIdleConns(dbconfig.GetPoolMinConnections())
	} else {
		db.SetMaxIdleConns(dbconfig.GetMaxIdleConns())
	}
}
