// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLandingPageHTMLEscapesHealthPath(t *testing.T) {
	originalVersion := Version
	Version = "test"
	t.Cleanup(func() {
		Version = originalVersion
	})

	body := landingPageHTML("/healthz' onclick='alert(1)")

	if strings.Contains(body, "onclick='alert(1)") {
		t.Fatalf("expected landing page to escape health path, got %q", body)
	}
	if !strings.Contains(body, "href='/healthz&#39; onclick=&#39;alert(1)'") {
		t.Fatalf("expected escaped health path in href, got %q", body)
	}
}

func TestReadinessHandlerReflectsLeadership(t *testing.T) {
	var ready atomic.Bool
	handler := readinessHandler(&ready)

	standby := httptest.NewRecorder()
	handler(standby, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if standby.Code != http.StatusServiceUnavailable || standby.Body.String() != "standby\n" {
		t.Fatalf("standby response = %d %q", standby.Code, standby.Body.String())
	}

	ready.Store(true)
	active := httptest.NewRecorder()
	handler(active, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if active.Code != http.StatusOK || active.Body.String() != "ready\n" {
		t.Fatalf("active response = %d %q", active.Code, active.Body.String())
	}
}

func TestParseConfigFileExplicitFlag(t *testing.T) {
	got, err := parseConfigFile([]string{"--config.file", "example-config.yaml"}, emptyEnv, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected config flag to parse, got %v", err)
	}
	if got != "example-config.yaml" {
		t.Fatalf("expected explicit config file, got %q", got)
	}
}

func TestParseConfigFileFromEnvironment(t *testing.T) {
	got, err := parseConfigFile(nil, func(key string) string {
		if key == "CONFIG_FILE" {
			return "env-config.yaml"
		}
		return ""
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected CONFIG_FILE fallback to parse, got %v", err)
	}
	if got != "env-config.yaml" {
		t.Fatalf("expected environment config file, got %q", got)
	}
}

func TestParseConfigFileRequiresConfig(t *testing.T) {
	_, err := parseConfigFile(nil, emptyEnv, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected missing config file to fail")
	}
	if !strings.Contains(err.Error(), "config file is required") {
		t.Fatalf("expected required config error, got %v", err)
	}
}

func TestParseConfigFileRejectsRemovedFlags(t *testing.T) {
	_, err := parseConfigFile([]string{"--web.telemetry-path", "/metrics"}, emptyEnv, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected removed flag to fail")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("expected unknown flag error, got %v", err)
	}
}

func emptyEnv(string) string {
	return ""
}
