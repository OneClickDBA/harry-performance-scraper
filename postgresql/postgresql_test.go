// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package postgresql

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPartitionDay(t *testing.T) {
	tests := []struct {
		name      string
		parent    pgx.Identifier
		partition string
		want      time.Time
		wantOK    bool
	}{
		{
			name:      "generated partition",
			parent:    pgx.Identifier{"monitoring", "oracle_metric_samples"},
			partition: "oracle_metric_samples_2026_06_26",
			want:      time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC),
			wantOK:    true,
		},
		{
			name:      "unrelated partition",
			parent:    pgx.Identifier{"oracle_metric_samples"},
			partition: "other_table_2026_06_26",
			wantOK:    false,
		},
		{
			name:      "invalid date suffix",
			parent:    pgx.Identifier{"oracle_metric_samples"},
			partition: "oracle_metric_samples_latest",
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := partitionDay(tt.parent, tt.partition)
			if ok != tt.wantOK {
				t.Fatalf("expected ok=%t, got %t", tt.wantOK, ok)
			}
			if ok && !got.Equal(tt.want) {
				t.Fatalf("expected day %s, got %s", tt.want, got)
			}
		})
	}
}
