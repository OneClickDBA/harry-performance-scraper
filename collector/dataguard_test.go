// Copyright (c) 2026, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

package collector

import "testing"

func TestParseDataGuardInterval(t *testing.T) {
	tests := []struct {
		value string
		want  *float64
	}{
		{value: "+00 00:01:30", want: float64TestPtr(90)},
		{value: "+01 02:03:04.5", want: float64TestPtr(93784.5)},
		{value: "-00 00:00:02", want: float64TestPtr(-2)},
		{value: "12.5", want: float64TestPtr(12.5)},
		{value: "UNKNOWN", want: nil},
	}
	for _, test := range tests {
		got, err := parseDataGuardInterval(test.value)
		if err != nil {
			t.Fatalf("parseDataGuardInterval(%q): %v", test.value, err)
		}
		if test.want == nil {
			if got != nil {
				t.Fatalf("parseDataGuardInterval(%q) = %v, want nil", test.value, *got)
			}
			continue
		}
		if got == nil || *got != *test.want {
			t.Fatalf("parseDataGuardInterval(%q) = %v, want %v", test.value, got, *test.want)
		}
	}
}

func float64TestPtr(value float64) *float64 { return &value }
