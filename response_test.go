package zabbix_sender

import (
	"strings"
	"testing"
	"time"
)

func TestGetInfo(t *testing.T) {
	tests := []struct {
		name     string
		response Response
		want     *ResponseInfo
		wantErr  string
	}{
		{
			name:     "valid info",
			response: Response{Response: "success", Info: "processed: 3; failed: 1; total: 4; seconds spent: 0.000123"},
			want:     &ResponseInfo{Processed: 3, Failed: 1, Total: 4, Spent: 123 * time.Microsecond},
		},
		{
			name:     "non-success response",
			response: Response{Response: "failed", Info: "host not found"},
			wantErr:  "non-success",
		},
		{
			name:     "wrong field count",
			response: Response{Response: "success", Info: "processed: 3; failed: 1"},
			wantErr:  "expected 4 fields",
		},
		{
			name:     "non-numeric processed",
			response: Response{Response: "success", Info: "processed: abc; failed: 0; total: 1; seconds spent: 0.000030"},
			wantErr:  "parsing info field",
		},
		{
			name:     "non-numeric failed",
			response: Response{Response: "success", Info: "processed: 1; failed: x; total: 1; seconds spent: 0.000030"},
			wantErr:  "parsing info field",
		},
		{
			name:     "invalid seconds spent",
			response: Response{Response: "success", Info: "processed: 1; failed: 0; total: 1; seconds spent: fast"},
			wantErr:  "parsing info field",
		},
		{
			name:     "field without colon",
			response: Response{Response: "success", Info: "processed 1; failed: 0; total: 1; seconds spent: 0.1"},
			wantErr:  "invalid info field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.response.GetInfo()

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (result: %+v)", tt.wantErr, got)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if *got != *tt.want {
				t.Errorf("expected %+v, got %+v", tt.want, got)
			}
		})
	}
}

func TestNormalizeHost(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"zabbix-proxy", "zabbix-proxy:10051"},
		{"zabbix-proxy:10051", "zabbix-proxy:10051"},
		{"  zabbix-proxy  ", "zabbix-proxy:10051"},
		{"", ""},
		{"192.168.1.1", "192.168.1.1:10051"},
		{"192.168.1.1:20051", "192.168.1.1:20051"},
		{"::1", "[::1]:10051"},
		{"2001:db8::1", "[2001:db8::1]:10051"},
		{"[::1]", "[::1]:10051"},
		{"[::1]:20051", "[::1]:20051"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := normalizeHost(tt.input); got != tt.expected {
				t.Errorf("normalizeHost(%q): expected %q, got %q", tt.input, tt.expected, got)
			}
		})
	}
}

func TestParseHostPort(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{input: "proxy1", want: "proxy1:10051"},
		{input: "proxy1:20051", want: "proxy1:20051"},
		{input: "::1", want: "[::1]:10051"},
		{input: "proxy1:", wantErr: true},
		{input: ":10051", wantErr: true},
		{input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseHostPort(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
