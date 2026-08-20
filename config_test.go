package zabbix_sender

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "zabbix_agentd.conf")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	return path
}

func TestParseAgentConfig(t *testing.T) {
	path := writeConfig(t, `
# This is a comment
Server=192.0.2.1

# comma = independent destinations, semicolon = HA nodes
ServerActive=zbx1.example.com;zbx2.example.com:20051,standalone.example.com
Hostname=my-agent
SourceIP=192.0.2.10

TLSConnect=cert
TLSCAFile=/etc/zabbix/ca.pem
TLSCertFile=/etc/zabbix/cert.pem
TLSKeyFile=/etc/zabbix/key.pem
`)

	cfg, err := ParseAgentConfig(path)
	if err != nil {
		t.Fatalf("ParseAgentConfig: %v", err)
	}

	wantClusters := [][]string{
		{"zbx1.example.com:10051", "zbx2.example.com:20051"},
		{"standalone.example.com:10051"},
	}
	if !reflect.DeepEqual(cfg.ServerActive, wantClusters) {
		t.Errorf("ServerActive: expected %v, got %v", wantClusters, cfg.ServerActive)
	}
	if cfg.Hostname != "my-agent" {
		t.Errorf("Hostname: expected my-agent, got %q", cfg.Hostname)
	}
	if cfg.SourceIP != "192.0.2.10" {
		t.Errorf("SourceIP: expected 192.0.2.10, got %q", cfg.SourceIP)
	}
	wantTLS := map[string]string{
		"tlsconnect":  "cert",
		"tlscafile":   "/etc/zabbix/ca.pem",
		"tlscertfile": "/etc/zabbix/cert.pem",
		"tlskeyfile":  "/etc/zabbix/key.pem",
	}
	if !reflect.DeepEqual(cfg.TLS, wantTLS) {
		t.Errorf("TLS: expected %v, got %v", wantTLS, cfg.TLS)
	}
}

func TestParseAgentConfigFallbacks(t *testing.T) {
	// no ServerActive: falls back to Server
	path := writeConfig(t, "Server=zbx.example.com:20051\n")
	cfg, err := ParseAgentConfig(path)
	if err != nil {
		t.Fatalf("ParseAgentConfig: %v", err)
	}
	want := [][]string{{"zbx.example.com:20051"}}
	if !reflect.DeepEqual(cfg.ServerActive, want) {
		t.Errorf("expected fallback to Server (%v), got %v", want, cfg.ServerActive)
	}

	// neither: falls back to localhost
	path = writeConfig(t, "Hostname=lonely\n")
	cfg, err = ParseAgentConfig(path)
	if err != nil {
		t.Fatalf("ParseAgentConfig: %v", err)
	}
	want = [][]string{{"127.0.0.1:10051"}}
	if !reflect.DeepEqual(cfg.ServerActive, want) {
		t.Errorf("expected localhost fallback (%v), got %v", want, cfg.ServerActive)
	}
}

func TestParseAgentConfigCaseInsensitive(t *testing.T) {
	path := writeConfig(t, "serveractive=zbx.example.com\nHOSTNAME=shouty\n")
	cfg, err := ParseAgentConfig(path)
	if err != nil {
		t.Fatalf("ParseAgentConfig: %v", err)
	}
	if cfg.ServerActive[0][0] != "zbx.example.com:10051" {
		t.Errorf("lowercase serveractive should be honored, got %v", cfg.ServerActive)
	}
	if cfg.Hostname != "shouty" {
		t.Errorf("uppercase HOSTNAME should be honored, got %q", cfg.Hostname)
	}
}

func TestParseAgentConfigIPv6(t *testing.T) {
	path := writeConfig(t, "ServerActive=::1;[2001:db8::1]:20051\n")
	cfg, err := ParseAgentConfig(path)
	if err != nil {
		t.Fatalf("ParseAgentConfig: %v", err)
	}
	want := [][]string{{"[::1]:10051", "[2001:db8::1]:20051"}}
	if !reflect.DeepEqual(cfg.ServerActive, want) {
		t.Errorf("expected %v, got %v", want, cfg.ServerActive)
	}
}

func TestParseAgentConfigMissingFile(t *testing.T) {
	if _, err := ParseAgentConfig(filepath.Join(t.TempDir(), "missing.conf")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestTLSClientConfigModes(t *testing.T) {
	// unencrypted / absent
	cfg := &AgentConfig{TLS: map[string]string{}}
	if tlsCfg, err := cfg.TLSClientConfig(); err != nil || tlsCfg != nil {
		t.Errorf("absent TLSConnect: expected (nil, nil), got (%v, %v)", tlsCfg, err)
	}
	cfg.TLS["tlsconnect"] = "unencrypted"
	if tlsCfg, err := cfg.TLSClientConfig(); err != nil || tlsCfg != nil {
		t.Errorf("unencrypted: expected (nil, nil), got (%v, %v)", tlsCfg, err)
	}

	// psk: clear error pointing at DialFunc
	cfg.TLS["tlsconnect"] = "psk"
	if _, err := cfg.TLSClientConfig(); err == nil || !strings.Contains(err.Error(), "DialFunc") {
		t.Errorf("psk: expected DialFunc guidance error, got %v", err)
	}

	// unknown mode
	cfg.TLS["tlsconnect"] = "quantum"
	if _, err := cfg.TLSClientConfig(); err == nil {
		t.Error("unknown TLSConnect value should error")
	}

	// cert: built from real files
	_, certPEM, keyPEM := testCertificate(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	for file, data := range map[string][]byte{caFile: certPEM, certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}
	cfg.TLS = map[string]string{
		"tlsconnect":  "cert",
		"tlscafile":   caFile,
		"tlscertfile": certFile,
		"tlskeyfile":  keyFile,
	}
	tlsCfg, err := cfg.TLSClientConfig()
	if err != nil {
		t.Fatalf("cert mode: %v", err)
	}
	if tlsCfg == nil || tlsCfg.RootCAs == nil || len(tlsCfg.Certificates) != 1 {
		t.Errorf("cert mode: incomplete tls.Config: %+v", tlsCfg)
	}
}

func TestNewSenderFromConfig(t *testing.T) {
	path := writeConfig(t, `
ServerActive=zbx1;zbx2:20051,standalone
SourceIP=127.0.0.1
`)

	senders, err := NewSenderFromConfig(path)
	if err != nil {
		t.Fatalf("NewSenderFromConfig: %v", err)
	}
	if len(senders) != 2 {
		t.Fatalf("expected 2 senders (one per destination), got %d", len(senders))
	}

	wantHosts := [][]string{
		{"zbx1:10051", "zbx2:20051"},
		{"standalone:10051"},
	}
	for i, s := range senders {
		if !reflect.DeepEqual(s.Hosts, wantHosts[i]) {
			t.Errorf("sender %d hosts: expected %v, got %v", i, wantHosts[i], s.Hosts)
		}
		if s.SourceIP != "127.0.0.1" {
			t.Errorf("sender %d SourceIP: expected 127.0.0.1, got %q", i, s.SourceIP)
		}
		if s.TLSConfig != nil {
			t.Errorf("sender %d: no TLS expected", i)
		}
		if s.ConnectTimeout != defaultConnectTimeout {
			t.Errorf("sender %d: default timeouts expected", i)
		}
	}
}

// TestNewSenderFromConfigEndToEnd parses a config pointing at a live mock
// and sends a metric through the resulting sender.
func TestNewSenderFromConfigEndToEnd(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveResponses(mock, successResp)

	path := writeConfig(t, "ServerActive="+mock.address+"\nHostname=cfg-host\n")

	senders, err := NewSenderFromConfig(path)
	if err != nil {
		t.Fatalf("NewSenderFromConfig: %v", err)
	}
	if len(senders) != 1 {
		t.Fatalf("expected 1 sender, got %d", len(senders))
	}

	res, err := sendTestPacket(senders[0])
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestNewSenderFromConfigPSKError(t *testing.T) {
	path := writeConfig(t, "ServerActive=zbx1\nTLSConnect=psk\nTLSPSKFile=/etc/zabbix/psk\n")
	if _, err := NewSenderFromConfig(path); err == nil || !strings.Contains(err.Error(), "DialFunc") {
		t.Fatalf("expected PSK guidance error, got %v", err)
	}
}
