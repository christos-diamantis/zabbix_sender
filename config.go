package zabbix_sender

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
)

// AgentConfig holds the sender-relevant values of a zabbix_agentd.conf /
// zabbix_agent2.conf file.
type AgentConfig struct {
	// ServerActive holds the parsed ServerActive value: one entry per
	// independent destination (comma-separated in the config file), each
	// entry a list of HA nodes (semicolon-separated) with ports
	// normalized. Every destination is meant to receive a full copy of
	// the data; within a destination only the first reachable node is.
	ServerActive [][]string

	// Hostname is the agent's Hostname, useful as the default Metric host.
	Hostname string

	// SourceIP is the local address to bind outgoing connections to.
	SourceIP string

	// TLS holds every TLS* key of the config file verbatim, with
	// lowercased key names ("tlsconnect", "tlscafile", "tlscertfile",
	// "tlskeyfile", "tlspskfile", "tlspskidentity", ...).
	TLS map[string]string
}

// ParseAgentConfig reads a Zabbix agent configuration file (flat Key=Value
// lines, # comments). It extracts ServerActive (falling back to Server, then
// to 127.0.0.1:10051), Hostname, SourceIP, and all TLS* keys. Keys are
// matched case-insensitively; Include directives are not followed.
func ParseAgentConfig(path string) (*AgentConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config file: %w", err)
	}
	defer f.Close()

	values := map[string]string{}
	cfg := &AgentConfig{TLS: map[string]string{}}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		values[key] = value
		if strings.HasPrefix(key, "tls") {
			cfg.TLS[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	serverActive := values["serveractive"]
	if serverActive == "" {
		serverActive = values["server"]
	}
	if serverActive == "" {
		serverActive = "127.0.0.1:" + defaultPort
	}

	// comma = independent destinations, semicolon = HA nodes of one destination
	for _, cluster := range strings.Split(serverActive, ",") {
		var nodes []string
		for _, node := range strings.Split(cluster, ";") {
			if node = normalizeHost(node); node != "" {
				nodes = append(nodes, node)
			}
		}
		if len(nodes) > 0 {
			cfg.ServerActive = append(cfg.ServerActive, nodes)
		}
	}
	if len(cfg.ServerActive) == 0 {
		return nil, fmt.Errorf("config file %s contains no usable ServerActive/Server address", path)
	}

	cfg.Hostname = values["hostname"]
	cfg.SourceIP = values["sourceip"]

	return cfg, nil
}

// TLSClientConfig builds a tls.Config from the config file's TLS* keys.
// It returns (nil, nil) when TLSConnect is absent or "unencrypted", a
// certificate-based config for TLSConnect=cert, and an error for
// TLSConnect=psk: Go's crypto/tls has no TLS-PSK support, use
// Sender.DialFunc with a PSK-capable transport instead.
func (c *AgentConfig) TLSClientConfig() (*tls.Config, error) {
	switch mode := c.TLS["tlsconnect"]; mode {
	case "", "unencrypted":
		return nil, nil
	case "cert":
		return TLSConfigFromFiles(c.TLS["tlscafile"], c.TLS["tlscertfile"], c.TLS["tlskeyfile"])
	case "psk":
		return nil, fmt.Errorf("TLSConnect=psk is not supported by Go's crypto/tls; provide a PSK-capable transport via Sender.DialFunc")
	default:
		return nil, fmt.Errorf("unsupported TLSConnect value %q", mode)
	}
}

// NewSenderFromConfig creates one Sender per ServerActive destination of a
// Zabbix agent configuration file, with SourceIP and certificate-based TLS
// applied. Every returned Sender is meant to receive a full copy of the
// data (Zabbix agent semantics for comma-separated ServerActive entries).
func NewSenderFromConfig(path string) ([]*Sender, error) {
	cfg, err := ParseAgentConfig(path)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := cfg.TLSClientConfig()
	if err != nil {
		return nil, err
	}

	senders := make([]*Sender, 0, len(cfg.ServerActive))
	for _, nodes := range cfg.ServerActive {
		s := NewSenderHosts(nodes)
		s.SourceIP = cfg.SourceIP
		s.TLSConfig = tlsConfig
		senders = append(senders, s)
	}
	return senders, nil
}
