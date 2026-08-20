package zabbix_sender

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// RedirectInfo struct.
type RedirectInfo struct {
	Revision int    `json:"revision"`
	Address  string `json:"address"`
}

// Response is the response struct from Zabbix server/proxy.
type Response struct {
	Response string        `json:"response"`
	Info     string        `json:"info"`
	Redirect *RedirectInfo `json:"redirect,omitempty"`
}

// ResponseInfo struct holds parsed statistics from response "info" field.
type ResponseInfo struct {
	Processed int
	Failed    int
	Total     int
	Spent     time.Duration
}

// parseHostPort validates and returns a normalized host:port address.
func parseHostPort(addr string) (string, error) {
	addr = normalizeHost(addr)
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("invalid redirect address: %s", addr)
	}
	return addr, nil
}

// defaultPort is the standard Zabbix trapper port.
const defaultPort = "10051"

// normalizeHost ensures the address has a port; defaults to 10051 if missing.
// Bare IPv6 literals are bracketed so the address is dialable.
func normalizeHost(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return addr
	}
	// bracketed IPv6, with or without port: "[::1]:10051" or "[::1]"
	if strings.HasPrefix(addr, "[") {
		if strings.Contains(addr, "]:") {
			return addr
		}
		return addr + ":" + defaultPort
	}
	// bare IPv6 literal: more than one colon
	if strings.Count(addr, ":") > 1 {
		return "[" + addr + "]:" + defaultPort
	}
	if strings.Contains(addr, ":") {
		return addr
	}
	return addr + ":" + defaultPort
}

// GetInfo parses success response "info" field into statistics.
func (r *Response) GetInfo() (*ResponseInfo, error) {
	ret := new(ResponseInfo)

	if r.Response != "success" {
		return nil, fmt.Errorf("cannot parse info of a non-success response (%s)", r.Response)
	}

	sp := strings.Split(r.Info, ";")
	if len(sp) != 4 {
		return nil, fmt.Errorf("invalid info format, expected 4 fields got %d (%s)", len(sp), r.Info)
	}
	for i := range sp {
		key, value, found := strings.Cut(sp[i], ":")
		if !found {
			return nil, fmt.Errorf("invalid info field (%s)", sp[i])
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		var err error
		switch key {
		case "processed":
			ret.Processed, err = strconv.Atoi(value)
		case "failed":
			ret.Failed, err = strconv.Atoi(value)
		case "total":
			ret.Total, err = strconv.Atoi(value)
		case "seconds spent":
			var f float64
			f, err = strconv.ParseFloat(value, 64)
			ret.Spent = time.Duration(f * float64(time.Second))
		}
		if err != nil {
			return nil, fmt.Errorf("parsing info field %q value %q: %w", key, value, err)
		}
	}

	return ret, nil
}
