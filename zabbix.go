// Package zabbix_sender implements Zabbix sender protocol with proxy group redirects and multi-host HA support.
package zabbix_sender

import (
	"context"
	"fmt"
	"time"
)

const (
	defaultConnectTimeout = 5 * time.Second
	defaultWriteTimeout   = 5 * time.Second
	defaultReadTimeout    = 15 * time.Second
	defaultMaxRedirects   = 3
	defaultUpdateHost     = false
)

// Metric represents a Zabbix metric.
type Metric struct {
	Host   string `json:"host"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Clock  int64  `json:"clock,omitempty"`
	NS     int    `json:"ns,omitempty"`
	Active bool   `json:"-"`
}

// NewMetric creates a Zabbix metric.
//
// agentActive=true for active agent items ("agent data"),
// agentActive=false for trapper items ("sender data").
// t optionally sets custom timestamp.
func NewMetric(host, key, value string, agentActive bool, t ...time.Time) *Metric {
	m := &Metric{Host: host, Key: key, Value: value, Active: agentActive}
	if len(t) > 0 {
		m.Clock = t[0].Unix()
		m.NS = t[0].Nanosecond()
	}
	return m
}

// NewMetricValue is NewMetric for values of any type: numbers, booleans,
// fmt.Stringer implementations, etc. are formatted with fmt.Sprint (the
// Zabbix protocol carries all values as strings).
func NewMetricValue(host, key string, value interface{}, agentActive bool, t ...time.Time) *Metric {
	return NewMetric(host, key, fmt.Sprint(value), agentActive, t...)
}

// NewSender creates sender for single host.
func NewSender(host string) *Sender {
	return NewSenderHosts([]string{host})
}

// NewSenderHosts creates sender for multiple hosts (HA or Proxy Group).
func NewSenderHosts(hosts []string) *Sender {
	norm := make([]string, 0, len(hosts))
	for _, h := range hosts {
		norm = append(norm, normalizeHost(h))
	}
	return &Sender{
		Hosts:          norm,
		MaxRedirects:   defaultMaxRedirects,
		UpdateHost:     defaultUpdateHost,
		ConnectTimeout: defaultConnectTimeout,
		ReadTimeout:    defaultReadTimeout,
		WriteTimeout:   defaultWriteTimeout,
	}
}

// NewSenderTimeout creates Sender with custom timeouts.
func NewSenderTimeout(
	host string,
	connectTimeout time.Duration,
	readTimeout time.Duration,
	writeTimeout time.Duration,
) *Sender {
	s := NewSenderHosts([]string{host})
	s.ConnectTimeout = connectTimeout
	s.ReadTimeout = readTimeout
	s.WriteTimeout = writeTimeout
	return s
}

// SendValue sends one trapper value in a single call, formatting value with
// fmt.Sprint (like python-zabbix-utils' send_value).
func (s *Sender) SendValue(host, key string, value interface{}) (Response, error) {
	return s.SendValueContext(context.Background(), host, key, value)
}

// SendValueContext is SendValue honoring the context's deadline and
// cancellation on top of the Sender's own timeouts.
func (s *Sender) SendValueContext(ctx context.Context, host, key string, value interface{}) (Response, error) {
	packet := NewPacket([]*Metric{NewMetricValue(host, key, value, false)}, false)
	return s.SendContext(ctx, packet)
}
