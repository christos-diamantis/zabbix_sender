package zabbix_sender

import (
	"context"
	"sync"
)

// MultiSender fans metrics out to multiple independent destinations, each a
// Sender with its own HA node list — the Zabbix agent semantics for
// comma-separated ServerActive entries: every destination receives a full
// copy of the data.
type MultiSender struct {
	Senders []*Sender
}

// ClusterResult is the outcome of one destination's send.
type ClusterResult struct {
	Sender     *Sender
	ResActive  Response
	ErrActive  error
	ResTrapper Response
	ErrTrapper error
}

// Err returns the first non-nil error of the result.
func (r *ClusterResult) Err() error {
	if r.ErrActive != nil {
		return r.ErrActive
	}
	return r.ErrTrapper
}

// NewMultiSender creates a MultiSender from destination clusters, each a
// list of HA nodes (ports normalized, IPv6 supported).
func NewMultiSender(clusters [][]string) *MultiSender {
	m := &MultiSender{}
	for _, nodes := range clusters {
		m.Senders = append(m.Senders, NewSenderHosts(nodes))
	}
	return m
}

// NewMultiSenderFromConfig creates a MultiSender from a Zabbix agent
// configuration file (see NewSenderFromConfig).
func NewMultiSenderFromConfig(path string) (*MultiSender, error) {
	senders, err := NewSenderFromConfig(path)
	if err != nil {
		return nil, err
	}
	return &MultiSender{Senders: senders}, nil
}

// SendMetrics sends the metrics to every destination concurrently and
// returns one result per destination, in Senders order.
func (m *MultiSender) SendMetrics(metrics []*Metric) []ClusterResult {
	return m.SendMetricsContext(context.Background(), metrics)
}

// SendMetricsContext is SendMetrics honoring the context's deadline and
// cancellation on top of each Sender's own timeouts.
func (m *MultiSender) SendMetricsContext(ctx context.Context, metrics []*Metric) []ClusterResult {
	results := make([]ClusterResult, len(m.Senders))

	var wg sync.WaitGroup
	for i, sender := range m.Senders {
		wg.Add(1)
		go func(i int, sender *Sender) {
			defer wg.Done()
			r := ClusterResult{Sender: sender}
			r.ResActive, r.ErrActive, r.ResTrapper, r.ErrTrapper = sender.SendMetricsContext(ctx, metrics)
			results[i] = r
		}(i, sender)
	}
	wg.Wait()

	return results
}
