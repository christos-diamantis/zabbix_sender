package zabbix_sender

import (
	"testing"
	"time"
)

func TestMultiSenderSendsToAllDestinations(t *testing.T) {
	mock1 := newMockZabbixServer(t)
	defer mock1.Close()
	done1 := serveResponses(mock1, successResp)

	mock2 := newMockZabbixServer(t)
	defer mock2.Close()
	done2 := serveResponses(mock2, successResp)

	m := NewMultiSender([][]string{{mock1.address}, {mock2.address}})
	if len(m.Senders) != 2 {
		t.Fatalf("expected 2 senders, got %d", len(m.Senders))
	}

	results := m.SendMetrics([]*Metric{NewMetric("host1", "key1", "1", false)})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if err := r.Err(); err != nil {
			t.Errorf("destination %d: %v", i, err)
		}
		if r.ResTrapper.Response != "success" {
			t.Errorf("destination %d: expected success, got %q", i, r.ResTrapper.Response)
		}
	}

	// both destinations must have received the data
	if err := <-done1; err != nil {
		t.Fatalf("mock1 error: %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("mock2 error: %v", err)
	}
}

func TestMultiSenderPartialFailure(t *testing.T) {
	live := newMockZabbixServer(t)
	defer live.Close()
	done := serveResponses(live, successResp)

	dead := deadAddress(t)

	m := NewMultiSender([][]string{{live.address}, {dead}})
	m.Senders[1].ConnectTimeout = 500 * time.Millisecond

	results := m.SendMetrics([]*Metric{NewMetric("host1", "key1", "1", false)})

	if err := results[0].Err(); err != nil {
		t.Errorf("live destination should succeed: %v", err)
	}
	if results[1].Err() == nil {
		t.Error("dead destination should fail")
	}

	if err := <-done; err != nil {
		t.Fatalf("mock error: %v", err)
	}
}

func TestMultiSenderHAWithinDestination(t *testing.T) {
	live := newMockZabbixServer(t)
	defer live.Close()
	done := serveResponses(live, successResp)

	dead := deadAddress(t)

	// one destination with two HA nodes: only the reachable one gets data
	m := NewMultiSender([][]string{{dead, live.address}})
	m.Senders[0].ConnectTimeout = 500 * time.Millisecond

	results := m.SendMetrics([]*Metric{NewMetric("host1", "key1", "1", false)})
	if err := results[0].Err(); err != nil {
		t.Fatalf("HA failover within destination should succeed: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock error: %v", err)
	}
}

func TestNewMultiSenderFromConfig(t *testing.T) {
	mock1 := newMockZabbixServer(t)
	defer mock1.Close()
	done1 := serveResponses(mock1, successResp)

	mock2 := newMockZabbixServer(t)
	defer mock2.Close()
	done2 := serveResponses(mock2, successResp)

	path := writeConfig(t, "ServerActive="+mock1.address+","+mock2.address+"\n")

	m, err := NewMultiSenderFromConfig(path)
	if err != nil {
		t.Fatalf("NewMultiSenderFromConfig: %v", err)
	}

	results := m.SendMetrics([]*Metric{NewMetric("host1", "key1", "1", false)})
	for i, r := range results {
		if err := r.Err(); err != nil {
			t.Errorf("destination %d: %v", i, err)
		}
	}

	if err := <-done1; err != nil {
		t.Fatalf("mock1 error: %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("mock2 error: %v", err)
	}
}
