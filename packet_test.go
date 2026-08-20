package zabbix_sender

import (
	"encoding/binary"
	"encoding/json"
	"testing"
)

func TestDataLen(t *testing.T) {
	p := NewPacket([]*Metric{NewMetric("host1", "key1", "42", false)}, false)

	jsonData, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshaling packet: %v", err)
	}

	dataLen := p.DataLen()
	if len(dataLen) != 8 {
		t.Fatalf("DataLen should return 8 bytes, got %d", len(dataLen))
	}

	got := binary.LittleEndian.Uint64(dataLen)
	if got != uint64(len(jsonData)) {
		t.Errorf("DataLen: expected %d, got %d", len(jsonData), got)
	}
}

func TestNewPacketRequestType(t *testing.T) {
	metrics := []*Metric{NewMetric("host1", "key1", "42", false)}

	if p := NewPacket(metrics, true); p.Request != "agent data" {
		t.Errorf("active packet: expected request 'agent data', got %q", p.Request)
	}
	if p := NewPacket(metrics, false); p.Request != "sender data" {
		t.Errorf("trapper packet: expected request 'sender data', got %q", p.Request)
	}
}

// TestMetricJSON verifies the wire format: the Active flag must not be
// serialized and clock/ns must be omitted when no timestamp was given.
func TestMetricJSON(t *testing.T) {
	m := NewMetric("host1", "key1", "42", true)

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshaling metric: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshaling metric: %v", err)
	}

	for _, field := range []string{"active", "Active", "clock", "ns"} {
		if _, ok := raw[field]; ok {
			t.Errorf("field %q should not be serialized: %s", field, data)
		}
	}
	for _, field := range []string{"host", "key", "value"} {
		if _, ok := raw[field]; !ok {
			t.Errorf("field %q missing from wire format: %s", field, data)
		}
	}
}
