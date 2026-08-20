package zabbix_sender

import (
	"testing"
	"time"
)

func TestNewMetricValue(t *testing.T) {
	tests := []struct {
		value interface{}
		want  string
	}{
		{42, "42"},
		{3.14, "3.14"},
		{"text", "text"},
		{true, "true"},
		{int64(-7), "-7"},
	}
	for _, tt := range tests {
		m := NewMetricValue("host1", "key1", tt.value, false)
		if m.Value != tt.want {
			t.Errorf("NewMetricValue(%v): expected %q, got %q", tt.value, tt.want, m.Value)
		}
	}

	now := time.Now()
	m := NewMetricValue("host1", "key1", 1, true, now)
	if !m.Active {
		t.Error("Active flag should be set")
	}
	if m.Clock != now.Unix() {
		t.Errorf("Clock: expected %d, got %d", now.Unix(), m.Clock)
	}
}

func TestSendValue(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	done := make(chan error, 1)
	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		request, err := mock.readZabbixRequest(conn)
		if err != nil {
			done <- err
			return
		}
		if request.Request != "sender data" {
			done <- errAssert("request", "sender data", request.Request)
			return
		}
		if len(request.Data) != 1 || request.Data[0].Value != "42" || request.Data[0].Key != "answer" {
			done <- errAssert("data", `answer=42`, request.Data)
			return
		}
		done <- mock.writeZabbixResponse(conn, successResp)
	}()

	s := NewSender(mock.address)
	res, err := s.SendValue("host1", "answer", 42)
	if err != nil {
		t.Fatalf("SendValue: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}
