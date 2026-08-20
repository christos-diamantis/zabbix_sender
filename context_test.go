package zabbix_sender

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := NewSender(deadAddress(t))
	_, err := s.SendContext(ctx, NewPacket([]*Metric{NewMetric("h", "k", "1", false)}, false))
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in chain, got: %v", err)
	}
}

func TestSendContextCancelDuringRead(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	hold := make(chan struct{})
	defer close(hold)
	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := mock.readZabbixRequest(conn); err != nil {
			return
		}
		<-hold // never answer
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	s := NewSender(mock.address) // default 15s read timeout: only ctx can end this early
	start := time.Now()
	_, err := s.SendContext(ctx, NewPacket([]*Metric{NewMetric("h", "k", "1", false)}, false))
	if err == nil {
		t.Fatal("expected error after cancellation")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("cancellation should abort the read quickly, took %v", elapsed)
	}
}

func TestSendContextDeadlineBeatsReadTimeout(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	hold := make(chan struct{})
	defer close(hold)
	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := mock.readZabbixRequest(conn); err != nil {
			return
		}
		<-hold // never answer
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	s := NewSender(mock.address) // default 15s read timeout
	start := time.Now()
	_, err := s.SendContext(ctx, NewPacket([]*Metric{NewMetric("h", "k", "1", false)}, false))
	if err == nil {
		t.Fatal("expected deadline error")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("context deadline should beat the read timeout, took %v", elapsed)
	}
}

func TestSendMetricsContext(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveResponses(mock, successResp)

	m := NewMetric("host1", "key1", "1", false)
	s := NewSender(mock.address)

	_, _, resTrapper, errTrapper := s.SendMetricsContext(context.Background(), []*Metric{m})
	if errTrapper != nil {
		t.Fatalf("SendMetricsContext: %v", errTrapper)
	}
	if resTrapper.Response != "success" {
		t.Errorf("expected success, got %q", resTrapper.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}
