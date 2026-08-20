package zabbix_sender

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// serveCountedResponses accepts one connection per expected chunk, asserts
// the number of metrics in each request, and reports per-chunk statistics.
func serveCountedResponses(mock *mockZabbixServer, wantCounts []int) <-chan error {
	done := make(chan error, 1)
	go func() {
		for i, want := range wantCounts {
			conn, err := mock.listener.Accept()
			if err != nil {
				done <- err
				return
			}
			request, err := mock.readZabbixRequest(conn)
			if err != nil {
				conn.Close()
				done <- err
				return
			}
			if len(request.Data) != want {
				conn.Close()
				done <- fmt.Errorf("chunk %d: expected %d metrics, got %d", i+1, want, len(request.Data))
				return
			}
			resp := fmt.Sprintf(`{"response":"success","info":"processed: %d; failed: 0; total: %d; seconds spent: 0.000100"}`, want, want)
			if err := mock.writeZabbixResponse(conn, resp); err != nil {
				conn.Close()
				done <- err
				return
			}
			conn.Close()
		}
		done <- nil
	}()
	return done
}

func makeMetrics(n int) []*Metric {
	metrics := make([]*Metric, 0, n)
	for i := 0; i < n; i++ {
		metrics = append(metrics, NewMetric("host1", fmt.Sprintf("key%d", i), "1", false))
	}
	return metrics
}

func TestSendChunked(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveCountedResponses(mock, []int{2, 2, 1})

	s := NewSender(mock.address)
	s.ChunkSize = 2

	res, err := s.Send(NewPacket(makeMetrics(5), false))
	if err != nil {
		t.Fatalf("chunked send: %v", err)
	}

	info, err := res.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo on aggregated response: %v", err)
	}
	if info.Processed != 5 || info.Total != 5 || info.Failed != 0 {
		t.Errorf("aggregate: expected processed=5 failed=0 total=5, got %+v", info)
	}
	if info.Spent <= 0 {
		t.Errorf("aggregate spent should be positive, got %v", info.Spent)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendChunkedExactMultiple(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveCountedResponses(mock, []int{3, 3})

	s := NewSender(mock.address)
	s.ChunkSize = 3

	res, err := s.Send(NewPacket(makeMetrics(6), false))
	if err != nil {
		t.Fatalf("chunked send: %v", err)
	}
	info, err := res.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.Total != 6 {
		t.Errorf("expected total=6, got %d", info.Total)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendNotChunkedBelowLimit(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveCountedResponses(mock, []int{5}) // single packet

	s := NewSender(mock.address)
	s.ChunkSize = 10

	if _, err := s.Send(NewPacket(makeMetrics(5), false)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendChunkingDisabled(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveCountedResponses(mock, []int{300}) // one packet despite > DefaultChunkSize

	s := NewSender(mock.address)
	s.ChunkSize = -1

	if _, err := s.Send(NewPacket(makeMetrics(300), false)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendChunkedDefaultSize(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveCountedResponses(mock, []int{250, 50})

	s := NewSender(mock.address) // ChunkSize 0 -> DefaultChunkSize (250)

	res, err := s.Send(NewPacket(makeMetrics(300), false))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	info, err := res.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.Total != 300 {
		t.Errorf("expected total=300, got %d", info.Total)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendChunkedErrorNamesChunk(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	go func() {
		// answer the first chunk, close the second without answering
		for i := 0; i < 2; i++ {
			conn, err := mock.listener.Accept()
			if err != nil {
				return
			}
			if _, err := mock.readZabbixRequest(conn); err != nil {
				conn.Close()
				return
			}
			if i == 0 {
				mock.writeZabbixResponse(conn, successResp)
			}
			conn.Close()
		}
	}()

	s := NewSender(mock.address)
	s.ChunkSize = 1
	s.ReadTimeout = 200 * time.Millisecond // keep the failover retry short

	_, err := s.Send(NewPacket(makeMetrics(2), false))
	if err == nil {
		t.Fatal("expected error when a chunk fails")
	}
	if !strings.Contains(err.Error(), "chunk 2/2") {
		t.Errorf("error should name the failing chunk, got: %v", err)
	}
}
