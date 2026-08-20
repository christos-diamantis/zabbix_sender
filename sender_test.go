package zabbix_sender

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// serveResponses accepts one connection per response, reads the request and
// answers with the corresponding JSON body. The returned channel receives one
// error (or nil) when all responses have been served.
func serveResponses(mock *mockZabbixServer, responses ...string) <-chan error {
	done := make(chan error, 1)
	go func() {
		for _, jsonResp := range responses {
			conn, err := mock.listener.Accept()
			if err != nil {
				done <- err
				return
			}
			if _, err := mock.readZabbixRequest(conn); err != nil {
				conn.Close()
				done <- err
				return
			}
			if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
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

const successResp = `{"response":"success","info":"processed: 1; failed: 0; total: 1; seconds spent: 0.000030"}`

// deadAddress returns an address that refuses connections.
func deadAddress(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestSendRedirect(t *testing.T) {
	target := newMockZabbixServer(t)
	defer target.Close()
	targetDone := serveResponses(target, successResp)

	redirector := newMockZabbixServer(t)
	defer redirector.Close()
	redirectResp := fmt.Sprintf(`{"response":"failed","redirect":{"revision":1,"address":"%s"}}`, target.address)
	redirectorDone := serveResponses(redirector, redirectResp)

	s := NewSender(redirector.address)
	res, err := s.Send(NewPacket([]*Metric{NewMetric("host1", "key1", "1", false)}, false))
	if err != nil {
		t.Fatalf("Send should follow redirect and succeed: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success response, got %q", res.Response)
	}

	// UpdateHost is false: the starting host stays cached
	if s.PrimaryHost() != redirector.address {
		t.Errorf("PrimaryHost: expected starting host %s, got %s", redirector.address, s.PrimaryHost())
	}

	if err := <-redirectorDone; err != nil {
		t.Fatalf("redirector mock error: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target mock error: %v", err)
	}
}

func TestSendRedirectUpdateHost(t *testing.T) {
	target := newMockZabbixServer(t)
	defer target.Close()
	targetDone := serveResponses(target, successResp)

	redirector := newMockZabbixServer(t)
	defer redirector.Close()
	redirectResp := fmt.Sprintf(`{"response":"failed","redirect":{"revision":1,"address":"%s"}}`, target.address)
	redirectorDone := serveResponses(redirector, redirectResp)

	s := NewSender(redirector.address)
	s.UpdateHost = true
	_, err := s.Send(NewPacket([]*Metric{NewMetric("host1", "key1", "1", false)}, false))
	if err != nil {
		t.Fatalf("Send should follow redirect and succeed: %v", err)
	}

	// UpdateHost is true: the final redirect target is cached
	if s.PrimaryHost() != target.address {
		t.Errorf("PrimaryHost: expected redirect target %s, got %s", target.address, s.PrimaryHost())
	}

	if err := <-redirectorDone; err != nil {
		t.Fatalf("redirector mock error: %v", err)
	}
	if err := <-targetDone; err != nil {
		t.Fatalf("target mock error: %v", err)
	}
}

func TestSendMaxRedirectsExceeded(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	// always redirect to itself: MaxRedirects+1 attempts are served
	redirectResp := fmt.Sprintf(`{"response":"failed","redirect":{"revision":1,"address":"%s"}}`, mock.address)
	done := serveResponses(mock, redirectResp, redirectResp, redirectResp)

	s := NewSender(mock.address)
	s.MaxRedirects = 2
	_, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected max redirects error, got nil")
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

// sendTestPacket sends a minimal packet through the full Send path.
func sendTestPacket(s *Sender) (Response, error) {
	return s.Send(NewPacket([]*Metric{NewMetric("host1", "key1", "1", false)}, false))
}

func TestSendFailover(t *testing.T) {
	dead := deadAddress(t)

	live := newMockZabbixServer(t)
	defer live.Close()
	done := serveResponses(live, successResp)

	s := NewSenderHosts([]string{dead, live.address})
	res, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("Send should fail over to the live host: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success response, got %q", res.Response)
	}
	if s.PrimaryHost() != live.address {
		t.Errorf("PrimaryHost: expected %s, got %s", live.address, s.PrimaryHost())
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendPrimaryHostCacheInvalidation(t *testing.T) {
	live := newMockZabbixServer(t)
	defer live.Close()
	done := serveResponses(live, successResp)

	s := NewSenderHosts([]string{live.address})
	s.SetPrimaryHost(deadAddress(t)) // stale cache pointing to a dead host

	_, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("Send should fall back to the host list: %v", err)
	}
	if s.PrimaryHost() != live.address {
		t.Errorf("PrimaryHost: expected refreshed cache %s, got %s", live.address, s.PrimaryHost())
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendAllHostsFailed(t *testing.T) {
	s := NewSenderHosts([]string{deadAddress(t), deadAddress(t)})
	s.ConnectTimeout = 500 * time.Millisecond

	_, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected error when all hosts are unreachable")
	}
	if s.PrimaryHost() != "" {
		t.Errorf("PrimaryHost should stay empty, got %s", s.PrimaryHost())
	}
}

func TestSendNoHostsConfigured(t *testing.T) {
	s := &Sender{}
	if _, err := s.Send(NewPacket(nil, false)); err == nil {
		t.Fatal("expected error for sender without hosts")
	}
}

// TestSendFailedResponseIsTerminal verifies that an application-level "failed"
// answer does not trigger failover to the next host: the answering host is
// authoritative, the other hosts of the HA list would answer the same.
func TestSendFailedResponseIsTerminal(t *testing.T) {
	answering := newMockZabbixServer(t)
	defer answering.Close()
	done := serveResponses(answering, `{"response":"failed","info":"some application error"}`)

	backup := newMockZabbixServer(t)
	defer backup.Close()
	var backupConns int32
	go func() {
		for {
			conn, err := backup.listener.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&backupConns, 1)
			conn.Close()
		}
	}()

	s := NewSenderHosts([]string{answering.address, backup.address})
	res, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected error for failed response")
	}

	var respErr *ResponseError
	if !errors.As(err, &respErr) {
		t.Fatalf("expected *ResponseError, got %T: %v", err, err)
	}
	if res.Response != "failed" {
		t.Errorf("expected the failed response to be returned, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt32(&backupConns); n != 0 {
		t.Errorf("backup host should not be contacted on application-level failure, got %d connections", n)
	}
}

func TestSendReadTimeout(t *testing.T) {
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

	s := NewSenderTimeout(mock.address, time.Second, 200*time.Millisecond, time.Second)

	start := time.Now()
	_, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected read timeout error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestSendResponseTooShort(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := mock.readZabbixRequest(conn); err != nil {
			return
		}
		conn.Write([]byte("ZBXD")) // truncated header
	}()

	s := NewSender(mock.address)
	if _, err := sendTestPacket(s); err == nil {
		t.Fatal("expected error for truncated response")
	}
}

func TestSendIncompleteResponseBody(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := mock.readZabbixRequest(conn); err != nil {
			return
		}
		// header announces 100 bytes but only 2 follow
		resp := append([]byte("ZBXD\x01"), encodeDataLength(100)...)
		resp = append(resp, []byte("{}")...)
		conn.Write(resp)
	}()

	s := NewSender(mock.address)
	if _, err := sendTestPacket(s); err == nil {
		t.Fatal("expected error for incomplete response body")
	}
}

func TestSendMetricsEmpty(t *testing.T) {
	s := NewSender("127.0.0.1:1") // must never be contacted

	resActive, errActive, resTrapper, errTrapper := s.SendMetrics(nil)
	if errActive != nil || errTrapper != nil {
		t.Fatalf("no packets should be sent for empty metrics: active=%v trapper=%v", errActive, errTrapper)
	}
	if _, err := resActive.GetInfo(); err == nil {
		t.Error("GetInfo on zero active response should fail")
	}
	if _, err := resTrapper.GetInfo(); err == nil {
		t.Error("GetInfo on zero trapper response should fail")
	}
}

// serveSuccessLoop answers every incoming connection with a success response
// until the listener is closed.
func serveSuccessLoop(mock *mockZabbixServer) {
	go func() {
		for {
			conn, err := mock.listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := mock.readZabbixRequest(c); err != nil {
					return
				}
				mock.writeZabbixResponse(c, successResp)
			}(conn)
		}
	}()
}

// TestConcurrentSends verifies the Sender is safe for concurrent use:
// parallel sends share the primary-host cache while it is concurrently
// read, written, and cleared. Run with -race.
func TestConcurrentSends(t *testing.T) {
	live := newMockZabbixServer(t)
	defer live.Close()
	serveSuccessLoop(live)

	dead := deadAddress(t)
	s := NewSenderHosts([]string{dead, live.address})
	s.ConnectTimeout = time.Second

	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if n%5 == 0 {
				s.SetPrimaryHost("") // concurrent cache invalidation
			}
			if _, err := sendTestPacket(s); err != nil {
				errs <- err
			}
			_ = s.PrimaryHost() // concurrent cache read
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent send failed: %v", err)
	}

	if got := s.PrimaryHost(); got != live.address {
		t.Errorf("PrimaryHost: expected %s, got %s", live.address, got)
	}
}
