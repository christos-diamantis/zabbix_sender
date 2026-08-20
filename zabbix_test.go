package zabbix_sender

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

type ZabbixRequestData struct {
	Host  string `json:"host"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Clock int64  `json:"clock"`
	NS    int    `json:"ns"`
}

type ZabbixRequest struct {
	Request      string              `json:"request"`
	Data         []ZabbixRequestData `json:"data"`
	Clock        int                 `json:"clock"`
	NS           int                 `json:"ns"`
	Host         string              `json:"host"`
	HostMetadata string              `json:"host_metadata"`
}

// mockZabbixServer is a helper struct to encapsulate mock server logic
type mockZabbixServer struct {
	listener net.Listener
	address  string
	t        *testing.T
}

// newMockZabbixServer creates a new mock server on a random available port
func newMockZabbixServer(t *testing.T) *mockZabbixServer {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	return &mockZabbixServer{
		listener: listener,
		address:  listener.Addr().String(),
		t:        t,
	}
}

func (m *mockZabbixServer) Close() {
	m.listener.Close()
}

// readZabbixRequest reads and parses a Zabbix protocol request,
// transparently decompressing compressed (flag 0x02) packets.
func (m *mockZabbixServer) readZabbixRequest(conn net.Conn) (*ZabbixRequest, error) {
	// Read protocol header: "ZBXD" + flags + uint32 datalen + uint32 reserved
	header := make([]byte, 13)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	if string(header[:4]) != "ZBXD" {
		return nil, fmt.Errorf("invalid protocol header: %q", header[:4])
	}
	flags := header[4]
	dataLength := binary.LittleEndian.Uint32(header[5:9])

	// Read data content
	content := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, content); err != nil {
		return nil, fmt.Errorf("failed to read content: %w", err)
	}

	if flags&0x02 != 0 {
		zr, err := zlib.NewReader(bytes.NewReader(content))
		if err != nil {
			return nil, fmt.Errorf("failed to open compressed content: %w", err)
		}
		defer zr.Close()
		if content, err = io.ReadAll(zr); err != nil {
			return nil, fmt.Errorf("failed to decompress content: %w", err)
		}
	}

	// Parse JSON request
	var request ZabbixRequest
	if err := json.Unmarshal(content, &request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}

	return &request, nil
}

// writeZabbixResponse writes a Zabbix protocol response
func (m *mockZabbixServer) writeZabbixResponse(conn net.Conn, jsonData string) error {
	response := fmt.Sprintf("ZBXD\x01%s%s",
		string(encodeDataLength(len(jsonData))),
		jsonData)

	if _, err := conn.Write([]byte(response)); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	return nil
}

// encodeDataLength encodes length as 8-byte little endian
func encodeDataLength(length int) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(length))
	return buf
}

func TestSendActiveMetric(t *testing.T) {
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

		if request.Request != "agent data" {
			done <- fmt.Errorf("expected 'agent data', got '%s'", request.Request)
			return
		}

		jsonResp := `{"response":"success","info":"processed: 1; failed: 0; total: 1; seconds spent: 0.000030"}`
		if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
			done <- err
			return
		}

		done <- nil
	}()

	m := NewMetric("zabbixAgent1", "ping", "13", true)
	s := NewSender(mock.address)
	resActive, errActive, resTrapper, errTrapper := s.SendMetrics([]*Metric{m})

	if errActive != nil {
		t.Fatalf("error sending active metric: %v", errActive)
	}
	if errTrapper != nil {
		t.Fatalf("trapper error should be nil: %v", errTrapper)
	}

	raInfo, err := resActive.GetInfo()
	if err != nil {
		t.Fatalf("error getting active response info: %v", err)
	}

	if raInfo.Failed != 0 {
		t.Errorf("Failed: expected 0, got %d", raInfo.Failed)
	}
	if raInfo.Processed != 1 {
		t.Errorf("Processed: expected 1, got %d", raInfo.Processed)
	}
	if raInfo.Total != 1 {
		t.Errorf("Total: expected 1, got %d", raInfo.Total)
	}

	if _, err := resTrapper.GetInfo(); err == nil {
		t.Error("Expected error when getting trapper info (no trapper metrics sent)")
	}

	if err := <-done; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

func TestSendTrapperMetric(t *testing.T) {
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
			done <- fmt.Errorf("expected 'sender data', got '%s'", request.Request)
			return
		}

		jsonResp := `{"response":"success","info":"processed: 1; failed: 0; total: 1; seconds spent: 0.000030"}`
		if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
			done <- err
			return
		}

		done <- nil
	}()

	m := NewMetric("zabbixAgent1", "ping", "13", false)
	s := NewSender(mock.address)
	resActive, errActive, resTrapper, errTrapper := s.SendMetrics([]*Metric{m})

	if errTrapper != nil {
		t.Fatalf("error sending trapper metric: %v", errTrapper)
	}
	if errActive != nil {
		t.Fatalf("active error should be nil: %v", errActive)
	}

	rtInfo, err := resTrapper.GetInfo()
	if err != nil {
		t.Fatalf("error getting trapper response info: %v", err)
	}

	if rtInfo.Failed != 0 {
		t.Errorf("Failed: expected 0, got %d", rtInfo.Failed)
	}
	if rtInfo.Processed != 1 {
		t.Errorf("Processed: expected 1, got %d", rtInfo.Processed)
	}
	if rtInfo.Total != 1 {
		t.Errorf("Total: expected 1, got %d", rtInfo.Total)
	}

	if _, err := resActive.GetInfo(); err == nil {
		t.Error("Expected error when getting active info (no active metrics sent)")
	}

	if err := <-done; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

func TestSendActiveAndTrapperMetric(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	done := make(chan error, 1)

	go func() {
		defer func() { done <- nil }()

		for i := 0; i < 2; i++ {
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

			var jsonResp string
			switch request.Request {
			case "sender data":
				jsonResp = `{"response":"success","info":"processed: 1; failed: 0; total: 1; seconds spent: 0.000030"}`
			case "agent data":
				jsonResp = `{"response":"success","info":"processed: 1; failed: 0; total: 1; seconds spent: 0.111111"}`
			default:
				conn.Close()
				done <- fmt.Errorf("unexpected request type: %s", request.Request)
				return
			}

			if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
				conn.Close()
				done <- err
				return
			}

			conn.Close()
		}
	}()

	m1 := NewMetric("zabbixAgent1", "ping", "13", true)
	m2 := NewMetric("zabbixTrapper1", "pong", "13", false)

	s := NewSender(mock.address)
	resActive, errActive, resTrapper, errTrapper := s.SendMetrics([]*Metric{m1, m2})

	if errActive != nil {
		t.Fatalf("error sending active metric: %v", errActive)
	}
	if errTrapper != nil {
		t.Fatalf("error sending trapper metric: %v", errTrapper)
	}

	raInfo, err := resActive.GetInfo()
	if err != nil {
		t.Fatalf("error getting active response info: %v", err)
	}

	if raInfo.Failed != 0 {
		t.Errorf("Active Failed: expected 0, got %d", raInfo.Failed)
	}
	if raInfo.Processed != 1 {
		t.Errorf("Active Processed: expected 1, got %d", raInfo.Processed)
	}
	if raInfo.Total != 1 {
		t.Errorf("Active Total: expected 1, got %d", raInfo.Total)
	}

	rtInfo, err := resTrapper.GetInfo()
	if err != nil {
		t.Fatalf("error getting trapper response info: %v", err)
	}

	if rtInfo.Failed != 0 {
		t.Errorf("Trapper Failed: expected 0, got %d", rtInfo.Failed)
	}
	if rtInfo.Processed != 1 {
		t.Errorf("Trapper Processed: expected 1, got %d", rtInfo.Processed)
	}
	if rtInfo.Total != 1 {
		t.Errorf("Trapper Total: expected 1, got %d", rtInfo.Total)
	}

	if err := <-done; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

// TestRegisterHostSuccess tests successful registration when host already exists
func TestRegisterHostSuccess(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	serverDone := make(chan error, 1)

	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		request, err := mock.readZabbixRequest(conn)
		if err != nil {
			serverDone <- err
			return
		}

		if request.Request != "active checks" {
			serverDone <- fmt.Errorf("expected 'active checks', got '%s'", request.Request)
			return
		}

		// Host exists - return success with active checks
		jsonResp := `{"response":"success","data":[{"key":"net.if.in[eth0]","delay":60,"lastlogsize":0,"mtime":0}]}`
		if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
			serverDone <- err
			return
		}

		serverDone <- nil
	}()

	s := NewSender(mock.address)
	err := s.RegisterHost("prueba", "prueba")
	if err != nil {
		t.Fatalf("RegisterHost should succeed when host exists: %v", err)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

// TestRegisterHostNotFound tests error when autoregistration keeps failing:
// the server answers "failed" on the initial call and on the confirmation retry.
func TestRegisterHostNotFound(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	serverDone := make(chan error, 1)

	go func() {
		for i := 0; i < 2; i++ {
			conn, err := mock.listener.Accept()
			if err != nil {
				serverDone <- err
				return
			}

			request, err := mock.readZabbixRequest(conn)
			if err != nil {
				conn.Close()
				serverDone <- err
				return
			}

			if request.Request != "active checks" {
				conn.Close()
				serverDone <- fmt.Errorf("expected 'active checks', got '%s'", request.Request)
				return
			}

			// Host not found - return failure on both calls
			jsonResp := `{"response":"failed","info":"host [prueba] not found"}`
			if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
				conn.Close()
				serverDone <- err
				return
			}
			conn.Close()
		}
		serverDone <- nil
	}()

	s := NewSender(mock.address)
	err := s.RegisterHost("prueba", "prueba")
	if err == nil {
		t.Fatal("RegisterHost should fail when host not found")
	}

	t.Logf("Got expected error: %v", err)

	if err := <-serverDone; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

// TestRegisterHostAutoregistrationFlow reproduces the real Zabbix
// autoregistration flow: the first "active checks" request answers "failed"
// (host not found, which triggers the server-side autoregistration) and the
// confirmation retry answers "success".
func TestRegisterHostAutoregistrationFlow(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()

	serverDone := make(chan error, 1)

	go func() {
		for i := 0; i < 2; i++ {
			conn, err := mock.listener.Accept()
			if err != nil {
				serverDone <- err
				return
			}

			if _, err := mock.readZabbixRequest(conn); err != nil {
				conn.Close()
				serverDone <- err
				return
			}

			var jsonResp string
			if i == 0 {
				jsonResp = `{"response":"failed","info":"host [newhost] not found"}`
			} else {
				jsonResp = `{"response":"success","data":[]}`
			}
			if err := mock.writeZabbixResponse(conn, jsonResp); err != nil {
				conn.Close()
				serverDone <- err
				return
			}
			conn.Close()
		}
		serverDone <- nil
	}()

	s := NewSender(mock.address)
	if err := s.RegisterHost("newhost", "meta"); err != nil {
		t.Fatalf("RegisterHost should succeed on failed-then-success flow, got: %v", err)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

func TestInvalidResponseHeader(t *testing.T) {
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

		if request.Request != "agent data" {
			done <- fmt.Errorf("expected 'agent data', got '%s'", request.Request)
			return
		}

		// Send invalid header (BXD instead of ZBXD)
		invalidResp := "BXD\x01\x00\x00\x00\x00\x00\x00\x00\x00{\"response\":\"success\",\"info\":\"processed: 1; failed: 0; total: 1; seconds spent: 0.000030\"}"
		if _, err := conn.Write([]byte(invalidResp)); err != nil {
			done <- err
			return
		}

		done <- nil
	}()

	m := NewMetric("zabbixAgent1", "ping", "13", true)
	s := NewSender(mock.address)
	_, errActive, _, _ := s.SendMetrics([]*Metric{m})

	if errActive == nil {
		t.Fatal("expected error due to invalid Zabbix protocol header, got nil")
	}

	if err := <-done; err != nil {
		t.Fatalf("Mock server error: %v", err)
	}
}

func TestNewMetricsWithTime(t *testing.T) {
	now := time.Now()
	m := NewMetric("zabbixAgent1", "ping", "13", false, now)

	if m.Clock != now.Unix() {
		t.Errorf("Clock: expected %d, got %d", now.Unix(), m.Clock)
	}
	if m.NS != now.Nanosecond() {
		t.Errorf("NS: expected %d, got %d", now.Nanosecond(), m.NS)
	}
}

func TestNewPacketWithTime(t *testing.T) {
	now := time.Now()

	m1 := NewMetric("zabbixAgent1", "ping", "13", false, time.Now())
	m2 := NewMetric("zabbixAgent2", "pong", "42", true, time.Now())

	p := NewPacket([]*Metric{m1, m2}, false, now)

	if p.Clock != now.Unix() {
		t.Errorf("Clock: expected %d, got %d", now.Unix(), p.Clock)
	}
	if p.NS != now.Nanosecond() {
		t.Errorf("NS: expected %d, got %d", now.Nanosecond(), p.NS)
	}
}

func TestNormalizeHost_DefaultPort(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{} // can be string or []string
		expected []string
	}{
		{
			name:     "single host without port",
			input:    "zabbix-proxy",
			expected: []string{"zabbix-proxy:10051"},
		},
		{
			name:     "single host with port",
			input:    "zabbix-proxy:10051",
			expected: []string{"zabbix-proxy:10051"},
		},
		{
			name:     "multiple hosts mixed",
			input:    []string{"zabbix-proxy1:10051", "zabbix-proxy2"},
			expected: []string{"zabbix-proxy1:10051", "zabbix-proxy2:10051"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s *Sender

			switch v := tt.input.(type) {
			case string:
				s = NewSender(v)
			case []string:
				s = NewSenderHosts(v)
			}

			if len(s.Hosts) != len(tt.expected) {
				t.Fatalf("expected %d hosts, got %d", len(tt.expected), len(s.Hosts))
			}

			for i, expected := range tt.expected {
				if s.Hosts[i] != expected {
					t.Errorf("host[%d]: expected %s, got %s", i, expected, s.Hosts[i])
				}
			}
		})
	}
}

// Integration tests against a real Zabbix server live in integration_test.go
// (build tag "integration", configured via ZABBIX_ADDR).
