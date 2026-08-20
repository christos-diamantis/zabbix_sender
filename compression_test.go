package zabbix_sender

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
)

// writeCompressedZabbixResponse writes a compressed (flag 0x02) response.
func writeCompressedZabbixResponse(conn net.Conn, jsonData string) error {
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	if _, err := zw.Write([]byte(jsonData)); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}

	resp := append([]byte("ZBXD"), 0x03) // protocol | compression
	resp = binary.LittleEndian.AppendUint32(resp, uint32(compressed.Len()))
	resp = binary.LittleEndian.AppendUint32(resp, uint32(len(jsonData)))
	resp = append(resp, compressed.Bytes()...)
	_, err := conn.Write(resp)
	return err
}

// TestSendCompressed verifies the raw wire format of a compressed request:
// flag 0x03, datalen = compressed size, reserved = uncompressed size, and a
// zlib body that decompresses to the packet JSON.
func TestSendCompressed(t *testing.T) {
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

		header := make([]byte, 13)
		if _, err := io.ReadFull(conn, header); err != nil {
			done <- err
			return
		}
		if string(header[:4]) != "ZBXD" || header[4] != 0x03 {
			done <- errAssert("header/flags", "ZBXD 0x03", string(header[:4])+" "+string(rune(header[4])))
			return
		}

		dataLen := binary.LittleEndian.Uint32(header[5:9])
		reserved := binary.LittleEndian.Uint32(header[9:13])

		body := make([]byte, dataLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			done <- err
			return
		}

		zr, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			done <- err
			return
		}
		plain, err := io.ReadAll(zr)
		zr.Close()
		if err != nil {
			done <- err
			return
		}
		if uint32(len(plain)) != reserved {
			done <- errAssert("reserved field", reserved, len(plain))
			return
		}

		var request ZabbixRequest
		if err := json.Unmarshal(plain, &request); err != nil {
			done <- err
			return
		}
		if request.Request != "sender data" {
			done <- errAssert("request", "sender data", request.Request)
			return
		}

		done <- writeCompressedZabbixResponse(conn, successResp)
	}()

	s := NewSender(mock.address)
	s.Compression = true

	res, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("compressed send: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

// TestCompressedResponseWithoutCompressedRequest verifies compressed
// responses are accepted even when the request was sent uncompressed.
func TestCompressedResponseWithoutCompressedRequest(t *testing.T) {
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
		if _, err := mock.readZabbixRequest(conn); err != nil {
			done <- err
			return
		}
		done <- writeCompressedZabbixResponse(conn, successResp)
	}()

	s := NewSender(mock.address) // Compression off

	res, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestLargePacketResponseRejected(t *testing.T) {
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
		resp := append([]byte("ZBXD"), 0x05) // protocol | large packet
		resp = binary.LittleEndian.AppendUint32(resp, 2)
		resp = binary.LittleEndian.AppendUint32(resp, 0)
		resp = append(resp, []byte("{}")...)
		conn.Write(resp)
	}()

	s := NewSender(mock.address)
	_, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected error for large-packet flag")
	}
	if !strings.Contains(err.Error(), "large-packet") {
		t.Errorf("expected large-packet error, got: %v", err)
	}
}

func TestCompressedResponseGarbageRejected(t *testing.T) {
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
		resp := append([]byte("ZBXD"), 0x03)
		resp = binary.LittleEndian.AppendUint32(resp, 4)
		resp = binary.LittleEndian.AppendUint32(resp, 100)
		resp = append(resp, []byte("junk")...) // not zlib
		conn.Write(resp)
	}()

	s := NewSender(mock.address)
	if _, err := sendTestPacket(s); err == nil {
		t.Fatal("expected error for invalid zlib body")
	}
}

// errAssert builds a comparable assertion error for the mock goroutines.
func errAssert(what string, expected, got interface{}) error {
	return &assertError{what: what, expected: expected, got: got}
}

type assertError struct {
	what          string
	expected, got interface{}
}

func (e *assertError) Error() string {
	return e.what + ": expected " + toString(e.expected) + ", got " + toString(e.got)
}

func toString(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
