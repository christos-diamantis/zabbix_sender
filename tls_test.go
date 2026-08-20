package zabbix_sender

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// testCertificate generates a self-signed certificate for 127.0.0.1 and
// returns it as a server tls.Certificate plus its PEM-encoded forms.
func testCertificate(t *testing.T) (tls.Certificate, []byte, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "zabbix-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("building key pair: %v", err)
	}
	return cert, certPEM, keyPEM
}

// newTLSMockZabbixServer wraps the mock server's listener in TLS.
func newTLSMockZabbixServer(t *testing.T, cert tls.Certificate) *mockZabbixServer {
	t.Helper()
	mock := newMockZabbixServer(t)
	mock.listener = tls.NewListener(mock.listener, &tls.Config{Certificates: []tls.Certificate{cert}})
	return mock
}

func clientPool(t *testing.T, certPEM []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to add test certificate to pool")
	}
	return pool
}

func TestSendOverTLS(t *testing.T) {
	cert, certPEM, _ := testCertificate(t)
	mock := newTLSMockZabbixServer(t, cert)
	defer mock.Close()
	done := serveResponses(mock, successResp)

	s := NewSender(mock.address)
	s.TLSConfig = &tls.Config{RootCAs: clientPool(t, certPEM)}

	res, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("Send over TLS: %v", err)
	}
	if res.Response != "success" {
		t.Errorf("expected success, got %q", res.Response)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendTLSToPlainServerFails(t *testing.T) {
	_, certPEM, _ := testCertificate(t)
	mock := newMockZabbixServer(t) // plain TCP server
	defer mock.Close()
	go func() {
		conn, err := mock.listener.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	s := NewSender(mock.address)
	s.TLSConfig = &tls.Config{RootCAs: clientPool(t, certPEM)}
	s.ConnectTimeout = time.Second
	s.ReadTimeout = time.Second

	if _, err := sendTestPacket(s); err == nil {
		t.Fatal("TLS handshake against a plain server should fail")
	}
}

func TestSendWithDialFunc(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveResponses(mock, successResp)

	var calls int32
	s := NewSender(mock.address)
	s.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		atomic.AddInt32(&calls, 1)
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}

	if _, err := sendTestPacket(s); err != nil {
		t.Fatalf("Send with DialFunc: %v", err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("DialFunc should be called once, got %d", n)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendWithSourceIP(t *testing.T) {
	mock := newMockZabbixServer(t)
	defer mock.Close()
	done := serveResponses(mock, successResp)

	s := NewSender(mock.address)
	s.SourceIP = "127.0.0.1"

	if _, err := sendTestPacket(s); err != nil {
		t.Fatalf("Send with SourceIP: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}

func TestSendWithInvalidSourceIP(t *testing.T) {
	s := NewSender("127.0.0.1:1")
	s.SourceIP = "not-an-ip"

	_, err := sendTestPacket(s)
	if err == nil {
		t.Fatal("expected error for invalid SourceIP")
	}
}

func TestTLSConfigFromFiles(t *testing.T) {
	_, certPEM, keyPEM := testCertificate(t)
	dir := t.TempDir()

	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	for file, data := range map[string][]byte{caFile: certPEM, certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}

	cfg, err := TLSConfigFromFiles(caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("TLSConfigFromFiles: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs should be set")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}

	// CA only, no client cert
	cfg, err = TLSConfigFromFiles(caFile, "", "")
	if err != nil {
		t.Fatalf("TLSConfigFromFiles CA-only: %v", err)
	}
	if len(cfg.Certificates) != 0 {
		t.Error("no client certificate expected")
	}

	// errors
	if _, err := TLSConfigFromFiles(filepath.Join(dir, "missing.pem"), "", ""); err == nil {
		t.Error("expected error for missing CA file")
	}
	if _, err := TLSConfigFromFiles("", certFile, filepath.Join(dir, "missing.pem")); err == nil {
		t.Error("expected error for missing key file")
	}
	badCA := filepath.Join(dir, "bad.pem")
	os.WriteFile(badCA, []byte("not a pem"), 0o600)
	if _, err := TLSConfigFromFiles(badCA, "", ""); err == nil {
		t.Error("expected error for non-PEM CA file")
	}
}

// TestSendOverTLSEndToEnd sends a full metric batch over TLS with a client
// certificate and verifies the parsed response.
func TestSendOverTLSWithClientCert(t *testing.T) {
	cert, certPEM, keyPEM := testCertificate(t)

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	for file, data := range map[string][]byte{caFile: certPEM, certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}

	mock := newMockZabbixServer(t)
	pool := clientPool(t, certPEM)
	mock.listener = tls.NewListener(mock.listener, &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})
	defer mock.Close()
	done := serveResponses(mock, successResp)

	cfg, err := TLSConfigFromFiles(caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("TLSConfigFromFiles: %v", err)
	}

	s := NewSender(mock.address)
	s.TLSConfig = cfg

	res, err := sendTestPacket(s)
	if err != nil {
		t.Fatalf("Send over mutual TLS: %v", err)
	}
	info, err := res.GetInfo()
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.Processed != 1 {
		t.Errorf("expected processed=1, got %d", info.Processed)
	}

	if err := <-done; err != nil {
		t.Fatalf("mock server error: %v", err)
	}
}
