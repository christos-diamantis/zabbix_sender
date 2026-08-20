package zabbix_sender

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfigFromFiles builds a certificate-based tls.Config the way
// zabbix_sender does for TLSConnect=cert:
//
//   - caFile (TLSCAFile) is the CA bundle used to verify the server;
//     empty means the system CA pool.
//   - certFile/keyFile (TLSCertFile/TLSKeyFile) are the client certificate
//     and key; both empty means no client certificate is presented.
//
// The result can be assigned to Sender.TLSConfig, possibly after further
// adjustment (e.g. ServerName when connecting by IP).
func TLSConfigFromFiles(caFile, certFile, keyFile string) (*tls.Config, error) {
	cfg := &tls.Config{}

	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in CA file %s", caFile)
		}
		cfg.RootCAs = pool
	}

	if certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
