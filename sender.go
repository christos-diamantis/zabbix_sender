package zabbix_sender

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// zabbixHeader is the Zabbix protocol header: "ZBXD" + protocol flags (0x01).
// https://www.zabbix.com/documentation/current/en/manual/appendix/protocols/header_datalen
var zabbixHeader = []byte("ZBXD\x01")

// headerLen is the full header size: "ZBXD" + flags byte + 8-byte data length.
const headerLen = 13

// maxPacketSize caps outgoing packet bodies. It bounds the length arithmetic
// of the send buffer (flagged by CodeQL as a potential allocation-size
// overflow) and is far beyond any real Zabbix payload.
const maxPacketSize = 1 << 30 // 1 GiB

// DefaultMaxResponseSize caps how large a response body the sender accepts
// when Sender.MaxResponseSize is 0. Real Zabbix answers are tiny; the cap
// only guards against a broken or malicious peer.
const DefaultMaxResponseSize = 16 << 20 // 16 MiB

// Sender sends packets to a Zabbix server/proxy.
//
// A Sender is safe for concurrent use by multiple goroutines: sends run in
// parallel, and the working-host cache is internally synchronized. The
// configuration fields (Hosts, MaxRedirects, timeouts, ...) are read without
// locking and must be set before the Sender is shared between goroutines.
type Sender struct {
	Hosts          []string // ordered list of proxies/servers; first successful cached as primary host
	MaxRedirects   int      // max redirect attempts before error; default is 3
	UpdateHost     bool     // if true, cache the final redirect target instead of the starting host
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	// MaxResponseSize caps the response body length the sender accepts.
	// 0 means DefaultMaxResponseSize.
	MaxResponseSize uint64

	mu          sync.RWMutex
	primaryHost string // cached working host (empty = try Hosts in order)
}

// PrimaryHost returns the cached working host (empty = none cached yet).
func (s *Sender) PrimaryHost() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.primaryHost
}

// SetPrimaryHost pre-sets (or, with "", clears) the cached working host.
func (s *Sender) SetPrimaryHost(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.primaryHost = host
}

// setPrimaryHostIf updates the cache only if it still holds expected, so a
// concurrent send that already refreshed the cache is not overwritten with
// stale information.
func (s *Sender) setPrimaryHostIf(expected, host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.primaryHost == expected {
		s.primaryHost = host
	}
}

// ResponseError is returned when a server/proxy was reached and answered with
// a well-formed non-success response. It lets callers distinguish
// application-level failures from transport errors; Send does not fail over
// to another host when it occurs.
type ResponseError struct {
	Host string
	Res  Response
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("host %s answered %q: %s", e.Host, e.Res.Response, e.Res.Info)
}

func isResponseError(err error) bool {
	var respErr *ResponseError
	return errors.As(err, &respErr)
}

// SendMetrics sends mixed active+trapper metrics.
// Automatically separates into "agent data" and "sender data" packets.
// Returns 4 values: (activeRes, activeErr, trapperRes, trapperErr)
func (s *Sender) SendMetrics(metrics []*Metric) (resActive Response, errActive error, resTrapper Response, errTrapper error) {
	return s.SendMetricsContext(context.Background(), metrics)
}

// SendMetricsContext is SendMetrics honoring the context's deadline and
// cancellation on top of the Sender's own timeouts.
func (s *Sender) SendMetricsContext(ctx context.Context, metrics []*Metric) (resActive Response, errActive error, resTrapper Response, errTrapper error) {
	var trapperMetrics []*Metric
	var activeMetrics []*Metric

	for i := range metrics {
		if metrics[i].Active {
			activeMetrics = append(activeMetrics, metrics[i])
		} else {
			trapperMetrics = append(trapperMetrics, metrics[i])
		}
	}

	if len(trapperMetrics) > 0 {
		packetTrapper := NewPacket(trapperMetrics, false)
		resTrapper, errTrapper = s.SendContext(ctx, packetTrapper)
	}

	if len(activeMetrics) > 0 {
		packetActive := NewPacket(activeMetrics, true)
		resActive, errActive = s.SendContext(ctx, packetActive)
	}

	return resActive, errActive, resTrapper, errTrapper
}

// Send sends single packet with redirect/HA handling.
// Caches working PrimaryHost for future calls. Fails over to the next host
// only on transport errors; a host that answers (even with "failed") is
// considered reachable and its answer final (see ResponseError).
func (s *Sender) Send(packet *Packet) (Response, error) {
	return s.SendContext(context.Background(), packet)
}

// SendContext is Send honoring the context's deadline and cancellation on
// top of the Sender's own timeouts.
func (s *Sender) SendContext(ctx context.Context, packet *Packet) (Response, error) {
	primary := s.PrimaryHost()
	if primary == "" && len(s.Hosts) == 0 {
		return Response{}, errors.New("no hosts configured")
	}

	var lastErr error

	if primary != "" {
		res, final, err := s.sendWithRedirects(ctx, packet, primary)
		if err == nil || isResponseError(err) {
			if s.UpdateHost && final != "" {
				s.setPrimaryHostIf(primary, final)
			}
			return res, err
		}
		lastErr = err
		s.setPrimaryHostIf(primary, "") // clear cache, fall back to the host list
	}

	for _, host := range s.Hosts {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		res, final, err := s.sendWithRedirects(ctx, packet, host)
		if err == nil || isResponseError(err) {
			cached := host
			if s.UpdateHost && final != "" {
				cached = final
			}
			s.SetPrimaryHost(cached)
			return res, err
		}
		lastErr = err
	}

	return Response{}, fmt.Errorf("all %d hosts failed: %w", len(s.Hosts), lastErr)
}

// sendWithRedirects follows proxy group redirects up to MaxRedirects and
// returns the final host that answered.
func (s *Sender) sendWithRedirects(ctx context.Context, packet *Packet, startHost string) (res Response, finalHost string, err error) {
	currentHost := startHost

	for redirectCount := 0; redirectCount <= s.MaxRedirects; redirectCount++ {
		if err := ctx.Err(); err != nil {
			return res, currentHost, err
		}
		res, err = s.sendOnce(ctx, packet, currentHost)
		if err != nil {
			return res, currentHost, fmt.Errorf("sendOnce to %s failed: %w", currentHost, err)
		}

		// success - done
		if res.Response == "success" {
			return res, currentHost, nil
		}

		// non-success without redirect: the server was reached and gave a final answer
		if res.Redirect == nil || res.Redirect.Address == "" {
			return res, currentHost, &ResponseError{Host: currentHost, Res: res}
		}

		// got redirect - update target and retry
		newHost, perr := parseHostPort(res.Redirect.Address)
		if perr != nil {
			return res, currentHost, perr
		}
		currentHost = newHost
	}

	return res, currentHost, fmt.Errorf("max redirects exceeded from %s", startHost)
}

func (s *Sender) sendOnce(ctx context.Context, packet *Packet, host string) (res Response, err error) {
	// Timeout to resolve and connect to the server; the context can cancel
	// the dial or shorten the deadline further.
	dialer := net.Dialer{Timeout: s.ConnectTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return res, fmt.Errorf("connecting to %s (timeout=%v): %w", host, s.ConnectTimeout, err)
	}
	defer conn.Close()

	// Abort in-flight reads/writes as soon as the context is canceled.
	watcherDone := make(chan struct{})
	defer close(watcherDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.SetDeadline(time.Now())
		case <-watcherDone:
		}
	}()

	body, err := json.Marshal(packet)
	if err != nil {
		return res, fmt.Errorf("marshaling packet: %w", err)
	}
	if len(body) > maxPacketSize {
		return res, fmt.Errorf("packet too large: %d bytes (limit %d)", len(body), maxPacketSize)
	}

	buffer := make([]byte, 0, headerLen+len(body))
	buffer = append(buffer, zabbixHeader...)
	buffer = binary.LittleEndian.AppendUint64(buffer, uint64(len(body)))
	buffer = append(buffer, body...)

	// Write timeout
	conn.SetWriteDeadline(earliestDeadline(ctx, s.WriteTimeout))

	// Send packet to zabbix
	if _, err = conn.Write(buffer); err != nil {
		return res, fmt.Errorf("sending the data to %s (timeout=%v): %w", host, s.WriteTimeout, err)
	}

	// Read timeout
	conn.SetReadDeadline(earliestDeadline(ctx, s.ReadTimeout))

	// Read exactly one length-prefixed response instead of reading until
	// the server closes the connection.
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return res, fmt.Errorf("reading the response header from %s (timeout=%v): %w", host, s.ReadTimeout, err)
	}

	if !bytes.Equal(header[:5], zabbixHeader) {
		return res, fmt.Errorf("got no valid header [%+v], expected [%+v]", header[:5], zabbixHeader)
	}

	maxSize := s.MaxResponseSize
	if maxSize == 0 {
		maxSize = DefaultMaxResponseSize
	}
	dataLen := binary.LittleEndian.Uint64(header[5:])
	if dataLen > maxSize {
		return res, fmt.Errorf("response from %s too large: header announces %d bytes, limit is %d", host, dataLen, maxSize)
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(conn, data); err != nil {
		return res, fmt.Errorf("incomplete response from %s: header announces %d bytes: %w", host, dataLen, err)
	}

	if err := json.Unmarshal(data, &res); err != nil {
		return res, fmt.Errorf("zabbix response from %s is not valid: %w", host, err)
	}

	return res, nil
}

// earliestDeadline returns now+timeout, or the context's deadline if that
// comes first.
func earliestDeadline(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

// RegisterHost sends host autoregistration request ("active checks").
// A first "failed" answer is expected for unknown hosts (it is what triggers
// the server-side autoregistration), so it retries once to confirm.
func (s *Sender) RegisterHost(host, hostmetadata string) error {
	return s.RegisterHostContext(context.Background(), host, hostmetadata)
}

// RegisterHostContext is RegisterHost honoring the context's deadline and
// cancellation on top of the Sender's own timeouts.
func (s *Sender) RegisterHostContext(ctx context.Context, host, hostmetadata string) error {
	newPacket := func() *Packet {
		return &Packet{Request: "active checks", Host: host, HostMetadata: hostmetadata}
	}

	_, err := s.SendContext(ctx, newPacket())
	if err == nil {
		return nil // host already registered
	}
	if !isResponseError(err) {
		return fmt.Errorf("sending packet: %w", err)
	}

	// The first call answered "failed", which triggers the autoregistration.
	// Call again to verify the host is now registered.
	_, err = s.SendContext(ctx, newPacket())
	if err == nil {
		return nil
	}
	if isResponseError(err) {
		return fmt.Errorf("autoregistration failed, verify hostmetadata: %w", err)
	}
	return fmt.Errorf("sending packet: %w", err)
}
