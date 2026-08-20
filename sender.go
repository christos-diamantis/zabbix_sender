package zabbix_sender

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// zabbixHeader is the Zabbix protocol header: "ZBXD" + protocol flags (0x01).
// https://www.zabbix.com/documentation/current/en/manual/appendix/protocols/header_datalen
var zabbixHeader = []byte("ZBXD\x01")

// headerLen is the full header size: "ZBXD" + flags byte + 8-byte data length.
const headerLen = 13

// Sender struct.
type Sender struct {
	Hosts          []string // ordered list of proxies/servers; first successful cached in PrimaryHost
	PrimaryHost    string   // cached working host (empty = try Hosts in order)
	MaxRedirects   int      // max redirect attempts before error; default is 3
	UpdateHost     bool     // if true, cache the final redirect target instead of the starting host
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
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
		resTrapper, errTrapper = s.Send(packetTrapper)
	}

	if len(activeMetrics) > 0 {
		packetActive := NewPacket(activeMetrics, true)
		resActive, errActive = s.Send(packetActive)
	}

	return resActive, errActive, resTrapper, errTrapper
}

// Send sends single packet with redirect/HA handling.
// Caches working PrimaryHost for future calls. Fails over to the next host
// only on transport errors; a host that answers (even with "failed") is
// considered reachable and its answer final (see ResponseError).
func (s *Sender) Send(packet *Packet) (Response, error) {
	if s.PrimaryHost == "" && len(s.Hosts) == 0 {
		return Response{}, errors.New("no hosts configured")
	}

	var lastErr error

	if s.PrimaryHost != "" {
		res, final, err := s.sendWithRedirects(packet, s.PrimaryHost)
		if err == nil || isResponseError(err) {
			if s.UpdateHost && final != "" {
				s.PrimaryHost = final
			}
			return res, err
		}
		lastErr = err
		s.PrimaryHost = "" // clear cache, fall back to the host list
	}

	for _, host := range s.Hosts {
		res, final, err := s.sendWithRedirects(packet, host)
		if err == nil || isResponseError(err) {
			s.PrimaryHost = host
			if s.UpdateHost && final != "" {
				s.PrimaryHost = final
			}
			return res, err
		}
		lastErr = err
	}

	return Response{}, fmt.Errorf("all %d hosts failed: %w", len(s.Hosts), lastErr)
}

// sendWithRedirects follows proxy group redirects up to MaxRedirects and
// returns the final host that answered.
func (s *Sender) sendWithRedirects(packet *Packet, startHost string) (res Response, finalHost string, err error) {
	currentHost := startHost

	for redirectCount := 0; redirectCount <= s.MaxRedirects; redirectCount++ {
		res, err = s.sendOnce(packet, currentHost)
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

func (s *Sender) sendOnce(packet *Packet, host string) (res Response, err error) {
	// Timeout to resolve and connect to the server
	conn, err := net.DialTimeout("tcp", host, s.ConnectTimeout)
	if err != nil {
		return res, fmt.Errorf("connecting to %s (timeout=%v): %w", host, s.ConnectTimeout, err)
	}
	defer conn.Close()

	body, err := json.Marshal(packet)
	if err != nil {
		return res, fmt.Errorf("marshaling packet: %w", err)
	}

	buffer := make([]byte, 0, headerLen+len(body))
	buffer = append(buffer, zabbixHeader...)
	buffer = binary.LittleEndian.AppendUint64(buffer, uint64(len(body)))
	buffer = append(buffer, body...)

	// Write timeout
	conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))

	// Send packet to zabbix
	if _, err = conn.Write(buffer); err != nil {
		return res, fmt.Errorf("sending the data to %s (timeout=%v): %w", host, s.WriteTimeout, err)
	}

	// Read timeout; the server closes the connection after answering
	conn.SetReadDeadline(time.Now().Add(s.ReadTimeout))
	response, err := io.ReadAll(conn)
	if err != nil {
		return res, fmt.Errorf("reading the response from %s (timeout=%v): %w", host, s.ReadTimeout, err)
	}

	if len(response) < headerLen {
		return res, fmt.Errorf("response too short from %s: %d bytes", host, len(response))
	}

	if !bytes.Equal(response[:5], zabbixHeader) {
		return res, fmt.Errorf("got no valid header [%+v], expected [%+v]", response[:5], zabbixHeader)
	}

	dataLen := binary.LittleEndian.Uint64(response[5:headerLen])
	data := response[headerLen:]
	if uint64(len(data)) < dataLen {
		return res, fmt.Errorf("incomplete response from %s: header announces %d bytes, got %d", host, dataLen, len(data))
	}
	data = data[:dataLen]

	if err := json.Unmarshal(data, &res); err != nil {
		return res, fmt.Errorf("zabbix response from %s is not valid: %w", host, err)
	}

	return res, nil
}

// RegisterHost sends host autoregistration request ("active checks").
// A first "failed" answer is expected for unknown hosts (it is what triggers
// the server-side autoregistration), so it retries once to confirm.
func (s *Sender) RegisterHost(host, hostmetadata string) error {
	newPacket := func() *Packet {
		return &Packet{Request: "active checks", Host: host, HostMetadata: hostmetadata}
	}

	_, err := s.Send(newPacket())
	if err == nil {
		return nil // host already registered
	}
	if !isResponseError(err) {
		return fmt.Errorf("sending packet: %w", err)
	}

	// The first call answered "failed", which triggers the autoregistration.
	// Call again to verify the host is now registered.
	_, err = s.Send(newPacket())
	if err == nil {
		return nil
	}
	if isResponseError(err) {
		return fmt.Errorf("autoregistration failed, verify hostmetadata: %w", err)
	}
	return fmt.Errorf("sending packet: %w", err)
}
