package zabbix_sender

import (
	"encoding/binary"
	"encoding/json"
	"time"
)

// Packet struct.
type Packet struct {
	Request      string    `json:"request"`
	Data         []*Metric `json:"data,omitempty"`
	Clock        int64     `json:"clock,omitempty"`
	NS           int       `json:"ns,omitempty"`
	Host         string    `json:"host,omitempty"`
	HostMetadata string    `json:"host_metadata,omitempty"`
}

// NewPacket returns a zabbix packet with a list of metrics
func NewPacket(data []*Metric, agentActive bool, t ...time.Time) *Packet {
	var request string
	if agentActive {
		request = "agent data"
	} else {
		request = "sender data"
	}

	p := &Packet{Request: request, Data: data}
	if len(t) > 0 {
		p.Clock = t[0].Unix()
		p.NS = t[0].Nanosecond()
	}
	return p
}

// DataLen Packet class method, return 8 bytes with packet length in little endian order
func (p *Packet) DataLen() []byte {
	dataLen := make([]byte, 8)
	JSONData, _ := json.Marshal(p)
	binary.LittleEndian.PutUint64(dataLen, uint64(len(JSONData)))
	return dataLen
}
