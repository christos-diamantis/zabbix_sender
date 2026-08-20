# zabbix-sender

Golang package, implementing zabbix sender protocol for sending metrics to zabbix. 
Supports modern Zabbix 7.0+ proxy group redirect and multi-host high availability.

## ✨ Features
- Send data on single host (Zabbix server or Proxy)
- Send data on multiple hosts in HA (like Zabbix Agent `ServerActive`)
- Proxy group redirects (Zabbix 7.0+)
- Active agent emulation
- Trapper items
- Host autoregistration
- Primary host caching (remembers working proxy)
- Configurable timeouts & redirect limits
- Goroutine-safe: one sender, unlimited concurrent sends

## 📦 Installation
```bash
go get github.com/christos-diamantis/zabbix_sender
```

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "time"

    "github.com/christos-diamantis/zabbix_sender"
)

func main() {
    // Single host
    sender := zabbix_sender.NewSender("zabbix-proxy:10051")

    // Multiple hosts in HA
    senderHA := zabbix_sender.NewSenderHosts([]string{"zabbix-proxy1:10051", "zabbix-proxy2:10051", "zabbix-proxy3"})

    // Create multiple metrics to send as batch
    var metrics []*zabbix_sender.Metric
    metrics = append(metrics, zabbix_sender.NewMetric("localhost", "cpu", "1.22", true, time.Now())) // Emulating Zabbix agent (active agent items) and specifying timestamp
    metrics = append(metrics, zabbix_sender.NewMetric("localhost", "status", "OK", true)) // Emulating Zabbix agent (active agent items)
    metrics = append(metrics, zabbix_sender.NewMetric("localhost", "someTrapper", "3.14", false)) // Sending on trapper item type

    // Send the metrics on the single host
    resActive, errActive, resTrapper, errTrapper := sender.SendMetrics(metrics)

    // Print the results of sending to single host
    fmt.Printf("Agent active, response=%s, info=%s, error=%v\n", resActive.Response, resActive.Info, errActive)
    fmt.Printf("Trapper, response=%s, info=%s, error=%v\n", resTrapper.Response, resTrapper.Info, errTrapper)

    // Send the metrics on the list of hosts
    resActiveHA, errActiveHA, resTrapperHA, errTrapperHA := senderHA.SendMetrics(metrics)

    // Print the results of sending to list of hosts
    fmt.Printf("Agent active, response=%s, info=%s, error=%v\n", resActiveHA.Response, resActiveHA.Info, errActiveHA)
    fmt.Printf("Trapper, response=%s, info=%s, error=%v\n", resTrapperHA.Response, resTrapperHA.Info, errTrapperHA)

}
```

## 📖 All Usage Examples
1. Single Host
```go
sender := zabbix_sender.NewSender("my-zabbix-proxy:10051")
```

2. Multiple Hosts
```go
hosts := []string{
    "my-zabbix-proxy1:10051",
    "my-zabbix-proxy2:10051",
    "my-zabbix-proxy3",
}
sender := zabbix_sender.NewSenderHosts(hosts)
sender.MaxRedirects = 3
sender.UpdateHost = true // cache final redirected proxy
```
**Behavior:** Tries cached `PrimaryHost` first -> falls back to list order -> caches first successful host.

3. Active Agent emulation
```go
// Emulate Zabbix Agent active checks
metrics := []*zabbix_sender.Metric{
    zabbix_sender.NewMetric("MyAgent", "agent.ping", "1", true),           // active=true
    zabbix_sender.NewMetric("MyAgent", "agent.version", "2.4", true),      // active=true
    zabbix_sender.NewMetric("MyAgent", "system.cpu.util", "15.2", true),   // active=true
}
resActive, _, _, _ := sender.SendMetrics(metrics) // uses "agent data" protocol
```

4. Trapper Items
```go
// Custom trapper items
metrics := []*zabbix_sender.Metric{
    zabbix_sender.NewMetric("AppServer", "app.metrics.custom", "123", false), // trapper=false
    zabbix_sender.NewMetric("Database", "db.connections", "47", false),
}
_, errTrapper, _, _ := sender.SendMetrics(metrics) // uses "sender data" protocol
```

5. Mixed Active + Trapper
```go
metrics := []*zabbix_sender.Metric{
    zabbix_sender.NewMetric("Host", "agent.ping", "1", true),    // → active packet
    zabbix_sender.NewMetric("Host", "custom.metric", "42", false), // → trapper packet
}

resActive, errActive, resTrapper, errTrapper := sender.SendMetrics(metrics)
// resActive = agent data response
// resTrapper = sender data response  
```

6. Host autoregistration
```go
err := sender.RegisterHost("NewHost", "Linux mysql nginx version 1.18")
if err != nil {
    log.Fatal(err)
}
```

7. Custom timeouts
```go
sender := zabbix_sender.NewSenderTimeout(
    "proxy:10051",
    10*time.Second,  // connect
    30*time.Second,  // read  
    10*time.Second,  // write
)
```

8. Parse response statistics
```go
info, err := resActive.GetInfo()
if err == nil {
    fmt.Printf("Processed: %d, Failed: %d, Total: %d (%.3fs)\n",
        info.Processed, info.Failed, info.Total, info.Spent.Seconds())
}
```

## 🔧 Advanced Configuration
```go
sender := zabbix_sender.NewSenderHosts(hosts)
sender.MaxRedirects = 10      // handle complex proxy groups
sender.UpdateHost = true      // permanently cache final proxy
sender.SetPrimaryHost("known-good-proxy:10051") // pre-set cached host
```

A `Sender` is safe for concurrent use: call `SendMetrics`/`Send` from as many goroutines as you like. Set the configuration fields (`Hosts`, `MaxRedirects`, timeouts, ...) before sharing the sender between goroutines.
```go
cached := sender.PrimaryHost() // read the cached working host
```

## 🛠️ Compatibility

- **Zabbix:** 4.0+ (redirects: 7.0+)

- Go: 1.20+

## 🙏 Credits
Forked & enhanced from **chmller/go-zabbix-sender**.


## 📄 License
MIT License - see [LICENSE](LICENSE) file

⭐ **Star this repo if it helps your Zabbix setup!**
