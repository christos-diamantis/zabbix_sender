//go:build integration

package zabbix_sender

// Integration tests against a real Zabbix server/proxy.
//
// Run with:
//
//	ZABBIX_ADDR=127.0.0.1:10051 go test -tags integration -v ./...
//
// ZABBIX_ADDR may hold a comma-separated list of addresses for the HA tests.
// The target Zabbix must have a host (default "test-api", override with
// ZABBIX_TEST_HOST) with a trapper item (default key "master_item", override
// with ZABBIX_TEST_KEY).

import (
	"os"
	"strings"
	"testing"
)

func integrationHosts(t *testing.T) []string {
	t.Helper()
	addr := os.Getenv("ZABBIX_ADDR")
	if addr == "" {
		t.Skip("ZABBIX_ADDR not set, skipping integration test")
	}
	return strings.Split(addr, ",")
}

func integrationTestItem() (host, key string) {
	host = os.Getenv("ZABBIX_TEST_HOST")
	if host == "" {
		host = "test-api"
	}
	key = os.Getenv("ZABBIX_TEST_KEY")
	if key == "" {
		key = "master_item"
	}
	return host, key
}

func TestIntegration_SendTrapperMetric(t *testing.T) {
	hosts := integrationHosts(t)
	testHost, testKey := integrationTestItem()

	z := NewSenderHosts(hosts)
	z.MaxRedirects = 5

	metrics := []*Metric{
		NewMetric(testHost, testKey, "integration-test", false),
	}

	_, _, resTrapper, errTrapper := z.SendMetrics(metrics)
	if errTrapper != nil {
		t.Fatalf("sending trapper metric: %v", errTrapper)
	}

	info, err := resTrapper.GetInfo()
	if err != nil {
		t.Fatalf("parsing response info: %v", err)
	}
	t.Logf("processed=%d failed=%d total=%d spent=%v", info.Processed, info.Failed, info.Total, info.Spent)

	if info.Total != 1 {
		t.Errorf("expected total=1, got %d", info.Total)
	}
	if info.Processed != 1 {
		t.Errorf("expected processed=1, got %d (is the trapper item configured?)", info.Processed)
	}

	if z.PrimaryHost == "" {
		t.Error("PrimaryHost should be cached after a successful send")
	}
}

func TestIntegration_PrimaryHostReuse(t *testing.T) {
	hosts := integrationHosts(t)
	testHost, testKey := integrationTestItem()

	z := NewSenderHosts(hosts)

	metrics := []*Metric{NewMetric(testHost, testKey, "integration-test-1", false)}
	if _, _, _, err := z.SendMetrics(metrics); err != nil {
		t.Fatalf("first send: %v", err)
	}

	cached := z.PrimaryHost
	metrics = []*Metric{NewMetric(testHost, testKey, "integration-test-2", false)}
	if _, _, _, err := z.SendMetrics(metrics); err != nil {
		t.Fatalf("second send: %v", err)
	}

	if z.PrimaryHost != cached {
		t.Errorf("PrimaryHost changed between sends: %s -> %s", cached, z.PrimaryHost)
	}
}

func TestIntegration_RegisterHost(t *testing.T) {
	integrationHosts(t)

	hostName := os.Getenv("ZABBIX_REGISTER_HOST")
	if hostName == "" {
		t.Skip("ZABBIX_REGISTER_HOST not set, skipping autoregistration test")
	}

	z := NewSenderHosts(integrationHosts(t))
	if err := z.RegisterHost(hostName, os.Getenv("ZABBIX_REGISTER_METADATA")); err != nil {
		t.Fatalf("RegisterHost: %v", err)
	}
}
