package domain

import (
	"strings"
	"testing"
	"time"
)

func TestTelemetryNormalizeAndValidate(t *testing.T) {
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	item := Telemetry{
		DeviceID:    "  BAT-0001 ",
		SOC:         72.5,
		Voltage:     52.1,
		Current:     -12.2,
		Temperature: 34.8,
		CycleCount:  140,
		Status:      " Discharging ",
	}
	item.Normalize(now)
	if item.EventID == "" {
		t.Fatal("expected generated event ID")
	}
	if item.DeviceID != "BAT-0001" {
		t.Fatalf("expected trimmed device ID, got %q", item.DeviceID)
	}
	if err := item.Validate(now); err != nil {
		t.Fatalf("expected valid telemetry, got %v", err)
	}
}

func TestTelemetryValidationJoinsErrors(t *testing.T) {
	now := time.Now()
	item := Telemetry{SOC: 120, Temperature: 200, Status: "unknown"}
	item.Normalize(now)
	err := item.Validate(now)
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, expected := range []string{"device_id", "soc", "temperature", "status"} {
		if !strings.Contains(message, expected) {
			t.Fatalf("expected %q in %q", expected, message)
		}
	}
}
