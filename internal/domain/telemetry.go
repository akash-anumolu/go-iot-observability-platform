package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StatusCharging    = "charging"
	StatusDischarging = "discharging"
	StatusIdle        = "idle"
)

// Telemetry represents one battery observation received from an IoT device.
type Telemetry struct {
	EventID     string    `json:"event_id"`
	DeviceID    string    `json:"device_id"`
	Timestamp   time.Time `json:"timestamp"`
	SOC         float64   `json:"soc"`
	Voltage     float64   `json:"voltage"`
	Current     float64   `json:"current"`
	Temperature float64   `json:"temperature"`
	CycleCount  int       `json:"cycle_count"`
	Status      string    `json:"status"`
}

// Normalize fills safe defaults before validation and ingestion.
func (t *Telemetry) Normalize(now time.Time) {
	t.DeviceID = strings.TrimSpace(t.DeviceID)
	t.EventID = strings.TrimSpace(t.EventID)
	t.Status = strings.ToLower(strings.TrimSpace(t.Status))
	if t.Timestamp.IsZero() {
		t.Timestamp = now.UTC()
	} else {
		t.Timestamp = t.Timestamp.UTC()
	}
	if t.Status == "" {
		t.Status = StatusIdle
	}
	if t.EventID == "" && t.DeviceID != "" {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", t.DeviceID, t.Timestamp.UnixNano())))
		t.EventID = hex.EncodeToString(sum[:12])
	}
}

// Validate protects the ingestion pipeline from malformed or physically
// impossible observations.
func (t Telemetry) Validate(now time.Time) error {
	var errs []error
	if t.DeviceID == "" {
		errs = append(errs, errors.New("device_id is required"))
	}
	if len(t.DeviceID) > 100 {
		errs = append(errs, errors.New("device_id must not exceed 100 characters"))
	}
	if t.EventID == "" {
		errs = append(errs, errors.New("event_id is required"))
	}
	if t.Timestamp.After(now.Add(5 * time.Minute)) {
		errs = append(errs, errors.New("timestamp cannot be more than 5 minutes in the future"))
	}
	if t.SOC < 0 || t.SOC > 100 {
		errs = append(errs, errors.New("soc must be between 0 and 100"))
	}
	if t.Voltage < 0 || t.Voltage > 1_000 {
		errs = append(errs, errors.New("voltage must be between 0 and 1000"))
	}
	if t.Current < -2_000 || t.Current > 2_000 {
		errs = append(errs, errors.New("current must be between -2000 and 2000"))
	}
	if t.Temperature < -60 || t.Temperature > 150 {
		errs = append(errs, errors.New("temperature must be between -60 and 150"))
	}
	if t.CycleCount < 0 {
		errs = append(errs, errors.New("cycle_count cannot be negative"))
	}
	switch t.Status {
	case StatusCharging, StatusDischarging, StatusIdle:
	default:
		errs = append(errs, fmt.Errorf("status must be %q, %q, or %q", StatusCharging, StatusDischarging, StatusIdle))
	}
	return errors.Join(errs...)
}

// FleetSummary is the aggregate view returned by the fleet endpoint.
type FleetSummary struct {
	TotalDevices       int     `json:"total_devices"`
	OnlineDevices      int     `json:"online_devices"`
	OfflineDevices     int     `json:"offline_devices"`
	ChargingDevices    int     `json:"charging_devices"`
	CriticalSOCDevices int     `json:"critical_soc_devices"`
	HighTempDevices    int     `json:"high_temperature_devices"`
	AverageSOC         float64 `json:"average_soc"`
}
