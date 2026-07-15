package store

import (
	"errors"
	"testing"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/domain"
)

func TestMemoryStoreDeduplicatesAndBoundsHistory(t *testing.T) {
	data := NewMemory(2)
	now := time.Now().UTC()
	for index := 0; index < 3; index++ {
		item := domain.Telemetry{
			EventID:   string(rune('a' + index)),
			DeviceID:  "BAT-1",
			Timestamp: now.Add(time.Duration(index) * time.Second),
			SOC:       float64(60 + index),
		}
		if err := data.Save(item); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}
	if got := data.History("BAT-1", 10); len(got) != 2 {
		t.Fatalf("expected bounded history of 2, got %d", len(got))
	}
	if latest, ok := data.Latest("BAT-1"); !ok || latest.SOC != 62 {
		t.Fatalf("unexpected latest item: %+v, %v", latest, ok)
	}
	if err := data.Save(domain.Telemetry{EventID: "c", DeviceID: "BAT-1"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestMemoryStoreSummary(t *testing.T) {
	data := NewMemory(10)
	now := time.Now().UTC()
	items := []domain.Telemetry{
		{EventID: "1", DeviceID: "online", Timestamp: now, SOC: 50, Temperature: 30, Status: domain.StatusCharging},
		{EventID: "2", DeviceID: "offline", Timestamp: now.Add(-10 * time.Minute), SOC: 5, Temperature: 60, Status: domain.StatusIdle},
	}
	for _, item := range items {
		if err := data.Save(item); err != nil {
			t.Fatal(err)
		}
	}
	summary := data.Summary(now, 5*time.Minute)
	if summary.TotalDevices != 2 || summary.OnlineDevices != 1 || summary.OfflineDevices != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.CriticalSOCDevices != 1 || summary.HighTempDevices != 1 || summary.AverageSOC != 27.5 {
		t.Fatalf("unexpected fleet calculations: %+v", summary)
	}
}
