package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/ingest"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/metrics"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/store"
)

func TestIngestAndReadDevice(t *testing.T) {
	handler, data, cancel := testServer(t)
	defer cancel()
	payload := `{
		"event_id":"evt-1",
		"device_id":"BAT-1001",
		"timestamp":"2026-07-15T10:00:00Z",
		"soc":73.4,
		"voltage":52.2,
		"current":-8.1,
		"temperature":33.5,
		"cycle_count":120,
		"status":"discharging"
	}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", strings.NewReader(payload))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}

	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := data.Latest("BAT-1001"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not persist telemetry")
		}
		time.Sleep(5 * time.Millisecond)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/devices/BAT-1001", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "BAT-1001") {
		t.Fatalf("unexpected device response: %d %s", response.Code, response.Body.String())
	}
}

func TestRejectsInvalidTelemetry(t *testing.T) {
	handler, _, cancel := testServer(t)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(`{"device_id":"BAT-1","soc":150}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", response.Code, response.Body.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	handler, _, cancel := testServer(t)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "iot_ingestion_queue_capacity") {
		t.Fatalf("unexpected metrics response: %d %s", response.Code, response.Body.String())
	}
}

func testServer(t *testing.T) (http.Handler, *store.MemoryStore, context.CancelFunc) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	data := store.NewMemory(100)
	observability := metrics.New(32, 2)
	processor := ingest.NewProcessor(data, observability, logger, 2, 32)
	ctx, cancel := context.WithCancel(context.Background())
	processor.Start(ctx)
	server := NewServer(processor, data, observability, logger, 5*time.Minute)
	return server.Handler(), data, cancel
}
