package metrics

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/store"
)

type Metrics struct {
	accepted       atomic.Uint64
	rejected       atomic.Uint64
	duplicates     atomic.Uint64
	queueDepth     atomic.Int64
	queueCapacity  atomic.Int64
	workers        atomic.Int64
	processingNanos atomic.Uint64
	processed      atomic.Uint64
}

func New(queueCapacity, workers int) *Metrics {
	m := &Metrics{}
	m.queueCapacity.Store(int64(queueCapacity))
	m.workers.Store(int64(workers))
	return m
}

func (m *Metrics) Accepted()                    { m.accepted.Add(1) }
func (m *Metrics) Rejected()                    { m.rejected.Add(1) }
func (m *Metrics) Duplicate()                   { m.duplicates.Add(1) }
func (m *Metrics) SetQueueDepth(depth int)      { m.queueDepth.Store(int64(depth)) }
func (m *Metrics) ObserveProcessing(d time.Duration) {
	m.processingNanos.Add(uint64(d))
	m.processed.Add(1)
}

func (m *Metrics) Handler(data store.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		writeHelp(&b, "iot_telemetry_ingested_total", "Telemetry events handled by result.", "counter")
		fmt.Fprintf(&b, "iot_telemetry_ingested_total{result=\"accepted\"} %d\n", m.accepted.Load())
		fmt.Fprintf(&b, "iot_telemetry_ingested_total{result=\"rejected\"} %d\n", m.rejected.Load())
		fmt.Fprintf(&b, "iot_telemetry_ingested_total{result=\"duplicate\"} %d\n", m.duplicates.Load())
		writeGauge(&b, "iot_ingestion_queue_depth", "Current telemetry queue depth.", float64(m.queueDepth.Load()))
		writeGauge(&b, "iot_ingestion_queue_capacity", "Configured telemetry queue capacity.", float64(m.queueCapacity.Load()))
		writeGauge(&b, "iot_ingestion_workers", "Configured ingestion workers.", float64(m.workers.Load()))

		writeHelp(&b, "iot_telemetry_processing_duration_seconds", "Worker processing time.", "summary")
		fmt.Fprintf(&b, "iot_telemetry_processing_duration_seconds_sum %s\n", floatString(float64(m.processingNanos.Load())/float64(time.Second)))
		fmt.Fprintf(&b, "iot_telemetry_processing_duration_seconds_count %d\n", m.processed.Load())

		items := data.LatestSnapshot()
		writeGauge(&b, "iot_devices_total", "Devices with at least one telemetry event.", float64(len(items)))
		writeHelp(&b, "iot_device_soc_percent", "Latest device state of charge.", "gauge")
		writeHelp(&b, "iot_device_voltage_volts", "Latest device voltage.", "gauge")
		writeHelp(&b, "iot_device_temperature_celsius", "Latest device temperature.", "gauge")
		writeHelp(&b, "iot_device_last_seen_timestamp_seconds", "Latest device event timestamp.", "gauge")
		for _, item := range items {
			label := escapeLabel(item.DeviceID)
			fmt.Fprintf(&b, "iot_device_soc_percent{device_id=\"%s\"} %s\n", label, floatString(item.SOC))
			fmt.Fprintf(&b, "iot_device_voltage_volts{device_id=\"%s\"} %s\n", label, floatString(item.Voltage))
			fmt.Fprintf(&b, "iot_device_temperature_celsius{device_id=\"%s\"} %s\n", label, floatString(item.Temperature))
			fmt.Fprintf(&b, "iot_device_last_seen_timestamp_seconds{device_id=\"%s\"} %d\n", label, item.Timestamp.Unix())
		}
		_, _ = w.Write([]byte(b.String()))
	})
}

func writeHelp(b *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	writeHelp(b, name, help, "gauge")
	fmt.Fprintf(b, "%s %s\n", name, floatString(value))
}

func floatString(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}

