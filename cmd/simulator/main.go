package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/domain"
)

type batchRequest struct {
	Items []domain.Telemetry `json:"items"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	endpoint := envString("TELEMETRY_URL", "http://localhost:8080/api/v1/telemetry/batch")
	deviceCount := envInt("DEVICE_COUNT", 25)
	interval := envDuration("SIMULATION_INTERVAL", 2*time.Second)

	client := &http.Client{Timeout: 5 * time.Second}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("simulator started", "endpoint", endpoint, "devices", deviceCount, "interval", interval)
	for {
		select {
		case <-ctx.Done():
			logger.Info("simulator stopped")
			return
		case now := <-ticker.C:
			items := make([]domain.Telemetry, 0, deviceCount)
			for index := 1; index <= deviceCount; index++ {
				items = append(items, simulatedTelemetry(rng, index, now))
			}
			if err := postBatch(ctx, client, endpoint, items); err != nil {
				logger.Error("batch delivery failed", "error", err)
			} else {
				logger.Info("batch delivered", "events", len(items))
			}
		}
	}
}

func simulatedTelemetry(rng *rand.Rand, index int, now time.Time) domain.Telemetry {
	phase := float64(now.Unix()%3600)/3600*2*math.Pi + float64(index)/10
	soc := clamp(55+35*math.Sin(phase)+rng.NormFloat64()*2, 2, 100)
	temperature := 31 + math.Abs(20*math.Sin(phase/2)) + rng.NormFloat64()*1.5
	status := domain.StatusDischarging
	current := -(8 + rng.Float64()*12)
	if math.Cos(phase) > 0.35 {
		status = domain.StatusCharging
		current = 10 + rng.Float64()*15
	} else if math.Abs(math.Cos(phase)) < 0.08 {
		status = domain.StatusIdle
		current = rng.NormFloat64()
	}
	return domain.Telemetry{
		EventID:     fmt.Sprintf("sim-%03d-%d", index, now.UnixNano()),
		DeviceID:    fmt.Sprintf("BAT-%04d", index),
		Timestamp:   now.UTC(),
		SOC:         round(soc, 2),
		Voltage:     round(44+soc*0.12+rng.NormFloat64()*0.15, 2),
		Current:     round(current, 2),
		Temperature: round(temperature, 2),
		CycleCount:  80 + index*7 + int(now.Unix()/86_400)%200,
		Status:      status,
	}
}

func postBatch(ctx context.Context, client *http.Client, endpoint string, items []domain.Telemetry) error {
	body, err := json.Marshal(batchRequest{Items: items})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("telemetry API returned %s", response.Status)
	}
	return nil
}

func clamp(value, minimum, maximum float64) float64 {
	return math.Max(minimum, math.Min(maximum, value))
}

func round(value float64, places int) float64 {
	power := math.Pow10(places)
	return math.Round(value*power) / power
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

