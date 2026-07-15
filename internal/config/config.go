package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Address         string
	Workers         int
	QueueCapacity   int
	HistoryLimit    int
	OfflineAfter    time.Duration
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Address:         envString("HTTP_ADDRESS", ":8080"),
		Workers:         envInt("WORKERS", 8),
		QueueCapacity:   envInt("QUEUE_CAPACITY", 4_096),
		HistoryLimit:    envInt("HISTORY_LIMIT", 1_000),
		OfflineAfter:    envDuration("OFFLINE_AFTER", 5*time.Minute),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	if cfg.Workers < 1 || cfg.Workers > 256 {
		return Config{}, fmt.Errorf("WORKERS must be between 1 and 256")
	}
	if cfg.QueueCapacity < cfg.Workers {
		return Config{}, fmt.Errorf("QUEUE_CAPACITY must be greater than or equal to WORKERS")
	}
	if cfg.HistoryLimit < 1 {
		return Config{}, fmt.Errorf("HISTORY_LIMIT must be positive")
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
