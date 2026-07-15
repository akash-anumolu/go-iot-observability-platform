package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/api"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/config"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/ingest"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/metrics"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	data := store.NewMemory(cfg.HistoryLimit)
	observability := metrics.New(cfg.QueueCapacity, cfg.Workers)
	processor := ingest.NewProcessor(data, observability, logger, cfg.Workers, cfg.QueueCapacity)
	handler := api.NewServer(processor, data, observability, logger, cfg.OfflineAfter).Handler()

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	processor.Start(workerCtx)

	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("http server started", "address", cfg.Address, "workers", cfg.Workers)
		serverErrors <- server.ListenAndServe()
	}()

	signals, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-signals.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped unexpectedly", "error", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "error", err)
	}
	stopWorkers()
	if err := processor.Wait(shutdownCtx); err != nil {
		logger.Warn("workers did not stop before deadline", "error", err)
	}
	logger.Info("shutdown complete")
}
