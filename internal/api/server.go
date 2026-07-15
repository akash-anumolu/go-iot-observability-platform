package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/domain"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/ingest"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/metrics"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/store"
)

const maxBatchSize = 500

type Server struct {
	processor    *ingest.Processor
	store        store.Store
	metrics      *metrics.Metrics
	logger       *slog.Logger
	offlineAfter time.Duration
	startedAt    time.Time
}

func NewServer(
	processor *ingest.Processor,
	data store.Store,
	observability *metrics.Metrics,
	logger *slog.Logger,
	offlineAfter time.Duration,
) *Server {
	return &Server{
		processor:    processor,
		store:        data,
		metrics:      observability,
		logger:       logger,
		offlineAfter: offlineAfter,
		startedAt:    time.Now().UTC(),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler(s.store))
	mux.HandleFunc("POST /api/v1/telemetry", s.ingestOne)
	mux.HandleFunc("POST /api/v1/telemetry/batch", s.ingestBatch)
	mux.HandleFunc("GET /api/v1/devices", s.listDevices)
	mux.HandleFunc("GET /api/v1/devices/{deviceID}", s.getDevice)
	mux.HandleFunc("GET /api/v1/devices/{deviceID}/history", s.getHistory)
	mux.HandleFunc("GET /api/v1/fleet/summary", s.fleetSummary)
	return s.recover(s.requestLog(s.securityHeaders(mux)))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"started_at": s.startedAt,
		"uptime":     time.Since(s.startedAt).Round(time.Second).String(),
	})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) ingestOne(w http.ResponseWriter, r *http.Request) {
	var item domain.Telemetry
	if err := decodeJSON(w, r, &item); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	item.Normalize(time.Now())
	if err := item.Validate(time.Now()); err != nil {
		s.metrics.Rejected()
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
	defer cancel()
	if err := s.processor.Submit(ctx, item); err != nil {
		writeError(w, http.StatusServiceUnavailable, "queue_unavailable", "ingestion queue is at capacity; retry later")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":   "queued",
		"event_id": item.EventID,
	})
}

type batchRequest struct {
	Items []domain.Telemetry `json:"items"`
}

type batchError struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

func (s *Server) ingestBatch(w http.ResponseWriter, r *http.Request) {
	var request batchRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(request.Items) == 0 || len(request.Items) > maxBatchSize {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", fmt.Sprintf("items must contain between 1 and %d events", maxBatchSize))
		return
	}

	queued := 0
	rejected := make([]batchError, 0)
	now := time.Now()
	for index := range request.Items {
		item := request.Items[index]
		item.Normalize(now)
		if err := item.Validate(now); err != nil {
			s.metrics.Rejected()
			rejected = append(rejected, batchError{Index: index, Message: err.Error()})
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
		err := s.processor.Submit(ctx, item)
		cancel()
		if err != nil {
			rejected = append(rejected, batchError{Index: index, Message: err.Error()})
			continue
		}
		queued++
	}
	status := http.StatusAccepted
	if queued == 0 {
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, map[string]any{
		"queued":   queued,
		"rejected": rejected,
	})
}

func (s *Server) listDevices(w http.ResponseWriter, _ *http.Request) {
	items := s.store.LatestSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(items),
		"items": items,
	})
}

func (s *Server) getDevice(w http.ResponseWriter, r *http.Request) {
	deviceID := strings.TrimSpace(r.PathValue("deviceID"))
	item, ok := s.store.Latest(deviceID)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "device was not found")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) getHistory(w http.ResponseWriter, r *http.Request) {
	deviceID := strings.TrimSpace(r.PathValue("deviceID"))
	if _, ok := s.store.Latest(deviceID); !ok {
		writeError(w, http.StatusNotFound, "not_found", "device was not found")
		return
	}
	limit := 100
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1_000 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	items := s.store.History(deviceID, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id": deviceID,
		"count":     len(items),
		"items":     items,
	})
}

func (s *Server) fleetSummary(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Summary(time.Now(), s.offlineAfter))
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Info("request completed", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("request panic", "error", recovered, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("request body must contain one JSON object")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
