package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/domain"
)

var ErrDuplicate = errors.New("duplicate telemetry event")

type Store interface {
	Save(domain.Telemetry) error
	Latest(deviceID string) (domain.Telemetry, bool)
	LatestSnapshot() []domain.Telemetry
	History(deviceID string, limit int) []domain.Telemetry
	Summary(now time.Time, offlineAfter time.Duration) domain.FleetSummary
}

// MemoryStore is a concurrency-safe reference store. The interface keeps the
// ingestion layer independent from persistence so PostgreSQL or MongoDB can be
// added without changing handlers or workers.
type MemoryStore struct {
	mu           sync.RWMutex
	latest       map[string]domain.Telemetry
	history      map[string][]domain.Telemetry
	seen         map[string]struct{}
	seenOrder    []string
	seenHead     int
	seenLimit    int
	historyLimit int
}

func NewMemory(historyLimit int) *MemoryStore {
	seenLimit := historyLimit * 100
	if seenLimit < 10_000 {
		seenLimit = 10_000
	}
	return &MemoryStore{
		latest:       make(map[string]domain.Telemetry),
		history:      make(map[string][]domain.Telemetry),
		seen:         make(map[string]struct{}),
		seenOrder:    make([]string, 0, seenLimit),
		seenLimit:    seenLimit,
		historyLimit: historyLimit,
	}
}

func (s *MemoryStore) Save(item domain.Telemetry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.seen[item.EventID]; exists {
		return ErrDuplicate
	}
	s.seen[item.EventID] = struct{}{}
	s.seenOrder = append(s.seenOrder, item.EventID)
	if len(s.seenOrder)-s.seenHead > s.seenLimit {
		delete(s.seen, s.seenOrder[s.seenHead])
		s.seenHead++
	}
	if s.seenHead >= s.seenLimit {
		s.seenOrder = append([]string(nil), s.seenOrder[s.seenHead:]...)
		s.seenHead = 0
	}

	current, exists := s.latest[item.DeviceID]
	if !exists || item.Timestamp.After(current.Timestamp) {
		s.latest[item.DeviceID] = item
	}

	history := append(s.history[item.DeviceID], item)
	sort.Slice(history, func(i, j int) bool { return history[i].Timestamp.Before(history[j].Timestamp) })
	if overflow := len(history) - s.historyLimit; overflow > 0 {
		copy(history, history[overflow:])
		history = history[:s.historyLimit]
	}
	s.history[item.DeviceID] = history
	return nil
}

func (s *MemoryStore) Latest(deviceID string) (domain.Telemetry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.latest[deviceID]
	return item, ok
}

func (s *MemoryStore) LatestSnapshot() []domain.Telemetry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.Telemetry, 0, len(s.latest))
	for _, item := range s.latest {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].DeviceID < items[j].DeviceID })
	return items
}

func (s *MemoryStore) History(deviceID string, limit int) []domain.Telemetry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := s.history[deviceID]
	if limit <= 0 || limit > len(history) {
		limit = len(history)
	}
	result := make([]domain.Telemetry, limit)
	for i := 0; i < limit; i++ {
		result[i] = history[len(history)-1-i]
	}
	return result
}

func (s *MemoryStore) Summary(now time.Time, offlineAfter time.Duration) domain.FleetSummary {
	items := s.LatestSnapshot()
	summary := domain.FleetSummary{TotalDevices: len(items)}
	var socTotal float64
	for _, item := range items {
		socTotal += item.SOC
		if now.Sub(item.Timestamp) > offlineAfter {
			summary.OfflineDevices++
		} else {
			summary.OnlineDevices++
		}
		if item.Status == domain.StatusCharging {
			summary.ChargingDevices++
		}
		if item.SOC < 10 {
			summary.CriticalSOCDevices++
		}
		if item.Temperature > 55 {
			summary.HighTempDevices++
		}
	}
	if len(items) > 0 {
		summary.AverageSOC = socTotal / float64(len(items))
	}
	return summary
}
