package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/akash-anumolu/go-iot-observability-platform/internal/domain"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/metrics"
	"github.com/akash-anumolu/go-iot-observability-platform/internal/store"
)

var ErrQueueFull = errors.New("telemetry queue is full")

type Processor struct {
	queue   chan domain.Telemetry
	store   store.Store
	metrics *metrics.Metrics
	logger  *slog.Logger
	workers int
	wg      sync.WaitGroup
}

func NewProcessor(
	data store.Store,
	observability *metrics.Metrics,
	logger *slog.Logger,
	workers int,
	queueCapacity int,
) *Processor {
	return &Processor{
		queue:   make(chan domain.Telemetry, queueCapacity),
		store:   data,
		metrics: observability,
		logger:  logger,
		workers: workers,
	}
}

func (p *Processor) Start(ctx context.Context) {
	for workerID := 1; workerID <= p.workers; workerID++ {
		p.wg.Add(1)
		go p.worker(ctx, workerID)
	}
}

// Submit applies backpressure: callers wait until their context expires rather
// than allowing unbounded allocations during traffic spikes.
func (p *Processor) Submit(ctx context.Context, item domain.Telemetry) error {
	select {
	case p.queue <- item:
		p.metrics.SetQueueDepth(len(p.queue))
		return nil
	case <-ctx.Done():
		p.metrics.Rejected()
		return ErrQueueFull
	}
}

func (p *Processor) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Processor) worker(ctx context.Context, workerID int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-p.queue:
			p.metrics.SetQueueDepth(len(p.queue))
			started := time.Now()
			err := p.store.Save(item)
			p.metrics.ObserveProcessing(time.Since(started))
			switch {
			case errors.Is(err, store.ErrDuplicate):
				p.metrics.Duplicate()
			case err != nil:
				p.metrics.Rejected()
				p.logger.Error("telemetry persistence failed", "worker_id", workerID, "error", err)
			default:
				p.metrics.Accepted()
			}
		}
	}
}
