package service

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/an8kk/moxy/internal/core"
	"github.com/an8kk/moxy/internal/queue"
	"github.com/an8kk/moxy/internal/task"
)

var ErrInvalidQueueName = errors.New("queue name must not be empty")

// BackendFactory creates a queue backend for one named queue.
type BackendFactory func(queueName string) queue.Backend

// Stats reports the state of one service-managed queue.
type Stats = core.Stats

// ServiceConfig controls engines created by the service.
type ServiceConfig struct {
	Engine core.EngineConfig
}

// Service owns multiple named queues.
type Service struct {
	mu      sync.Mutex
	factory BackendFactory
	engines map[string]*core.Engine
	config  ServiceConfig
}

// New creates a service that lazily creates engines from the backend factory.
func New(factory BackendFactory) *Service {
	return NewWithConfig(factory, ServiceConfig{})
}

// NewWithConfig creates a service that applies config to lazily created engines.
func NewWithConfig(factory BackendFactory, config ServiceConfig) *Service {
	return &Service{
		factory: factory,
		engines: make(map[string]*core.Engine),
		config:  config,
	}
}

// Enqueue appends a task payload to a named queue, creating the queue on demand.
func (s *Service) Enqueue(queueName string, payload []byte) (task.Task, error) {
	engine, err := s.engineFor(queueName)
	if err != nil {
		return task.Task{}, err
	}

	return engine.Enqueue(payload)
}

// Fetch leases one task from a named queue.
func (s *Service) Fetch(queueName string, timeout time.Duration) (*core.Lease, error) {
	engine, err := s.engineFor(queueName)
	if err != nil {
		return nil, err
	}

	return engine.Fetch(timeout)
}

// Ack completes a lease by scanning the service's existing queues.
func (s *Service) Ack(leaseID string) error {
	engines := s.existingEngines()
	for _, engine := range engines {
		err := engine.Ack(leaseID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, core.ErrLeaseNotFound) {
			return err
		}
	}

	return core.ErrLeaseNotFound
}

// ReapExpired reaps expired leases across all existing queues.
func (s *Service) ReapExpired(now time.Time) (int, error) {
	engines := s.existingEngines()
	total := 0
	var errs []error
	for _, engine := range engines {
		requeued, err := engine.ReapExpired(now)
		total += requeued
		if err != nil {
			errs = append(errs, err)
		}
	}

	return total, errors.Join(errs...)
}

// Stats returns counts for a named queue.
func (s *Service) Stats(queueName string) (Stats, bool) {
	if !validQueueName(queueName) {
		return Stats{}, false
	}

	s.mu.Lock()
	engine, ok := s.engines[queueName]
	s.mu.Unlock()
	if !ok {
		return Stats{}, false
	}

	return engine.Stats(), true
}

func (s *Service) engineFor(queueName string) (*core.Engine, error) {
	if !validQueueName(queueName) {
		return nil, ErrInvalidQueueName
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	engine, ok := s.engines[queueName]
	if ok {
		return engine, nil
	}

	engine = core.NewEngineWithBackendAndConfig(s.factory(queueName), s.config.Engine)
	s.engines[queueName] = engine
	return engine, nil
}

func (s *Service) existingEngines() []*core.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()

	engines := make([]*core.Engine, 0, len(s.engines))
	for _, engine := range s.engines {
		engines = append(engines, engine)
	}
	return engines
}

func validQueueName(queueName string) bool {
	return strings.TrimSpace(queueName) != ""
}
