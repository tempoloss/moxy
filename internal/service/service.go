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

type BackendFactory func(queueName string) queue.Backend

type Stats = core.Stats

type Service struct {
	mu      sync.Mutex
	factory BackendFactory
	engines map[string]*core.Engine
}

func New(factory BackendFactory) *Service {
	return &Service{
		factory: factory,
		engines: make(map[string]*core.Engine),
	}
}

func (s *Service) Enqueue(queueName string, payload []byte) (task.Task, error) {
	engine, err := s.engineFor(queueName)
	if err != nil {
		return task.Task{}, err
	}

	return engine.Enqueue(payload), nil
}

func (s *Service) Fetch(queueName string, timeout time.Duration) (*core.Lease, error) {
	engine, err := s.engineFor(queueName)
	if err != nil {
		return nil, err
	}

	return engine.Fetch(timeout)
}

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

	engine = core.NewEngineWithBackend(s.factory(queueName))
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
