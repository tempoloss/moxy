package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/an8kk/moxy/internal/command"
	"github.com/an8kk/moxy/internal/protocol"
	"github.com/an8kk/moxy/internal/resp"
)

// Config controls the TCP server.
type Config struct {
	Addr string
}

// Server accepts RESP commands over TCP and dispatches them to a command handler.
type Server struct {
	handler *command.Handler
	cfg     Config

	mu       sync.Mutex
	listener net.Listener
	wg       sync.WaitGroup
}

func New(handler *command.Handler, cfg Config) *Server {
	return &Server{handler: handler, cfg: cfg}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := s.cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:6380"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.setListener(listener)

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("close listener", "err", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.clearListener(listener)
			s.wg.Wait()
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	adapter := protocol.NewAdapter(s.handler)

	for {
		value, err := reader.ReadValue()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if writeErr := writer.WriteValue(resp.Error("ERR " + err.Error())); writeErr != nil {
				slog.Error("write protocol error", "err", writeErr)
			}
			return
		}

		if err := writer.WriteValue(adapter.Handle(value)); err != nil {
			slog.Error("write response", "err", err)
			return
		}
	}
}

func (s *Server) setListener(listener net.Listener) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.listener = listener
}

func (s *Server) clearListener(listener net.Listener) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == listener {
		s.listener = nil
	}
}
