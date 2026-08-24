package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/tempoloss/moxy/internal/command"
	"github.com/tempoloss/moxy/internal/protocol"
	"github.com/tempoloss/moxy/internal/resp"
)

// Config controls the TCP server.
type Config struct {
	Addr     string
	MaxConns int
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

// EMFILE/ENFILE mean the process is out of descriptors; sleeping lets the
// reaper close sockets before the daemon exits.
func isTemporaryAccept(err error) bool {
	if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
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

	var slots chan struct{}
	if s.cfg.MaxConns > 0 {
		slots = make(chan struct{}, s.cfg.MaxConns)
	}

	var delay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) && isTemporaryAccept(err) {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
					if delay > time.Second {
						delay = time.Second
					}
				}
				slog.Warn("accept failed, retrying", "err", err, "delay", delay)
				time.Sleep(delay)
				continue
			}
			s.clearListener(listener)
			s.wg.Wait()
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		delay = 0

		if slots != nil {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				conn.Close()
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer func() {
				if slots != nil {
					<-slots
				}
				s.wg.Done()
			}()
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
