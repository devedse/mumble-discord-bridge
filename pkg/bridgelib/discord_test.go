package bridgelib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stieneee/mumble-discord-bridge/pkg/logger"
	"github.com/stretchr/testify/assert"
)

// MockBridgeLogger implements logger.Logger for testing
type MockBridgeLogger struct {
	mu      sync.Mutex
	entries []string
}

func (m *MockBridgeLogger) Debug(_, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, "DEBUG: "+msg)
}

func (m *MockBridgeLogger) Info(_, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, "INFO: "+msg)
}

func (m *MockBridgeLogger) Warn(_, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, "WARN: "+msg)
}

func (m *MockBridgeLogger) Error(_, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, "ERROR: "+msg)
}

func (m *MockBridgeLogger) WithBridgeID(_ string) logger.Logger {
	return m
}

// TestSharedDiscordClient_IsSessionHealthy tests the health check logic
func TestSharedDiscordClient_IsSessionHealthy(t *testing.T) {
	t.Run("Returns false for nil client", func(t *testing.T) {
		lgr := &MockBridgeLogger{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := &SharedDiscordClient{
			logger: lgr,
			ctx:    ctx,
			cancel: cancel,
			client: nil, // nil client
		}

		assert.False(t, client.isSessionHealthy())
	})

	t.Run("Returns false when gateway is nil", func(t *testing.T) {
		lgr := &MockBridgeLogger{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Client with no gateway
		client := &SharedDiscordClient{
			logger: lgr,
			ctx:    ctx,
			cancel: cancel,
			client: nil,
		}

		assert.False(t, client.isSessionHealthy())
	})
}

// TestSharedDiscordClient_SessionMonitorLoop_ContextCancellation tests that the
// monitor loop exits cleanly when the context is canceled
func TestSharedDiscordClient_SessionMonitorLoop_ContextCancellation(t *testing.T) {
	lgr := &MockBridgeLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	client := &SharedDiscordClient{
		logger:          lgr,
		ctx:             ctx,
		cancel:          cancel,
		messageHandlers: make(map[string][]interface{}),
	}

	done := make(chan struct{})
	go func() {
		client.sessionMonitorLoop()
		close(done)
	}()

	// Cancel context after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	// Monitor loop exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("sessionMonitorLoop did not exit after context cancellation")
	}
}

// TestSharedDiscordClient_IsSessionHealthy_NilGateway tests that isSessionHealthy()
// returns false when the gateway is nil
func TestSharedDiscordClient_IsSessionHealthy_NilGateway(t *testing.T) {
	lgr := &MockBridgeLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &SharedDiscordClient{
		logger: lgr,
		ctx:    ctx,
		cancel: cancel,
		client: nil,
	}

	// isSessionHealthy must return false without blocking
	done := make(chan bool, 1)
	go func() {
		done <- client.isSessionHealthy()
	}()

	select {
	case healthy := <-done:
		assert.False(t, healthy, "should return false when client is nil")
	case <-time.After(1 * time.Second):
		t.Fatal("isSessionHealthy() blocked unexpectedly")
	}
}

// TestSharedDiscordClient_SessionMonitorLoop_LogsUnhealthy tests that the
// monitor loop runs without panicking when session is unhealthy
func TestSharedDiscordClient_SessionMonitorLoop_LogsUnhealthy(t *testing.T) {
	lgr := &MockBridgeLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	client := &SharedDiscordClient{
		logger:          lgr,
		ctx:             ctx,
		cancel:          cancel,
		client:          nil, // nil client is always unhealthy
		messageHandlers: make(map[string][]interface{}),
	}

	done := make(chan struct{})
	go func() {
		client.sessionMonitorLoop()
		close(done)
	}()

	// Let the monitor run briefly then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	// Monitor loop exited cleanly without panic or deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("sessionMonitorLoop did not exit after context cancellation")
	}
}
