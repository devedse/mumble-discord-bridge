package bridge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiscord_SessionNotReadyWaits tests that manager handles not-ready client gracefully
func TestDiscord_SessionNotReadyWaits(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	time.Sleep(50 * time.Millisecond)

	status := manager.GetStatus()
	assert.True(t, status == ConnectionConnecting || status == ConnectionFailed || status == ConnectionDisconnected,
		"Expected connecting, failed, or disconnected, got %s", status)

	cancel()
	err := manager.Stop()
	require.NoError(t, err)
}

// TestDiscord_ConcurrentGetVoiceConn tests multiple readers during state change
func TestDiscord_ConcurrentGetVoiceConn(_ *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, ready := manager.GetVoiceConn()
			_ = conn
			_ = ready
		}()
	}

	wg.Wait()
}

// TestDiscord_UpdateChannel tests thread-safe channel update
func TestDiscord_UpdateChannel(_ *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "initial-channel", logger, emitter)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			manager.UpdateChannel("channel-" + string(rune('0'+idx%10)))
		}(i)
	}

	wg.Wait()
}

// TestDiscord_StopIdempotent ensures multiple Stop calls are safe
func TestDiscord_StopIdempotent(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	manager.InitContext(ctx)
	cancel()

	for i := 0; i < 5; i++ {
		err := manager.Stop()
		require.NoError(t, err, "Stop call %d failed", i)
	}
}

// TestDiscord_GetVoiceConn tests GetVoiceConn method
func TestDiscord_GetVoiceConn(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	// Before starting, should return nil
	conn, ready := manager.GetVoiceConn()
	assert.Nil(t, conn)
	assert.False(t, ready)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	conn, ready = manager.GetVoiceConn()
	_ = conn
	_ = ready

	cancel()
	err := manager.Stop()
	require.NoError(t, err)
}

// TestDiscord_GetVoiceConn_ChecksReady verifies GetVoiceConn checks connReady flag
func TestDiscord_GetVoiceConn_ChecksReady(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	// Set connReady=false
	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = false
	manager.connMutex.Unlock()

	// Should return nil when connReady=false
	conn, ready := manager.GetVoiceConn()
	assert.Nil(t, conn, "Should return nil when connReady is false")
	assert.False(t, ready)

	// Set connReady=true
	manager.connMutex.Lock()
	manager.connReady = true
	manager.connMutex.Unlock()

	// Should return connection when connReady=true
	conn, ready = manager.GetVoiceConn()
	assert.NotNil(t, conn, "Should return connection when connReady is true")
	assert.True(t, ready)
}

// TestDiscord_IsConnectionHealthy tests health check method
func TestDiscord_IsConnectionHealthy(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	healthy := manager.isConnectionHealthy()
	assert.False(t, healthy, "Should not be healthy without a conn")
}

// TestDiscord_RapidStartStop tests multiple start/stop cycles
func TestDiscord_RapidStartStop(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()

	for i := 0; i < 10; i++ {
		manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

		ctx, cancel := context.WithCancel(context.Background())

		manager.InitContext(ctx)

		time.Sleep(10 * time.Millisecond)

		cancel()
		err := manager.Stop()
		require.NoError(t, err, "Failed to stop on cycle %d", i)
	}
}

// TestDiscord_ConcurrentStatusReads tests concurrent status access
func TestDiscord_ConcurrentStatusReads(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = manager.GetStatus()
				_ = manager.IsConnected()
			}
		}()
	}

	wg.Wait()

	cancel()
	err := manager.Stop()
	require.NoError(t, err)
}

// TestDiscord_IsConnectionHealthy_WithNilConnection tests health check with nil connection
func TestDiscord_IsConnectionHealthy_WithNilConnection(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	healthy := manager.isConnectionHealthy()
	assert.False(t, healthy, "Should not be healthy with nil connection")
}

// TestDiscord_IsConnectionHealthy_WithConnReadyFalse tests health check with connReady=false
func TestDiscord_IsConnectionHealthy_WithConnReadyFalse(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = false
	manager.connMutex.Unlock()

	healthy := manager.isConnectionHealthy()
	assert.False(t, healthy, "Should not be healthy when connReady is false")
}

// TestDiscord_IsConnectionHealthy_ChecksConnReadyFlag tests that health check verifies connReady flag
func TestDiscord_IsConnectionHealthy_ChecksConnReadyFlag(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = false
	manager.connMutex.Unlock()

	healthy := manager.isConnectionHealthy()
	assert.False(t, healthy, "Should not be healthy when connReady is false")

	manager.connMutex.Lock()
	manager.connReady = true
	manager.connMutex.Unlock()

	healthy = manager.isConnectionHealthy()
	assert.True(t, healthy, "Should be healthy when connReady is true")
}

// TestDiscord_ConcurrentHealthChecks tests concurrent health check access
func TestDiscord_ConcurrentHealthChecks(t *testing.T) {
	t.Parallel()
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = false
	manager.connMutex.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = manager.isConnectionHealthy()
			}
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				manager.connMutex.Lock()
				manager.connReady = !manager.connReady
				manager.connMutex.Unlock()
			}
		}()
	}

	wg.Wait()
}

// TestDiscord_DisconnectInternalClearsConn tests that disconnect clears conn and connReady
func TestDiscord_DisconnectInternalClearsConn(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = true
	manager.connMutex.Unlock()

	manager.disconnectInternal()

	manager.connMutex.RLock()
	assert.Nil(t, manager.conn, "conn should be nil after disconnect")
	assert.False(t, manager.connReady, "connReady should be false after disconnect")
	manager.connMutex.RUnlock()
}

// TestDiscord_WaitForClientReady_Timeout tests that waitForClientReady() times out.
func TestDiscord_WaitForClientReady_Timeout(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	// Client with no gateway and no self user in cache
	client := createMockBotClient()

	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.InitContext(ctx)

	done := make(chan error, 1)
	go func() {
		done <- manager.waitForClientReady(200 * time.Millisecond)
	}()

	select {
	case err := <-done:
		assert.Error(t, err, "should return timeout error")
		assert.Contains(t, err.Error(), "timeout")
	case <-time.After(3 * time.Second):
		t.Fatal("waitForClientReady() did not time out as expected")
	}
}

// TestDiscord_SetStatusAfterStop ensures SetStatus is safe after stop
func TestDiscord_SetStatusAfterStop(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	manager.InitContext(ctx)

	err := manager.Stop()
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		manager.SetStatus(ConnectionConnected, nil)
	})
}

// TestDiscord_EventEmission tests that events are emitted properly
func TestDiscord_EventEmission(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.InitContext(ctx)

	manager.SetStatus(ConnectionConnecting, nil)
	manager.SetStatus(ConnectionConnected, nil)
	manager.SetStatus(ConnectionReconnecting, nil)

	evts := emitter.GetEventsByService("discord")
	assert.NotEmpty(t, evts, "Expected events to be emitted")

	cancel()
	err := manager.Stop()
	require.NoError(t, err)
}

// TestDiscord_MonitorConnection_ContextCancel tests that monitor exits on context cancellation
func TestDiscord_MonitorConnection_ContextCancel(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	ctx, cancel := context.WithCancel(context.Background())
	manager.InitContext(ctx)

	done := make(chan struct{})
	go func() {
		manager.monitorConnection(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	// Monitor exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("monitorConnection did not exit after context cancellation")
	}
}

// TestDiscord_MarkConnUnhealthy tests that marking conn unhealthy clears connReady
func TestDiscord_MarkConnUnhealthy(t *testing.T) {
	logger := NewMockLogger()
	emitter := NewMockBridgeEventEmitter()

	client := createMockBotClient()
	manager := NewDiscordVoiceConnectionManager(client, "test-guild", "test-channel", logger, emitter)

	mockConn := newMockVoiceConn()

	manager.connMutex.Lock()
	manager.conn = mockConn
	manager.connReady = true
	manager.connMutex.Unlock()

	assert.True(t, manager.isConnectionHealthy())

	manager.MarkConnUnhealthy()

	assert.False(t, manager.isConnectionHealthy())
}
