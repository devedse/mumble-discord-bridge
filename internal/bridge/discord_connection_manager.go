package bridge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/snowflake/v2"
	"github.com/stieneee/mumble-discord-bridge/pkg/logger"
)

// DiscordVoiceConnectionManager manages Discord voice connections using disgo.
type DiscordVoiceConnectionManager struct {
	*BaseConnectionManager
	client    *bot.Client
	conn      voice.Conn
	guildID   string
	channelID string
	connMutex sync.RWMutex
	connReady bool

	baseReconnectDelay time.Duration
}

// NewDiscordVoiceConnectionManager creates a new Discord connection manager
func NewDiscordVoiceConnectionManager(client *bot.Client, guildID, channelID string, logger logger.Logger, eventEmitter BridgeEventEmitter) *DiscordVoiceConnectionManager {
	base := NewBaseConnectionManager(logger, "discord", eventEmitter)
	return &DiscordVoiceConnectionManager{
		BaseConnectionManager: base,
		client:                client,
		guildID:               guildID,
		channelID:             channelID,
		baseReconnectDelay:    2 * time.Second,
	}
}

// Start runs the main connection loop
func (d *DiscordVoiceConnectionManager) Start(ctx context.Context) error {
	d.logger.Info("DISCORD_CONN", "Starting Discord connection manager")
	d.InitContext(ctx)
	go d.mainConnectionLoop(d.ctx)
	return nil
}

// UpdateChannel updates the target channel ID for the next connection attempt
func (d *DiscordVoiceConnectionManager) UpdateChannel(channelID string) {
	d.connMutex.Lock()
	defer d.connMutex.Unlock()
	d.channelID = channelID
	d.logger.Info("DISCORD_CONN", fmt.Sprintf("Updated target channel to: %s", channelID))
}

// mainConnectionLoop handles initial connection and monitoring.
func (d *DiscordVoiceConnectionManager) mainConnectionLoop(ctx context.Context) {
	defer d.logger.Info("DISCORD_CONN", "Main connection loop exiting")

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("DISCORD_CONN", "Context canceled, exiting connection loop")
			d.disconnectInternal()
			return
		default:
			d.logger.Debug("DISCORD_CONN", "Attempting to establish voice connection")
			if err := d.connectOnce(); err != nil {
				d.logger.Error("DISCORD_CONN", fmt.Sprintf("Connection attempt failed: %v", err))
				d.SetStatus(ConnectionFailed, err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(d.baseReconnectDelay):
					continue
				}
			}
			d.logger.Info("DISCORD_CONN", "Voice connection established, entering monitoring loop")
			d.monitorConnection(ctx)
			d.logger.Warn("DISCORD_CONN", "Connection monitoring exited, forcing full reconnect")
			d.disconnectInternal()
		}
	}
}

// monitorConnection monitors voice connection health.
func (d *DiscordVoiceConnectionManager) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	const safetyTimeout = 2 * time.Minute
	var unhealthySince time.Time
	wasHealthy := true

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			healthy := d.isConnectionHealthy()
			if healthy {
				if !wasHealthy {
					d.logger.Info("DISCORD_CONN", "Voice connection recovered")
					unhealthySince = time.Time{}
				}
				wasHealthy = true
				d.SetStatus(ConnectionConnected, nil)
			} else {
				if wasHealthy {
					d.logger.Warn("DISCORD_CONN", "Voice connection unhealthy")
					unhealthySince = time.Now()
					d.SetStatus(ConnectionReconnecting, nil)
				}
				wasHealthy = false
				if !unhealthySince.IsZero() && time.Since(unhealthySince) > safetyTimeout {
					d.logger.Error("DISCORD_CONN", fmt.Sprintf("Voice connection unhealthy for >%v, forcing full reconnect", safetyTimeout))
					return
				}
			}
		}
	}
}

// connectOnce establishes a Discord voice connection with timeout control
func (d *DiscordVoiceConnectionManager) connectOnce() error {
	d.SetStatus(ConnectionConnecting, nil)

	guildID, err := snowflake.Parse(d.guildID)
	if err != nil {
		return fmt.Errorf("invalid guild ID %s: %w", d.guildID, err)
	}

	d.connMutex.RLock()
	channelIDStr := d.channelID
	d.connMutex.RUnlock()

	channelID, err := snowflake.Parse(channelIDStr)
	if err != nil {
		return fmt.Errorf("invalid channel ID %s: %w", channelIDStr, err)
	}

	// Wait for gateway to be ready
	if err := d.waitForClientReady(10 * time.Second); err != nil {
		return fmt.Errorf("client not ready: %w", err)
	}

	// Create or get existing voice conn
	conn := d.client.VoiceManager.CreateConn(guildID)

	// Open with timeout
	connCtx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()

	d.logger.Debug("DISCORD_CONN", fmt.Sprintf("Opening voice connection to Guild=%s, Channel=%s", d.guildID, channelIDStr))
	if err := conn.Open(connCtx, channelID, false, false); err != nil {
		return fmt.Errorf("failed to open voice connection: %w", err)
	}

	d.connMutex.Lock()
	d.conn = conn
	d.connReady = true
	d.connMutex.Unlock()

	d.SetStatus(ConnectionConnected, nil)
	d.logger.Info("DISCORD_CONN", "Discord voice connection established and ready")
	return nil
}

// disconnectInternal disconnects from Discord voice
func (d *DiscordVoiceConnectionManager) disconnectInternal() {
	d.connMutex.Lock()
	conn := d.conn
	d.conn = nil
	d.connReady = false
	d.connMutex.Unlock()

	if conn != nil {
		d.logger.Debug("DISCORD_CONN", "Disconnecting from Discord voice")
		func() {
			defer func() {
				if r := recover(); r != nil {
					d.logger.Warn("DISCORD_CONN", fmt.Sprintf("Panic during disconnect: %v", r))
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn.Close(ctx)
		}()
	}
}

// waitForClientReady waits for the Discord gateway to be ready
func (d *DiscordVoiceConnectionManager) waitForClientReady(timeout time.Duration) error {
	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if d.client.Gateway != nil && d.client.Gateway.Status() == gateway.StatusReady {
			return nil
		}
		if _, ok := d.client.Caches.SelfUser(); ok {
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for client to be ready")
		}
		select {
		case <-ticker.C:
		case <-d.ctx.Done():
			return fmt.Errorf("context canceled while waiting for client")
		}
	}
}

// isConnectionHealthy checks if the voice connection is active.
func (d *DiscordVoiceConnectionManager) isConnectionHealthy() bool {
	d.connMutex.RLock()
	defer d.connMutex.RUnlock()
	return d.conn != nil && d.connReady
}

// MarkConnUnhealthy marks the connection as unhealthy (called on audio errors)
func (d *DiscordVoiceConnectionManager) MarkConnUnhealthy() {
	d.connMutex.Lock()
	d.connReady = false
	d.connMutex.Unlock()
}

// Stop gracefully stops the Discord connection manager
func (d *DiscordVoiceConnectionManager) Stop() error {
	d.logger.Info("DISCORD_CONN", "Stopping Discord connection manager")
	if err := d.BaseConnectionManager.Stop(); err != nil {
		d.logger.Error("DISCORD_CONN", fmt.Sprintf("Error stopping base connection manager: %v", err))
	}
	d.disconnectInternal()
	d.logger.Info("DISCORD_CONN", "Discord connection manager stopped")
	return nil
}

// GetVoiceConn returns the voice connection if ready
func (d *DiscordVoiceConnectionManager) GetVoiceConn() (voice.Conn, bool) {
	d.connMutex.RLock()
	defer d.connMutex.RUnlock()
	if d.conn == nil || !d.connReady {
		return nil, false
	}
	return d.conn, true
}

// isNetworkError checks if an error is a network-level error indicating disconnection
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// isTimeoutError checks if an error is a network timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
