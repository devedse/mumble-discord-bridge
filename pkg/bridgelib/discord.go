package bridgelib

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/godave/golibdave"
	"github.com/disgoorg/snowflake/v2"
	"github.com/stieneee/mumble-discord-bridge/pkg/logger"
)

// SharedDiscordClient is a shared Discord client that can be used by multiple bridge instances
type SharedDiscordClient struct {
	// The Discord bot client
	client *bot.Client

	// Logger for the Discord client
	logger logger.Logger

	// Mapping of guild:channel to message handlers
	messageHandlers     map[string][]interface{}
	messageHandlerMutex sync.RWMutex

	// Session monitoring
	ctx               context.Context
	cancel            context.CancelFunc
	monitoringEnabled bool
	monitoringMutex   sync.RWMutex
}

// NewSharedDiscordClient creates a new shared Discord client
func NewSharedDiscordClient(token string, lgr logger.Logger) (*SharedDiscordClient, error) {
	if lgr == nil {
		lgr = logger.NewConsoleLogger()
	}

	lgr.Debug("DISCORD_CLIENT", "Starting Discord client creation")

	ctx, cancel := context.WithCancel(context.Background())
	c := &SharedDiscordClient{
		logger:          lgr,
		messageHandlers: make(map[string][]interface{}),
		ctx:             ctx,
		cancel:          cancel,
	}

	lgr.Debug("DISCORD_CLIENT", "Creating Discord client")
	client, err := disgo.New(token,
		bot.WithGatewayConfigOpts(
			gateway.WithIntents(
				gateway.IntentGuilds,
				gateway.IntentGuildMessages,
				gateway.IntentGuildMessageReactions,
				gateway.IntentDirectMessages,
				gateway.IntentDirectMessageReactions,
				gateway.IntentMessageContent,
				gateway.IntentGuildVoiceStates,
			),
		),
		bot.WithVoiceManagerConfigOpts(
			voice.WithDaveSessionCreateFunc(golibdave.NewSession),
		),
		bot.WithEventListenerFunc(c.onMessageCreate),
		bot.WithEventListenerFunc(c.onGuildReady),
	)
	if err != nil {
		cancel()
		lgr.Error("DISCORD_CLIENT", fmt.Sprintf("Failed to create Discord client: %v", err))
		return nil, err
	}

	c.client = client
	lgr.Info("DISCORD_CLIENT", "SharedDiscordClient created successfully")
	return c, nil
}

// Connect connects to Discord and starts session monitoring
func (c *SharedDiscordClient) Connect() error {
	c.logger.Info("DISCORD_CLIENT", "Connecting to Discord")
	if err := c.client.OpenGateway(c.ctx); err != nil {
		c.logger.Error("DISCORD_CLIENT", fmt.Sprintf("Failed to connect to Discord: %v", err))
		return err
	}
	c.logger.Info("DISCORD_CLIENT", "Successfully connected to Discord")
	c.startSessionMonitoring()
	return nil
}

// Disconnect disconnects from Discord and stops session monitoring
func (c *SharedDiscordClient) Disconnect() error {
	c.logger.Info("DISCORD_CLIENT", "Disconnecting from Discord")
	c.stopSessionMonitoring()
	c.client.Close(context.Background())
	c.logger.Info("DISCORD_CLIENT", "Successfully disconnected from Discord")
	return nil
}

// RegisterHandler registers a handler for Discord events
func (c *SharedDiscordClient) RegisterHandler(handlerFunc interface{}) {
	if listener, ok := handlerFunc.(bot.EventListener); ok {
		c.client.AddEventListeners(listener)
	}
}

// SendMessage sends a message to a channel
func (c *SharedDiscordClient) SendMessage(channelID, content string) (*discord.Message, error) {
	cID, err := snowflake.Parse(channelID)
	if err != nil {
		return nil, fmt.Errorf("invalid channel ID %s: %w", channelID, err)
	}
	msg, err := c.client.Rest.CreateMessage(cID, discord.MessageCreate{Content: content})
	if err != nil {
		c.logger.Error("DISCORD_CLIENT", fmt.Sprintf("Failed to send message to channel %s: %v", channelID, err))
		return nil, err
	}
	return msg, nil
}

// GetClient returns the underlying Discord bot client
func (c *SharedDiscordClient) GetClient() *bot.Client {
	return c.client
}

// RegisterMessageHandler registers a message handler for a specific guild and channel
func (c *SharedDiscordClient) RegisterMessageHandler(guildID, channelID string, handlerFunc interface{}) {
	key := guildID + ":" + channelID
	c.messageHandlerMutex.Lock()
	defer c.messageHandlerMutex.Unlock()
	if _, exists := c.messageHandlers[key]; !exists {
		c.messageHandlers[key] = make([]interface{}, 0)
	}
	c.messageHandlers[key] = append(c.messageHandlers[key], handlerFunc)
	c.logger.Debug("DISCORD_CLIENT", fmt.Sprintf("Handler registered for key %s", key))
}

// UnregisterMessageHandler unregisters all message handlers for a specific guild and channel
func (c *SharedDiscordClient) UnregisterMessageHandler(guildID, channelID string, _ interface{}) {
	key := guildID + ":" + channelID
	c.messageHandlerMutex.Lock()
	defer c.messageHandlerMutex.Unlock()
	delete(c.messageHandlers, key)
	c.logger.Debug("DISCORD_CLIENT", fmt.Sprintf("All handlers cleared for key: %s", key))
}

// onMessageCreate routes message create events to the appropriate handlers
func (c *SharedDiscordClient) onMessageCreate(e *events.MessageCreate) {
	c.logger.Debug("DISCORD_HANDLER", "Message received")

	if e.Message.Author.ID == e.Client().ID() {
		c.logger.Debug("DISCORD_HANDLER", "Skipping message from self")
		return
	}

	guildID := ""
	if e.GuildID != nil {
		guildID = e.GuildID.String()
	}
	channelID := e.ChannelID.String()
	key := guildID + ":" + channelID

	c.messageHandlerMutex.RLock()
	handlers, exists := c.messageHandlers[key]
	c.messageHandlerMutex.RUnlock()

	if !exists {
		// Try guild-level prefix match
		c.messageHandlerMutex.RLock()
		defer c.messageHandlerMutex.RUnlock()
		guildPrefix := guildID + ":"
		for k, hdlrs := range c.messageHandlers {
			if strings.HasPrefix(k, guildPrefix) {
				for _, h := range hdlrs {
					if handler, ok := h.(func(*events.MessageCreate)); ok {
						handler(e)
					}
				}
			}
		}
		return
	}

	for _, h := range handlers {
		if handler, ok := h.(func(*events.MessageCreate)); ok {
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.logger.Error("DISCORD_HANDLER", fmt.Sprintf("Handler panicked: %v", r))
					}
				}()
				handler(e)
			}()
		}
	}
}

// onGuildReady routes guild ready events to the appropriate handlers
func (c *SharedDiscordClient) onGuildReady(e *events.GuildReady) {
	guildID := e.GuildID.String()
	c.logger.Info("DISCORD_HANDLER", fmt.Sprintf("Guild ready event for guild: %s", guildID))

	prefix := guildID + ":"
	c.messageHandlerMutex.RLock()
	defer c.messageHandlerMutex.RUnlock()

	for key, handlers := range c.messageHandlers {
		if strings.HasPrefix(key, prefix) {
			for _, h := range handlers {
				if handler, ok := h.(func(*events.GuildReady)); ok {
					handler(e)
				}
			}
		}
	}
}

// startSessionMonitoring starts the session health monitoring goroutine
func (c *SharedDiscordClient) startSessionMonitoring() {
	c.monitoringMutex.Lock()
	defer c.monitoringMutex.Unlock()
	if c.monitoringEnabled {
		return
	}
	c.monitoringEnabled = true
	c.logger.Info("DISCORD_CLIENT", "Starting Discord session health monitoring")
	go c.sessionMonitorLoop()
}

// stopSessionMonitoring stops the session health monitoring
func (c *SharedDiscordClient) stopSessionMonitoring() {
	c.monitoringMutex.Lock()
	defer c.monitoringMutex.Unlock()
	if !c.monitoringEnabled {
		return
	}
	c.logger.Info("DISCORD_CLIENT", "Stopping Discord session health monitoring")
	c.monitoringEnabled = false
	c.cancel()
}

// sessionMonitorLoop passively monitors session health and logs status.
func (c *SharedDiscordClient) sessionMonitorLoop() {
	c.logger.Info("DISCORD_CLIENT", "Session monitoring loop started")
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	var unhealthySince time.Time
	const lastResortThreshold = 10 * time.Minute

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("DISCORD_CLIENT", "Session monitoring loop exiting")
			return
		case <-ticker.C:
			if c.isSessionHealthy() {
				if !unhealthySince.IsZero() {
					c.logger.Info("DISCORD_CLIENT", fmt.Sprintf("Discord session recovered after %v", time.Since(unhealthySince)))
					unhealthySince = time.Time{}
				}
			} else {
				if unhealthySince.IsZero() {
					unhealthySince = time.Now()
					c.logger.Warn("DISCORD_CLIENT", "Discord session unhealthy, reconnection in progress")
				}
				unhealthyDuration := time.Since(unhealthySince)
				c.logger.Warn("DISCORD_CLIENT", fmt.Sprintf("Discord session unhealthy for %v", unhealthyDuration))
				if unhealthyDuration > lastResortThreshold {
					c.logger.Error("DISCORD_CLIENT", fmt.Sprintf("Discord session unhealthy for %v, attempting last-resort session reset", unhealthyDuration))
					c.lastResortSessionReset()
					unhealthySince = time.Now()
				}
			}
		}
	}
}

// isSessionHealthy checks if the Discord session is healthy and ready
func (c *SharedDiscordClient) isSessionHealthy() bool {
	if c.client == nil {
		return false
	}
	if c.client.Gateway == nil {
		return false
	}
	return c.client.Gateway.Status() == gateway.StatusReady
}

// lastResortSessionReset performs a hard session reset when reconnection has been failing.
func (c *SharedDiscordClient) lastResortSessionReset() {
	c.logger.Warn("DISCORD_CLIENT", "Performing last-resort session reset")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.client.Gateway.Close(ctx)

	select {
	case <-c.ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	reopenCtx, reopenCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer reopenCancel()
	if err := c.client.Gateway.Open(reopenCtx); err != nil {
		c.logger.Error("DISCORD_CLIENT", fmt.Sprintf("Last-resort session reset failed: %v", err))
	} else {
		c.logger.Info("DISCORD_CLIENT", "Last-resort session reset successful")
	}
}

// IsSessionHealthy exposes session health check for external callers
func (c *SharedDiscordClient) IsSessionHealthy() bool {
	return c.isSessionHealthy()
}
