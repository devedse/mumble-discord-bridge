package bridge

import (
	"fmt"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/snowflake/v2"
	"github.com/stieneee/gumble/gumble"
)

// DiscordListener holds references to the current BridgeConf
// and BridgeState for use by the event handlers
type DiscordListener struct {
	Bridge *BridgeState
}

// OnGuildReady handles Discord guild ready events.
func (l *DiscordListener) OnGuildReady(e *events.GuildReady) {
	if e.GuildID.String() != l.Bridge.BridgeConfig.GID {
		return
	}

	for _, vs := range e.Guild.VoiceStates {
		if vs.ChannelID == nil || vs.ChannelID.String() != l.Bridge.DiscordChannelID {
			continue
		}
		if e.Client().ID() == vs.UserID {
			// Ignore bot
			continue
		}

		u, err := e.Client().Rest.GetUser(vs.UserID)
		if err != nil {
			l.Bridge.Logger.Error("DISCORD_HANDLER", "Error looking up username")
			continue
		}

		var dmID snowflake.ID
		dmCh, err := e.Client().Rest.CreateDMChannel(u.ID)
		if err != nil {
			l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Error creating private channel for %s", u.Username))
		} else {
			dmID = dmCh.ID()
		}

		l.Bridge.DiscordUsersMutex.Lock()
		l.Bridge.DiscordUsers[vs.UserID.String()] = DiscordUser{
			username:    u.Username,
			seen:        true,
			dmChannelID: dmID,
		}
		l.Bridge.DiscordUsersMutex.Unlock()

		// If connected to mumble inform users of Discord users
		l.Bridge.BridgeMutex.Lock()
		connected := l.Bridge.Connected
		disableText := l.Bridge.BridgeConfig.MumbleDisableText
		// Get current Mumble client from connection manager
		var mumbleClient *gumble.Client
		if l.Bridge.MumbleConnectionManager != nil {
			mumbleClient = l.Bridge.MumbleConnectionManager.GetClient()
		}
		l.Bridge.BridgeMutex.Unlock()

		if connected && !disableText && mumbleClient != nil {
			mumbleClient.Do(func() {
				if mumbleClient.Self != nil && mumbleClient.Self.Channel != nil {
					mumbleClient.Self.Channel.Send(fmt.Sprintf("%v has joined Discord\n", u.Username), false)
				}
			})
		}

		// Notify external systems about the user count change
		l.Bridge.notifyMetricsChange()
	}
}

// MessageCreate handles Discord message creation events.
func (l *DiscordListener) MessageCreate(e *events.MessageCreate) {
	l.Bridge.Logger.Debug("DISCORD_HANDLER", fmt.Sprintf("MessageCreate called from Discord user: %s", e.Message.Author.Username))

	// Ignore all messages created by the bot itself
	if e.Message.Author.ID == e.Client().ID() {
		l.Bridge.Logger.Debug("DISCORD_HANDLER", "Ignoring message from self")
		return
	}

	// Get the guild ID from the event
	var guildID string
	if e.GuildID != nil {
		guildID = e.GuildID.String()
	}

	channelID := e.ChannelID.String()

	// If it's a DM (no guild), check guild ID matches config
	if guildID == "" {
		l.Bridge.Logger.Debug("DISCORD_HANDLER", "Message from DM, no guild ID")
		return
	}

	l.Bridge.Logger.Debug("DISCORD_HANDLER", fmt.Sprintf("Message from guild %s, expected guild: %s", guildID, l.Bridge.BridgeConfig.GID))

	if guildID != l.Bridge.BridgeConfig.GID {
		l.Bridge.Logger.Debug("DISCORD_HANDLER", "Guild ID mismatch, ignoring message")
		return
	}

	prefix := "!" + l.Bridge.BridgeConfig.Command

	l.Bridge.BridgeMutex.Lock()
	bridgeConnected := l.Bridge.Connected
	l.Bridge.BridgeMutex.Unlock()

	// If the message starts with "!" then send it to HandleCommand else process as chat
	if strings.HasPrefix(e.Message.Content, "!") {
		// check if discord command is enabled
		if !l.Bridge.BridgeConfig.DiscordCommand {
			return
		}

		// Only process commands from the configured channel
		if channelID != l.Bridge.BridgeConfig.CID {
			l.Bridge.Logger.Debug("DISCORD_HANDLER",
				fmt.Sprintf("Ignoring command from channel %s (bound to %s)",
					channelID, l.Bridge.BridgeConfig.CID))
			return
		}

		// process the shared command options
		l.Bridge.HandleCommand(e.Message.Content, func(s string) {
			if l.Bridge.DiscordClient == nil || l.Bridge.DiscordClient.Rest == nil {
				return
			}
			chID, err := snowflake.Parse(channelID)
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Invalid channel ID: %v", err))
				return
			}
			_, err = l.Bridge.DiscordClient.Rest.CreateMessage(chID, discord.MessageCreate{Content: s})
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Error sending command response: %v", err))
			}
		})

		// process the Discord specific command options
		chID, parseErr := snowflake.Parse(channelID)

		sendReply := func(msg string) {
			if parseErr != nil {
				return
			}
			if l.Bridge.DiscordClient == nil || l.Bridge.DiscordClient.Rest == nil {
				return
			}
			_, err := l.Bridge.DiscordClient.Rest.CreateMessage(chID, discord.MessageCreate{Content: msg})
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Error sending reply: %v", err))
			}
		}

		if strings.HasPrefix(e.Message.Content, prefix+" link") {
			if l.Bridge.Mode == BridgeModeConstant && strings.HasPrefix(e.Message.Content, prefix) {
				sendReply("Constant mode enabled, link commands can not be entered")
				return
			}
			if bridgeConnected {
				sendReply("Bridge already running, unlink first")
				return
			}

			// Get guild voice states from cache
			guildSnowflakeID, err := snowflake.Parse(guildID)
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Invalid guild ID: %v", err))
				return
			}

			foundUser := false
			for vs := range e.Client().Caches.VoiceStates(guildSnowflakeID) {
				if vs.UserID == e.Message.Author.ID {
					foundUser = true
					if vs.ChannelID != nil {
						if vs.ChannelID.String() != l.Bridge.BridgeConfig.CID {
							sendReply("Please join the configured voice channel to use this bridge")
							return
						}
						l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("Trying to join GID %v and VID %v", guildID, vs.ChannelID.String()))
						go l.Bridge.StartBridge()
						return
					}
				}
			}

			if !foundUser {
				sendReply("Couldn't find you in a voice channel. Please join a voice channel first.")
			} else {
				sendReply("Please join a voice channel first.")
			}
		}

		if strings.HasPrefix(e.Message.Content, prefix+" unlink") {
			if l.Bridge.Mode == BridgeModeConstant && strings.HasPrefix(e.Message.Content, prefix) {
				sendReply("Constant mode enabled, link commands can not be entered")
				return
			}
			if !bridgeConnected {
				sendReply("Bridge is not currently running")
				return
			}

			guildSnowflakeID, err := snowflake.Parse(guildID)
			if err != nil {
				l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("Invalid guild ID %v, allowing unlink anyway", guildID))
				l.Bridge.BridgeDie <- true
				return
			}

			for vs := range e.Client().Caches.VoiceStates(guildSnowflakeID) {
				if vs.UserID == e.Message.Author.ID && vs.ChannelID != nil && vs.ChannelID.String() == l.Bridge.DiscordChannelID {
					l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("Trying to leave GID %v and VID %v", guildID, vs.ChannelID.String()))
					l.Bridge.BridgeDie <- true
					return
				}
			}
		}

		if strings.HasPrefix(e.Message.Content, prefix+" refresh") {
			if l.Bridge.Mode == BridgeModeConstant && strings.HasPrefix(e.Message.Content, prefix) {
				sendReply("Constant mode enabled, link commands can not be entered")
				return
			}
			if !bridgeConnected {
				sendReply("Bridge is not currently running")
				return
			}

			guildSnowflakeID, err := snowflake.Parse(guildID)
			if err != nil {
				l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("Invalid guild ID %v, allowing refresh anyway", guildID))
				l.Bridge.BridgeDie <- true
				time.Sleep(5 * time.Second)
				go l.Bridge.StartBridge()
				return
			}

			for vs := range e.Client().Caches.VoiceStates(guildSnowflakeID) {
				if vs.UserID == e.Message.Author.ID {
					channelIDStr := ""
					if vs.ChannelID != nil {
						channelIDStr = vs.ChannelID.String()
					}
					l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("Trying to refresh GID %v and VID %v", guildID, channelIDStr))
					l.Bridge.BridgeDie <- true
					time.Sleep(5 * time.Second)
					go l.Bridge.StartBridge()
					return
				}
			}
		}
	} else if !strings.HasPrefix(e.Message.Content, "!") {
		// Get a truncated version of the message for logs
		content := e.Message.Content
		if len(content) > 50 {
			content = content[:47] + "..."
		}

		// Check if chat bridge is enabled
		if !l.Bridge.BridgeConfig.ChatBridge {
			l.Bridge.Logger.Debug("DISCORD→MUMBLE", fmt.Sprintf("Chat message received but ChatBridge is DISABLED: %s", content))
			return
		}

		// Check if the bridge is connected
		l.Bridge.BridgeMutex.Lock()
		bridgeConnected := l.Bridge.Connected
		l.Bridge.BridgeMutex.Unlock()

		if !bridgeConnected {
			l.Bridge.Logger.Debug("DISCORD→MUMBLE", "Chat message received but bridge is not connected")
			return
		}

		// Check if text messages to Mumble are disabled
		if l.Bridge.BridgeConfig.MumbleDisableText {
			l.Bridge.Logger.Debug("DISCORD→MUMBLE", "Chat message received but MumbleDisableText is true")
			return
		}

		l.Bridge.Logger.Debug("DISCORD→MUMBLE", fmt.Sprintf("Forwarding message from %s", e.Message.Author.Username))

		// Get MumbleClient reference under lock to prevent race conditions
		l.Bridge.BridgeMutex.Lock()
		var mumbleClient *gumble.Client
		if l.Bridge.MumbleConnectionManager != nil {
			mumbleClient = l.Bridge.MumbleConnectionManager.GetClient()
		}
		l.Bridge.BridgeMutex.Unlock()

		// Perform null checks
		if mumbleClient == nil ||
			mumbleClient.Self == nil ||
			mumbleClient.Self.Channel == nil {
			l.Bridge.Logger.Error("DISCORD→MUMBLE", "Cannot forward message - MumbleClient is not properly initialized")
			return
		}

		// Format and send the message to Mumble
		message := fmt.Sprintf("%v: %v\n", e.Message.Author.Username, e.Message.Content)

		// Use a separate goroutine with timeout to make the call more resilient
		messageSent := make(chan bool, 1)
		go func() {
			mumbleClient.Do(func() {
				mumbleClient.Self.Channel.Send(message, false)
				messageSent <- true
			})
		}()

		// Wait for confirmation or timeout
		select {
		case <-messageSent:
			l.Bridge.Logger.Debug("DISCORD→MUMBLE", "Successfully forwarded message")
		case <-time.After(2 * time.Second):
			l.Bridge.Logger.Error("DISCORD→MUMBLE", "Timed out while trying to send message")
		}
	}
}

// VoiceUpdate handles Discord voice state update events.
// Lock order: BridgeMutex -> MumbleUsersMutex -> DiscordUsersMutex
func (l *DiscordListener) VoiceUpdate(e *events.GuildVoiceStateUpdate) {
	if e.VoiceState.GuildID.String() != l.Bridge.BridgeConfig.GID {
		return
	}

	guildID := e.VoiceState.GuildID

	// Collect voice states from cache
	var voiceStates []discord.VoiceState
	for vs := range e.Client().Caches.VoiceStates(guildID) {
		voiceStates = append(voiceStates, vs)
	}

	// Collect users to add and users to remove under DiscordUsersMutex
	type userToAdd struct {
		userID      string
		username    string
		dmChannelID snowflake.ID
	}
	type userToRemove struct {
		userID   string
		username string
	}
	var usersToAdd []userToAdd
	var usersToRemove []userToRemove
	var userCount int

	botID := e.Client().ID()

	l.Bridge.DiscordUsersMutex.Lock()

	// Mark all users as unseen
	for u := range l.Bridge.DiscordUsers {
		du := l.Bridge.DiscordUsers[u]
		du.seen = false
		l.Bridge.DiscordUsers[u] = du
	}

	// Sync the channel voice states to the local discordUsersMap
	for _, vs := range voiceStates {
		if vs.ChannelID == nil || vs.ChannelID.String() != l.Bridge.DiscordChannelID {
			continue
		}
		if botID == vs.UserID {
			// Ignore bot
			continue
		}

		userIDStr := vs.UserID.String()
		if _, ok := l.Bridge.DiscordUsers[userIDStr]; !ok {
			u, err := e.Client().Rest.GetUser(vs.UserID)
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", "Error looking up username")
				continue
			}

			l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("User joined Discord: %s", u.Username))

			// Emit user joined event
			l.Bridge.EmitUserEvent("discord", 0, u.Username, nil)
			var dmID snowflake.ID
			dmCh, err := e.Client().Rest.CreateDMChannel(u.ID)
			if err != nil {
				l.Bridge.Logger.Error("DISCORD_HANDLER", fmt.Sprintf("Error creating private channel for %s", u.Username))
			} else {
				dmID = dmCh.ID()
			}
			l.Bridge.DiscordUsers[userIDStr] = DiscordUser{
				username:    u.Username,
				seen:        true,
				dmChannelID: dmID,
			}
			usersToAdd = append(usersToAdd, userToAdd{
				userID:      userIDStr,
				username:    u.Username,
				dmChannelID: dmID,
			})
		} else {
			du := l.Bridge.DiscordUsers[userIDStr]
			du.seen = true
			l.Bridge.DiscordUsers[userIDStr] = du
		}
	}

	// Identify users that are no longer connected
	for id := range l.Bridge.DiscordUsers {
		if !l.Bridge.DiscordUsers[id].seen {
			username := l.Bridge.DiscordUsers[id].username
			l.Bridge.Logger.Info("DISCORD_HANDLER", fmt.Sprintf("User left Discord channel: %s", username))

			// Emit user left event
			l.Bridge.EmitUserEvent("discord", 1, username, nil)
			usersToRemove = append(usersToRemove, userToRemove{
				userID:   id,
				username: username,
			})
		}
	}

	// Remove users from the map while still holding the lock
	for _, u := range usersToRemove {
		delete(l.Bridge.DiscordUsers, u.userID)
	}

	userCount = len(l.Bridge.DiscordUsers)
	l.Bridge.DiscordUsersMutex.Unlock()

	// Now acquire BridgeMutex to read bridge state and send Mumble messages
	l.Bridge.BridgeMutex.Lock()
	connected := l.Bridge.Connected
	disableText := l.Bridge.BridgeConfig.MumbleDisableText
	var mumbleClient *gumble.Client
	if l.Bridge.MumbleConnectionManager != nil {
		mumbleClient = l.Bridge.MumbleConnectionManager.GetClient()
	}
	l.Bridge.BridgeMutex.Unlock()

	// Send Mumble messages for users that joined
	if connected && !disableText && mumbleClient != nil {
		for _, u := range usersToAdd {
			username := u.username
			mumbleClient.Do(func() {
				if mumbleClient.Self != nil && mumbleClient.Self.Channel != nil {
					mumbleClient.Self.Channel.Send(fmt.Sprintf("%v has joined Discord\n", username), false)
				}
			})
		}

		// Send Mumble messages for users that left
		for _, u := range usersToRemove {
			username := u.username
			mumbleClient.Do(func() {
				if mumbleClient.Self != nil && mumbleClient.Self.Channel != nil {
					mumbleClient.Self.Channel.Send(fmt.Sprintf("%v has left Discord channel\n", username), false)
				}
			})
		}
	}

	// Update metrics
	promDiscordUsers.Set(float64(userCount))
}
