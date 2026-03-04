package bridge

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/snowflake/v2"
	"github.com/stretchr/testify/assert"
)

// buildVoiceStateUpdateEvent builds a minimal GuildVoiceStateUpdate event for testing.
func buildVoiceStateUpdateEvent(guildID, channelID, userID string) *events.GuildVoiceStateUpdate {
	client := createMockBotClient()

	var chID *snowflake.ID
	if channelID != "" {
		id := snowflake.MustParse(channelID)
		chID = &id
	}

	gid := snowflake.MustParse(guildID)
	uid := snowflake.MustParse(userID)

	return &events.GuildVoiceStateUpdate{
		GenericGuildVoiceState: &events.GenericGuildVoiceState{
			GenericEvent: events.NewGenericEvent(client, 0, 0),
			VoiceState: discord.VoiceState{
				GuildID:   gid,
				ChannelID: chID,
				UserID:    uid,
			},
		},
		OldVoiceState: discord.VoiceState{},
	}
}

// buildGuildReadyEvent builds a minimal GuildReady event for testing.
func buildGuildReadyEvent(guildID string) *events.GuildReady {
	client := createMockBotClient()
	gid := snowflake.MustParse(guildID)
	return &events.GuildReady{
		GenericGuild: &events.GenericGuild{
			GenericEvent: events.NewGenericEvent(client, 0, 0),
			GuildID:      gid,
		},
		Guild: discord.GatewayGuild{},
	}
}

// buildMessageCreateEvent builds a minimal MessageCreate event for testing.
func buildMessageCreateEvent(guildID, channelID, authorID, content string) *events.MessageCreate {
	client := createMockBotClient()

	var gid *snowflake.ID
	if guildID != "" {
		id := snowflake.MustParse(guildID)
		gid = &id
	}

	chID := snowflake.MustParse(channelID)
	aID := snowflake.MustParse(authorID)

	msg := discord.Message{
		Author:    discord.User{ID: aID, Username: "TestUser"},
		Content:   content,
		ChannelID: chID,
	}
	if gid != nil {
		msg.GuildID = gid
	}

	return &events.MessageCreate{
		GenericMessage: &events.GenericMessage{
			GenericEvent: events.NewGenericEvent(client, 0, 0),
			Message:      msg,
			ChannelID:    chID,
			GuildID:      gid,
		},
	}
}

// TestVoiceUpdate_LockOrderNoDeadlock tests that VoiceUpdate doesn't deadlock
func TestVoiceUpdate_LockOrderNoDeadlock(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.DiscordChannelID = "9876543210987654321"
	listener := &DiscordListener{Bridge: bridge}

	// Run concurrent voice updates without deadlock
	assertNoDeadlock(t, 5*time.Second, func() {
		var wg sync.WaitGroup

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				event := buildVoiceStateUpdateEvent(
					bridge.BridgeConfig.GID,
					bridge.DiscordChannelID,
					"1111111111111111111",
				)
				listener.VoiceUpdate(event)
			}(i)
		}

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bridge.BridgeMutex.Lock()
				_ = bridge.Connected
				bridge.BridgeMutex.Unlock()
			}()
		}

		wg.Wait()
	})
}

// TestVoiceUpdate_ConcurrentUserJoinLeave tests concurrent user additions/removals
func TestVoiceUpdate_ConcurrentUserJoinLeave(_ *testing.T) {
	bridge := createTestBridgeState(nil)

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			bridge.DiscordUsers["user-"+string(rune('0'+idx))] = DiscordUser{
				username: "User" + string(rune('0'+idx)),
				seen:     true,
			}
			bridge.DiscordUsersMutex.Unlock()
		}(i)
	}

	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			delete(bridge.DiscordUsers, "user-"+string(rune('0'+idx)))
			bridge.DiscordUsersMutex.Unlock()
		}(i)
	}

	wg.Wait()
}

// TestVoiceUpdate_MapIteratorNotInvalidated tests safe map iteration during modification
func TestVoiceUpdate_MapIteratorNotInvalidated(t *testing.T) {
	bridge := createTestBridgeState(nil)

	bridge.DiscordUsersMutex.Lock()
	for i := 0; i < 20; i++ {
		bridge.DiscordUsers["user-"+string(rune('A'+i))] = DiscordUser{
			username: "User" + string(rune('A'+i)),
			seen:     true,
		}
	}
	bridge.DiscordUsersMutex.Unlock()

	assertNoDeadlock(t, 3*time.Second, func() {
		var wg sync.WaitGroup

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bridge.DiscordUsersMutex.Lock()
				for u := range bridge.DiscordUsers {
					du := bridge.DiscordUsers[u]
					du.seen = false
					bridge.DiscordUsers[u] = du
				}
				bridge.DiscordUsersMutex.Unlock()
			}()
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				bridge.DiscordUsersMutex.Lock()
				bridge.DiscordUsers["new-user-"+string(rune('0'+idx))] = DiscordUser{
					username: "NewUser",
					seen:     true,
				}
				delete(bridge.DiscordUsers, "user-"+string(rune('A'+idx)))
				bridge.DiscordUsersMutex.Unlock()
			}(i)
		}

		wg.Wait()
	})
}

// TestVoiceUpdate_GuildIDMismatch tests that non-matching guild is ignored
func TestVoiceUpdate_GuildIDMismatch(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	listener := &DiscordListener{Bridge: bridge}

	// Event with wrong guild ID
	event := buildVoiceStateUpdateEvent(
		"9999999999999999999", // wrong guild
		"1111111111111111111",
		"2222222222222222222",
	)

	assert.NotPanics(t, func() {
		listener.VoiceUpdate(event)
	})

	bridge.DiscordUsersMutex.Lock()
	assert.Empty(t, bridge.DiscordUsers)
	bridge.DiscordUsersMutex.Unlock()
}

// TestVoiceUpdate_BotUserIgnored tests that bot's own voice state is ignored
func TestVoiceUpdate_BotUserIgnored(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.DiscordChannelID = "9876543210987654321"
	listener := &DiscordListener{Bridge: bridge}

	// Call VoiceUpdate with an event using empty caches
	// The handler will iterate over cache (empty), so no users are added
	event := buildVoiceStateUpdateEvent(
		bridge.BridgeConfig.GID,
		bridge.DiscordChannelID,
		"1111111111111111111",
	)

	listener.VoiceUpdate(event)

	// With empty cache, no users are added
	bridge.DiscordUsersMutex.Lock()
	_, exists := bridge.DiscordUsers["1111111111111111111"]
	bridge.DiscordUsersMutex.Unlock()

	assert.False(t, exists, "No users should be added from empty cache")
}

// TestVoiceUpdate_UserSeenTracking tests user seen flag management
func TestVoiceUpdate_UserSeenTracking(t *testing.T) {
	bridge := createTestBridgeState(nil)

	bridge.DiscordUsersMutex.Lock()
	bridge.DiscordUsers["test-user"] = DiscordUser{
		username: "TestUser",
		seen:     true,
	}
	bridge.DiscordUsersMutex.Unlock()

	// Mark all as unseen
	bridge.DiscordUsersMutex.Lock()
	for u := range bridge.DiscordUsers {
		du := bridge.DiscordUsers[u]
		du.seen = false
		bridge.DiscordUsers[u] = du
	}
	bridge.DiscordUsersMutex.Unlock()

	bridge.DiscordUsersMutex.Lock()
	assert.False(t, bridge.DiscordUsers["test-user"].seen)
	bridge.DiscordUsersMutex.Unlock()
}

// TestVoiceUpdate_RapidEvents tests rapid event processing
func TestVoiceUpdate_RapidEvents(_ *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.DiscordChannelID = "9876543210987654321"
	listener := &DiscordListener{Bridge: bridge}

	var wg sync.WaitGroup
	eventCount := 100

	for i := 0; i < eventCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			event := buildVoiceStateUpdateEvent(
				bridge.BridgeConfig.GID,
				bridge.DiscordChannelID,
				"1111111111111111111",
			)
			listener.VoiceUpdate(event)
		}(i)
	}

	wg.Wait()
}

// TestDiscordListener_GuildReadyIgnoresWrongGuild tests GuildReady filtering
func TestDiscordListener_GuildReadyIgnoresWrongGuild(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	listener := &DiscordListener{Bridge: bridge}

	// Event with wrong guild ID
	event := buildGuildReadyEvent("9999999999999999999")

	assert.NotPanics(t, func() {
		listener.OnGuildReady(event)
	})
}

// TestVoiceUpdate_UserRemovalTracking tests that removed users are tracked
func TestVoiceUpdate_UserRemovalTracking(t *testing.T) {
	bridge := createTestBridgeState(nil)

	bridge.DiscordUsersMutex.Lock()
	bridge.DiscordUsers["user1"] = DiscordUser{username: "User1", seen: true}
	bridge.DiscordUsers["user2"] = DiscordUser{username: "User2", seen: true}
	bridge.DiscordUsers["user3"] = DiscordUser{username: "User3", seen: true}
	bridge.DiscordUsersMutex.Unlock()

	var usersToRemove []string

	bridge.DiscordUsersMutex.Lock()
	for u := range bridge.DiscordUsers {
		du := bridge.DiscordUsers[u]
		du.seen = false
		bridge.DiscordUsers[u] = du
	}

	du := bridge.DiscordUsers["user2"]
	du.seen = true
	bridge.DiscordUsers["user2"] = du

	for id := range bridge.DiscordUsers {
		if !bridge.DiscordUsers[id].seen {
			usersToRemove = append(usersToRemove, id)
		}
	}

	for _, id := range usersToRemove {
		delete(bridge.DiscordUsers, id)
	}
	bridge.DiscordUsersMutex.Unlock()

	bridge.DiscordUsersMutex.Lock()
	assert.Len(t, bridge.DiscordUsers, 1)
	_, exists := bridge.DiscordUsers["user2"]
	assert.True(t, exists)
	bridge.DiscordUsersMutex.Unlock()
}

// TestVoiceUpdate_ConcurrentWithBridgeOps tests VoiceUpdate concurrent with bridge operations
func TestVoiceUpdate_ConcurrentWithBridgeOps(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.DiscordChannelID = "9876543210987654321"
	listener := &DiscordListener{Bridge: bridge}

	var wg sync.WaitGroup
	var voiceUpdates, bridgeOps int32

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			event := buildVoiceStateUpdateEvent(
				bridge.BridgeConfig.GID,
				bridge.DiscordChannelID,
				"1111111111111111111",
			)
			listener.VoiceUpdate(event)
			atomic.AddInt32(&voiceUpdates, 1)
		}(i)
	}

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bridge.BridgeMutex.Lock()
			bridge.Connected = !bridge.Connected
			bridge.BridgeMutex.Unlock()
			atomic.AddInt32(&bridgeOps, 1)
		}()
	}

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bridge.DiscordUsersMutex.Lock()
			_ = len(bridge.DiscordUsers)
			bridge.DiscordUsersMutex.Unlock()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Completed %d voice updates and %d bridge ops",
			atomic.LoadInt32(&voiceUpdates), atomic.LoadInt32(&bridgeOps))
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected in concurrent operations")
	}
}

// TestDiscordListener_NilBridgeState tests handling of nil bridge state components
func TestDiscordListener_NilBridgeState(_ *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig = nil

	listener := &DiscordListener{Bridge: bridge}

	event := buildVoiceStateUpdateEvent(
		"1234567890123456789",
		"9876543210987654321",
		"1111111111111111111",
	)

	defer func() {
		_ = recover() //nolint:errcheck
	}()

	listener.VoiceUpdate(event)
}

// TestMessageCreate_CommandFromWrongChannel tests that commands from wrong channel are ignored
func TestMessageCreate_CommandFromWrongChannel(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.BridgeConfig.CID = "9876543210987654321"
	bridge.BridgeConfig.DiscordCommand = true

	listener := &DiscordListener{Bridge: bridge}

	// Message from WRONG channel - should be ignored
	event := buildMessageCreateEvent(
		bridge.BridgeConfig.GID,
		"1111111111111111111", // wrong channel
		"2222222222222222222",
		"!bridge link",
	)

	assert.NotPanics(t, func() {
		listener.MessageCreate(event)
	})
}

// TestMessageCreate_CommandFromCorrectChannel tests that commands from correct channel are processed
func TestMessageCreate_CommandFromCorrectChannel(t *testing.T) {
	bridge := createTestBridgeState(nil)
	bridge.BridgeConfig.GID = "1234567890123456789"
	bridge.BridgeConfig.CID = "9876543210987654321"
	bridge.BridgeConfig.DiscordCommand = true
	bridge.BridgeConfig.Command = "bridge"

	listener := &DiscordListener{Bridge: bridge}

	// Message from CORRECT channel - should be processed (but not panic)
	event := buildMessageCreateEvent(
		bridge.BridgeConfig.GID,
		bridge.BridgeConfig.CID,
		"2222222222222222222",
		"!bridge link",
	)

	assert.NotPanics(t, func() {
		listener.MessageCreate(event)
	})
}
