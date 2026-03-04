package bridge

import (
	"context"
	"net"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	botgateway "github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/snowflake/v2"
)

// createMockBotClient creates a minimal *bot.Client for testing.
// It has empty caches and no gateway/rest/voice.
func createMockBotClient() *bot.Client {
	return &bot.Client{
		Caches: cache.New(),
	}
}

// mockUDPConn is a mock UDPConn for testing that reads from a channel
type mockUDPConn struct {
	packetChan   chan *voice.Packet
	writtenBytes []byte
	readDeadline time.Time
	closed       bool
}

func newMockUDPConn() *mockUDPConn {
	return &mockUDPConn{
		packetChan: make(chan *voice.Packet, 20),
	}
}

func (m *mockUDPConn) ReadPacket() (*voice.Packet, error) {
	if !m.readDeadline.IsZero() {
		timeout := time.Until(m.readDeadline)
		if timeout <= 0 {
			return nil, &net.OpError{Op: "read", Err: &timeoutErr{}}
		}
		select {
		case p, ok := <-m.packetChan:
			if !ok {
				return nil, net.ErrClosed
			}
			return p, nil
		case <-time.After(timeout):
			return nil, &net.OpError{Op: "read", Err: &timeoutErr{}}
		}
	}
	p, ok := <-m.packetChan
	if !ok {
		return nil, net.ErrClosed
	}
	return p, nil
}

func (m *mockUDPConn) Write(p []byte) (int, error) {
	m.writtenBytes = append(m.writtenBytes, p...)
	return len(p), nil
}

func (m *mockUDPConn) SetReadDeadline(t time.Time) error {
	m.readDeadline = t
	return nil
}

func (m *mockUDPConn) SetWriteDeadline(_ time.Time) error { return nil }
func (m *mockUDPConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockUDPConn) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (m *mockUDPConn) RemoteAddr() net.Addr               { return &net.UDPAddr{} }
func (m *mockUDPConn) Read(p []byte) (int, error)         { return 0, nil }
func (m *mockUDPConn) Close() error {
	m.closed = true
	close(m.packetChan)
	return nil
}
func (m *mockUDPConn) Open(_ context.Context, _ string, _ int, _ uint32) (string, int, error) {
	return "", 0, nil
}
func (m *mockUDPConn) SetSecretKey(_ voice.EncryptionMode, _ []byte) error { return nil }

// timeoutErr implements net.Error for timeout simulation
type timeoutErr struct{}

func (e *timeoutErr) Error() string   { return "i/o timeout" }
func (e *timeoutErr) Timeout() bool   { return true }
func (e *timeoutErr) Temporary() bool { return true }

// mockVoiceConn is a mock voice.Conn for testing
type mockVoiceConn struct {
	udp *mockUDPConn
}

func newMockVoiceConn() *mockVoiceConn {
	return &mockVoiceConn{udp: newMockUDPConn()}
}

func (m *mockVoiceConn) UDP() voice.UDPConn                                               { return m.udp }
func (m *mockVoiceConn) Gateway() voice.Gateway                                           { return nil }
func (m *mockVoiceConn) ChannelID() *snowflake.ID                                         { return nil }
func (m *mockVoiceConn) GuildID() snowflake.ID                                            { return 0 }
func (m *mockVoiceConn) UserIDBySSRC(_ uint32) snowflake.ID                               { return 0 }
func (m *mockVoiceConn) SetSpeaking(_ context.Context, _ voice.SpeakingFlags) error       { return nil }
func (m *mockVoiceConn) SetOpusFrameProvider(_ voice.OpusFrameProvider)                   {}
func (m *mockVoiceConn) SetOpusFrameReceiver(_ voice.OpusFrameReceiver)                   {}
func (m *mockVoiceConn) SetEventHandlerFunc(_ voice.EventHandlerFunc)                     {}
func (m *mockVoiceConn) Open(_ context.Context, _ snowflake.ID, _, _ bool) error          { return nil }
func (m *mockVoiceConn) Close(_ context.Context)                                          {}
func (m *mockVoiceConn) HandleVoiceStateUpdate(update botgateway.EventVoiceStateUpdate)   {}
func (m *mockVoiceConn) HandleVoiceServerUpdate(update botgateway.EventVoiceServerUpdate) {}

// createTestDiscordVoiceConnectionManager creates a DiscordVoiceConnectionManager
// with an injected mock voice connection for testing.
func createTestDiscordVoiceConnectionManager(mockConn *mockVoiceConn) *DiscordVoiceConnectionManager {
	base := NewBaseConnectionManager(NewMockLogger(), "discord-test", NewMockBridgeEventEmitter())
	base.SetStatus(ConnectionConnected, nil)

	client := createMockBotClient()

	mgr := &DiscordVoiceConnectionManager{
		BaseConnectionManager: base,
		client:                client,
		guildID:               "test-guild",
		channelID:             "test-channel",
	}

	if mockConn != nil {
		mgr.conn = mockConn
		mgr.connReady = true
	}

	return mgr
}

// makeVoicePacket creates a voice.Packet with the given parameters.
func makeVoicePacket(ssrc uint32, seq uint16, ts uint32, opus []byte) *voice.Packet {
	return &voice.Packet{
		SSRC:      ssrc,
		Sequence:  seq,
		Timestamp: ts,
		Opus:      opus,
	}
}
