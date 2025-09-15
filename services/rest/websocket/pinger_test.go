package websocket

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/goto/raccoon/services/rest/websocket/connection"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPinger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	connCh := make(chan connection.Conn, 1)

	upgrader := connection.NewUpgrader(connection.UpgraderConfig{
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		CheckOrigin:       true,
		MaxUser:           10,
		PongWaitInterval:  1 * time.Second,
		WriteWaitInterval: 1 * time.Second,
		ConnIDHeader:      "X-Conn-ID",
		ConnGroupHeader:   "X-Conn-Group",
		ConnGroupDefault:  "default",
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("failed to upgrade: %v", err)
			return
		}
		connCh <- conn
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"X-Conn-ID":    {"1"},
		"X-Conn-Group": {"test"},
	})
	if err != nil {
		t.Fatalf("failed to dial test server: %v", err)
	}

	// ping detection
	pingReceived := make(chan struct{}, 1)
	client.SetPingHandler(func(appData string) error {
		t.Logf("client received ping: %s", appData)
		select { // avoid blocking if already signaled
		case pingReceived <- struct{}{}:
		default:
		}
		return client.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})

	// close detection
	closed := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			_, _, err := client.ReadMessage()
			if err != nil {
				once.Do(func() { close(closed) })
				return
			}
		}
	}()

	// start pinger
	go Pinger(ctx, connCh, 1, 20*time.Millisecond, 5*time.Millisecond)

	// assert ping received
	select {
	case <-pingReceived:
		t.Log("client received ping")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client did not receive ping")
	}

	// now cancel -> triggers connection close
	cancel()

	select {
	case <-closed:
		t.Log("connection closed after cancel()")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection not closed after cancel()")
	}

	// close client at the very end
	client.Close()
}
