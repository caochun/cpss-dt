package ws

import (
	"encoding/json"
	"fmt"

	"nhooyr.io/websocket"
)

type Message struct {
	Type websocket.MessageType
	Data []byte
}

func (m *Message) String() string {
	if m.Type == websocket.MessageText {
		return string(m.Data)
	}
	return fmt.Sprintf("%v", m.Data)
}

func (m *Message) Unmarshal(e any) error {
	if err := json.Unmarshal(m.Data, e); err != nil {
		return err
	}
	return nil
}
