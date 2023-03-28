package ws

import (
	"context"

	"nhooyr.io/websocket"
)

type Connection struct {
	connection *websocket.Conn
	Context    context.Context
	messages   chan *Message
	sender     chan *Message
}

func NewConnection(ctx context.Context, uri string) (*Connection, error) {
	wsConn, _, err := websocket.Dial(ctx, uri, nil)
	if err != nil {
		return nil, err
	}
	conn := &Connection{
		connection: wsConn,
		Context:    ctx,
		messages:   make(chan *Message),
		sender:     make(chan *Message),
	}
	conn.startSubscription()
	conn.startSender()

	return conn, nil
}

func (c *Connection) Close() error {
	return c.connection.Close(websocket.StatusNormalClosure, "closed")
}

func (c *Connection) Receiver() <-chan *Message {
	return c.messages
}

func (c *Connection) Sender() chan<- *Message {
	return c.sender
}

func (c *Connection) startSubscription() {
	go func() {
		for {
			select {
			case <-c.Context.Done():
				return
			default:
				mt, data, err := c.connection.Read(c.Context)
				if err != nil {
					println("error reading message: ", err.Error())
				}
				c.messages <- &Message{
					Type: mt,
					Data: data,
				}
			}
		}
	}()
}

func (c *Connection) startSender() {
	go func() {
		for {
			select {
			case <-c.Context.Done():
				return
			case msg := <-c.sender:
				err := c.connection.Write(c.Context, msg.Type, msg.Data)
				if err != nil {
					println("error writing message: ", err.Error())
				}
			}
		}
	}()
}
