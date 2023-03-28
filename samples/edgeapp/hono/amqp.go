package hono

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp"
)

type AMQPConnection struct {
	BaseConn *amqp.Conn
	Context  context.Context
	Sessions map[string]*amqp.Session
}

func NewAMQPConnection(uri string, port int, user, password string, context context.Context) (*AMQPConnection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", uri, port), &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain(user, password),
	})

	if err != nil {
		return nil, err
	}

	return &AMQPConnection{
		BaseConn: conn,
		Context:  context,
		Sessions: map[string]*amqp.Session{},
	}, nil
}

func (c *AMQPConnection) NewSession(sessionName string, opts *amqp.SessionOptions) (*amqp.Session, error) {
	session, err := c.BaseConn.NewSession(c.Context, opts)
	if err != nil {
		return nil, err
	}
	c.Sessions[sessionName] = session
	return session, nil
}

func (c *AMQPConnection) Close() error {
	return c.BaseConn.Close()
}

func (c *AMQPConnection) GetSession(sessionName string) (*amqp.Session, error) {
	session, ok := c.Sessions[sessionName]
	if !ok {
		return nil, fmt.Errorf("session %s not found", sessionName)
	}
	return session, nil
}

func (c *AMQPConnection) NewSender(sessionName, address string, opts *amqp.SenderOptions) (*amqp.Sender, error) {
	session, err := c.GetSession(sessionName)
	if err != nil {
		return nil, err
	}
	sender, err := session.NewSender(c.Context, address, opts)
	if err != nil {
		return nil, err
	}
	return sender, nil
}

func (c *AMQPConnection) NewReceiver(sessionName, address string, opts *amqp.ReceiverOptions) (*amqp.Receiver, error) {
	session, err := c.GetSession(sessionName)
	if err != nil {
		return nil, err
	}
	receiver, err := session.NewReceiver(c.Context, address, opts)
	if err != nil {
		return nil, err
	}
	return receiver, nil
}
