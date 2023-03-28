package hono

import (
	"context"

	"github.com/Azure/go-amqp"
)

type Connection struct {
	connection *AMQPConnection
	receiver   *amqp.Receiver
	sender     *amqp.Sender
	device     *Device

	command   chan *Command
	telemetry chan *amqp.Message
	event     chan *amqp.Message
	response  chan *amqp.Message
}

func NewConnection(uri string, port int, device *Device, context context.Context) (*Connection, error) {
	user, password := device.Credentials()
	baseConn, err := NewAMQPConnection(uri, port, user, password, context)
	if err != nil {
		return nil, err
	}
	session, err := baseConn.NewSession("hono", nil)
	if err != nil {
		return nil, err
	}

	receiver, err := session.NewReceiver(context, "command", nil)
	if err != nil {
		return nil, err
	}

	sender, err := session.NewSender(context, "", &amqp.SenderOptions{
		DynamicAddress:              true,
		SettlementMode:              amqp.SenderSettleModeUnsettled.Ptr(),
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeFirst.Ptr(),
	})
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		connection: baseConn,
		sender:     sender,
		receiver:   receiver,
		device:     device,
		command:    make(chan *Command),
		telemetry:  make(chan *amqp.Message),
		event:      make(chan *amqp.Message),
		response:   make(chan *amqp.Message),
	}

	conn.startSubscription()
	conn.startResponseSender()
	conn.startTelemetrySender()
	return conn, nil
}

func (c *Connection) startSubscription() {
	go func() {
		for {
			select {
			case <-c.Context().Done():
				return
			default:
				msg, err := c.receiver.Receive(c.Context())
				if err != nil {
					println("Error receiving message: " + err.Error())
					continue
				}
				c.receiver.AcceptMessage(c.Context(), msg)
				command, err := NewCommand(msg)
				if err != nil {
					println("Error creating command: ", err.Error())
					continue
				}
				c.command <- command
			}
		}
	}()
}

func (c *Connection) startResponseSender() {
	go func() {
		for {
			select {
			case <-c.Context().Done():
				return
			case msg := <-c.response:
				err := c.sender.Send(c.Context(), msg)
				if err != nil {
					println("Error sending response: " + err.Error())
				}
			}
		}
	}()
}

func (c *Connection) startTelemetrySender() {
	go func() {
		for {
			select {
			case <-c.Context().Done():
				return
			case msg := <-c.telemetry:
				err := c.sender.Send(c.Context(), msg)
				if err != nil {
					println("Error sending telemetry: " + err.Error())
				}
			}
		}
	}()
}

func (c *Connection) Context() context.Context {
	return c.connection.Context
}

func (c *Connection) Device() *Device {
	return c.device
}

func (c *Connection) CommandReceiver() <-chan *Command {
	return c.command
}

func (c *Connection) ResponseSender() chan<- *amqp.Message {
	return c.response
}

func (c *Connection) EventSender() chan<- *amqp.Message {
	return c.event
}

func (c *Connection) TelemetrySender() chan<- *amqp.Message {
	return c.telemetry
}

func (c *Connection) Close() error {
	return c.connection.Close()
}
