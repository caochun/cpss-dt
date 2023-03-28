package hono

import (
	"encoding/json"
	"errors"

	"github.com/Azure/go-amqp"
)

type ResponseBuilder struct {
	command     *Command
	device      string
	topic       string
	path        string
	status      int
	body        any
	corrId      string
	contentType string
}

func (c *ResponseBuilder) SetDevice(device string) *ResponseBuilder {
	c.device = device
	return c
}

func (c *ResponseBuilder) SetCommand(command *Command) *ResponseBuilder {
	c.command = command
	return c
}

func (c *ResponseBuilder) SetTopic(topic string) *ResponseBuilder {
	c.topic = topic
	return c
}

func (c *ResponseBuilder) SetPath(path string) *ResponseBuilder {
	c.path = path
	return c
}

func (c *ResponseBuilder) SetCorrelationID(id string) *ResponseBuilder {
	c.corrId = id
	return c
}

func (c *ResponseBuilder) SetContentType(contentType string) *ResponseBuilder {
	c.contentType = contentType
	return c
}

func (c *ResponseBuilder) SetStatus(status int) *ResponseBuilder {
	c.status = status
	return c
}

func (c *ResponseBuilder) SetBody(body any) *ResponseBuilder {
	c.body = body
	return c
}

func (c *ResponseBuilder) Message() (*amqp.Message, error) {
	replyTo := c.command.GetReplyTo()
	honoCorrId := c.command.GetCorrelationID()

	if c.device == "" {
		return nil, errors.New("device of the topic is not set")
	}

	response := map[string]any{
		"topic":  c.device + c.topic,
		"path":   c.path,
		"value":  c.body,
		"status": int32(c.status),
		"headers": map[string]any{
			"correlation-id": c.corrId,
			"content-type":   c.contentType,
		},
	}

	data, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}

	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			ContentType:   &c.contentType,
			CorrelationID: &honoCorrId,
			To:            replyTo,
		},
		ApplicationProperties: map[string]any{
			"status": int32(c.status),
		},
		Data: [][]byte{data},
	}, nil
}
