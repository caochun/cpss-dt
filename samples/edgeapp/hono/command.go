package hono

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/go-amqp"
)

type Payload struct {
	FullTopic string         `json:"topic"`
	Path      string         `json:"path"`
	Value     map[string]any `json:"value"`
	Headers   map[string]any `json:"headers"`
	Status    int            `json:"status"`
}

type Command struct {
	raw  *amqp.Message
	body *Payload
}

func NewCommand(raw *amqp.Message) (*Command, error) {
	payload := &Payload{}
	err := json.Unmarshal(raw.GetData(), payload)
	if err != nil {
		return nil, err
	}
	return &Command{raw: raw, body: payload}, nil
}

func (c *Command) GetCorrelationID() string {
	return c.raw.Properties.CorrelationID.(string)
}

func (c *Command) GetReplyTo() *string {
	return c.raw.Properties.ReplyTo
}

func (c *Command) GetPayload() *Payload {
	return c.body
}

func (c *Command) Message() *amqp.Message {
	return c.raw
}

func (c *Command) String() string {
	return string(c.raw.GetData())
}

func (p *Payload) GetDittoCorrelationID() string {
	return p.Headers["correlation-id"].(string)
}

func (p *Payload) ParseTopic() (device string, topic string) {
	slices := strings.SplitN(p.FullTopic, "/", 3)
	if len(slices) < 2 {
		return "", p.FullTopic
	}
	return fmt.Sprintf("%s/%s", slices[0], slices[1]), "/" + slices[2]
}
