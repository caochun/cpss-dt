package hono

import (
	"encoding/json"
	"errors"

	"github.com/Azure/go-amqp"
)

var endPoint = "telemetry"

type TelemetryBuilder struct {
	device      *Device
	topic       string
	headers     map[string]any
	path        string
	value       any
	contentType string
}

func (c *TelemetryBuilder) SetDevice(device *Device) *TelemetryBuilder {
	c.device = device
	return c
}

func (c *TelemetryBuilder) SetTopic(topic string) *TelemetryBuilder {
	c.topic = topic
	return c
}

func (c *TelemetryBuilder) SetPath(path string) *TelemetryBuilder {
	c.path = path
	return c
}

func (c *TelemetryBuilder) SetValue(value any) *TelemetryBuilder {
	c.value = value
	return c
}

func (c *TelemetryBuilder) SetHeader(key string, value any) *TelemetryBuilder {
	if c.headers == nil {
		c.headers = map[string]any{}
	}

	c.headers[key] = value
	return c
}

func (c *TelemetryBuilder) SetContentType(contentType string) *TelemetryBuilder {
	c.contentType = contentType
	return c
}

func (c *TelemetryBuilder) Message() (*amqp.Message, error) {
	if c.headers == nil {
		c.headers = map[string]any{}
	}

	if c.device == nil {
		return nil, errors.New("device of the topic is not set")
	}

	telemetry := map[string]any{
		"topic":   c.device.Prefix() + c.topic,
		"path":    c.path,
		"value":   c.value,
		"headers": c.headers,
	}

	if c.contentType == "" {
		c.contentType = "application/json"
	}

	bodyData, err := json.Marshal(telemetry)
	if err != nil {
		return nil, err
	}

	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			To:          &endPoint,
			ContentType: &c.contentType,
		},
		Data: [][]byte{bodyData},
	}, nil
}
