package service

import (
	"edgeapp/hono"
	"edgeapp/ws"
	"time"
)

type Event struct {
	Timestamp string `json:"timestamp"`
	Status    int    `json:"status"`
}

func (e *Event) Json() (map[string]any, error) {
	ts, err := time.Parse(time.DateTime, e.Timestamp)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":        e.Status,
		"last_modified": ts.Unix(),
	}, nil
}

func StartTelemetrySender(conn *hono.Connection) {
	telemetry := conn.TelemetrySender()
	wsConn, err := ws.NewConnection(conn.Context(), "ws://114.212.82.238:30174/websocket")
	if err != nil {
		panic(err)
	}
	receiver := wsConn.Receiver()

	handler := func(event *Event) error {
		payload, err := event.Json()
		if err != nil {
			return err
		}
		telemetryBuilder := &hono.TelemetryBuilder{}
		telemetryBuilder.SetDevice(conn.Device())
		telemetryBuilder.SetTopic(MODIFY_THING.String())
		telemetryBuilder.SetPath("/features/status/properties")
		telemetryBuilder.SetContentType("application/json")
		telemetryBuilder.SetValue(payload)
		msg, err := telemetryBuilder.Message()
		if err != nil {
			return err
		}
		telemetry <- msg
		println("Sending telemetry message:", string(msg.GetData()))

		return nil
	}

	go func() {
		for {
			select {
			case <-conn.Context().Done():
				return
			case msg := <-receiver:
				event := &Event{}
				if err := msg.Unmarshal(event); err != nil {
					println("Error unmarshalling event: " + err.Error())
					continue
				}
				if err := handler(event); err != nil {
					println("Error sending telemetry: " + err.Error())
				}
			}
		}
	}()
}
