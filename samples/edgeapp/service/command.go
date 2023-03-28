package service

import "edgeapp/hono"

func StartCommandHandler(conn *hono.Connection) {
	response := conn.ResponseSender()
	command := conn.CommandReceiver()
	service := &GateService{
		Host: "114.212.82.238",
		Port: 32071,
	}

	handler := func(cmd *hono.Command) error {
		payload := cmd.GetPayload()
		device, topic := payload.ParseTopic()
		if device != conn.Device().Prefix() || cmd.GetReplyTo() == nil {
			return nil
		}
		println("Received command:", cmd.String())
		resBuilder := &hono.ResponseBuilder{}

		reqStatus := payload.Value["status"].(float64)
		data, status, err := service.Request(int(reqStatus))
		body := map[string]any{
			"status": status,
			"data":   data,
		}
		if err != nil {
			body["message"] = err.Error()
		} else {
			body["message"] = "ok"
		}

		resBuilder.SetDevice(device)
		resBuilder.SetCommand(cmd)
		resBuilder.SetTopic(topic)
		resBuilder.SetPath(payload.Path)
		resBuilder.SetCorrelationID(payload.GetDittoCorrelationID())
		resBuilder.SetStatus(200)
		resBuilder.SetContentType("application/json")
		resBuilder.SetBody(body)
		res, err := resBuilder.Message()
		if err != nil {
			return err
		}
		response <- res
		return nil
	}

	go func() {
		for {
			select {
			case <-conn.Context().Done():
				return
			case cmd := <-command:
				if err := handler(cmd); err != nil {
					println("Error handling command: " + err.Error())
				}
			}
		}
	}()
}
