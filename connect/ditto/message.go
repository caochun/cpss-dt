package ditto

import (
	"fmt"
	"net/http"
)

func (api *API) Ask(thingId, message, contentType string, data any, timeout int) (*http.Response, error) {
	url := api.RequestURL(THINGS) + "/" + thingId + THING_MESSAGES + "/" + message
	if timeout != 0 {
		url += fmt.Sprintf("?timeout=%d", timeout)
	}
	resp, err := api.Post(url, contentType, data)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200, 204:
		return resp, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) FireForgot(thingId, message, contentType string, data any) error {
	_, err := api.Ask(thingId, message, contentType, data, 0)
	if err != nil {
		return err
	}
	return nil
}
