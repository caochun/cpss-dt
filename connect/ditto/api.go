package ditto

import (
	"connect/api"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type API struct {
	api.Base
}

func New(url string, port int, authId, password string) *API {
	return &API{
		Base: api.Base{
			Client:  http.DefaultClient,
			BaseURL: fmt.Sprintf("http://%s:%d", url, port),
			Auth: &api.Auth{
				AuthId:   authId,
				Password: password,
			},
		},
	}
}

func (api *API) RequestURL(ep EndPoint) string {
	return api.BaseURL + string(ep)
}

func (api *API) ErrorHandler(resp *http.Response) error {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	res := &Error{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return err
	}
	return res
}

type Error struct {
	Status      int    `json:"status"`
	ErrorName   string `json:"error"`
	Message     string `json:"message"`
	Description string `json:"description"`
	HRef        string `json:"href"`
}

func (resp *Error) Error() string {
	data, err := json.Marshal(resp)
	if err != nil {
		return resp.Message
	}
	return string(data)
}
