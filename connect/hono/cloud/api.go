package cloud

import (
	"connect/api"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type API struct {
	api.Base
}

func New(url string, port int) *API {
	return &API{
		Base: api.Base{
			Client:  http.DefaultClient,
			BaseURL: fmt.Sprintf("http://%s:%d", url, port),
		},
	}
}

func (api *API) RequestURL(ep EndPoint) string {
	return api.BaseURL + string(ep)
}

func (api *API) ErrorHandler(resp *http.Response) error {
	if resp.StatusCode == 401 {
		return errors.New("error calling api: unauthorized request")
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	e := &Error{}
	err = json.Unmarshal(data, e)
	if err != nil {
		return err
	}
	return e
}

type Error struct {
	ErrorMessage string `json:"error"`
}

func (e *Error) Error() string {
	return e.ErrorMessage
}

type RetrieveResult[T any] struct {
	Total  int `json:"total"`
	Result []T `json:"result"`
}
