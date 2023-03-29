package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Interface interface {
	NewRequest(method, url string, body any) (*http.Request, error)
	Get(url string) (*http.Response, error)
	Post(url, contentType string, data any) (*http.Response, error)
	Put(url string, data any) (*http.Response, error)
	Delete(url string) (*http.Response, error)
	ErrorHandler(resp *http.Response) error
}

type Auth struct {
	AuthId   string
	Password string
}

func (auth *Auth) String() string {
	return auth.AuthId + ":" + auth.Password
}

type Base struct {
	Client  *http.Client
	BaseURL string
	Auth    *Auth
}

func New(url string, port int) *Base {
	return &Base{
		Client:  http.DefaultClient,
		BaseURL: fmt.Sprintf("http://%s:%d", url, port),
	}
}

func (api *Base) NewRequest(method, url string, body any) (*http.Request, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(data)
	} else {
		reader = nil
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	if api.Auth != nil {
		req.SetBasicAuth(api.Auth.AuthId, api.Auth.Password)
	}
	return req, nil
}

func (api *Base) Get(url string) (*http.Response, error) {
	req, err := api.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := api.Client.Do(req)
	return resp, err
}

func (api *Base) Post(url, contentType string, data any) (*http.Response, error) {
	req, err := api.NewRequest(http.MethodPost, url, data)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := api.Client.Do(req)
	return resp, err
}

func (api *Base) Put(url string, data any) (*http.Response, error) {
	request, err := api.NewRequest(http.MethodPut, url, data)
	if err != nil {
		return nil, err
	}
	if data != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	resp, err := api.Client.Do(request)
	return resp, err
}

func (api *Base) Delete(url string) (*http.Response, error) {
	request, err := api.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := api.Client.Do(request)
	return resp, err
}

func (api *Base) ErrorHandler(resp *http.Response) error {
	return nil
}
