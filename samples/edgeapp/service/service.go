package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type GateService struct {
	Host string
	Port int
}

func (s *GateService) Request(reqStatus int) (data map[string]any, status int, err error) {
	var endpoint string
	if reqStatus == 0 {
		endpoint = "down"
	} else {
		endpoint = "up"
	}

	url := fmt.Sprintf("http://%s:%d/pole/%s", s.Host, s.Port, endpoint)
	res, err := http.Get(url)
	if err != nil {
		status = 500
		return
	}
	status = res.StatusCode
	rawData, err := io.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(rawData, &data)
	return
}
