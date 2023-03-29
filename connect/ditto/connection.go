package ditto

import (
	"encoding/json"
	"io"
)

type Connection struct {
	Id      string             `json:"id,omitempty"`
	Name    string             `json:"name,omitempty"`
	Type    string             `json:"connectionType"`
	Status  string             `json:"connectionStatus"`
	URI     string             `json:"uri"`
	Sources []ConnectionSource `json:"sources"`
	Targets []ConnectionTarget `json:"targets"`
}

type ConnectionSource struct {
	Addresses               []string                          `json:"addresses,omitempty"`
	ConsumerCount           int                               `json:"consumerCount,omitempty"`
	QoS                     int                               `json:"qos,omitempty"`
	AuthorizationContext    []string                          `json:"authorizationContext,omitempty"`
	Enforcement             ConnectionEnforcement             `json:"enforcement,omitempty"`
	AcknowledgementRequests ConnectionAcknowledgementRequests `json:"acknowledgementRequests,omitempty"`
	PayloadMapping          []string                          `json:"payloadMapping,omitempty"`
	HeaderMapping           map[string]string                 `json:"headerMapping,omitempty"`
	ReplyTarget             ConnectionReplyTarget             `json:"replyTarget,omitempty"`
}

type ConnectionEnforcement struct {
	Input   string   `json:"input"`
	Filters []string `json:"filters"`
}

type ConnectionAcknowledgementRequests struct {
	Includes []string `json:"includes"`
	Filter   string   `json:"filter,omitempty"`
}

type ConnectionReplyTarget struct {
	Enabled               bool              `json:"enabled,omitempty"`
	Address               string            `json:"address"`
	HeaderMapping         map[string]string `json:"headerMapping,omitempty"`
	ExpectedResponseTypes []string          `json:"expectedResponseTypes"`
}

type ConnectionTarget struct {
	Address                    string            `json:"address,omitempty"`
	Topics                     []string          `json:"topics,omitempty"`
	QoS                        int               `json:"qos,omitempty"`
	AuthorizationContext       []string          `json:"authorizationContext,omitempty"`
	IssuedAcknowledgementLabel string            `json:"issuedAcknowledgementLabel,omitempty"`
	PayloadMapping             []string          `json:"payloadMapping,omitempty"`
	HeaderMapping              map[string]string `json:"headerMapping,omitempty"`
}

func (c *Connection) String() string {
	res, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return c.Name
	}
	return string(res)
}

func (api *API) RetrieveConnections() ([]*Connection, error) {
	url := api.RequestURL(CONNECTIONS)
	resp, err := api.Get(url)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200:
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var res []*Connection
		err = json.Unmarshal(data, &res)
		if err != nil {
			return nil, err
		}
		return res, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) RetrieveConnection(connectionId string) (*Connection, error) {
	url := api.RequestURL(CONNECTIONS) + "/" + connectionId
	resp, err := api.Get(url)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200:
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		res := &Connection{}
		err = json.Unmarshal(data, res)
		if err != nil {
			return nil, err
		}
		return res, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) ModifyConnection(req *Connection) (*Connection, error) {
	url := api.RequestURL(CONNECTIONS) + "/" + req.Id
	resp, err := api.Put(url, req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 204:
		return req, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) DeleteConnection(connectionId string) error {
	url := api.RequestURL(CONNECTIONS) + "/" + connectionId
	resp, err := api.Delete(url)
	if err != nil {
		return nil
	}
	switch resp.StatusCode {
	case 204:
		return nil
	default:
		return api.ErrorHandler(resp)
	}
}
