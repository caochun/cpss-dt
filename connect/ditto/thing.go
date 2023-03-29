package ditto

import (
	"encoding/json"
	"io"
)

type Thing struct {
	ThingId    string         `json:"thingId,omitempty"`
	PolicyId   string         `json:"policyId,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Features   map[string]any `json:"features,omitempty"`
}

func (thing *Thing) String() string {
	res, err := json.MarshalIndent(thing, "", "  ")
	if err != nil {
		return thing.ThingId
	}
	return string(res)
}

func (api *API) RetrieveThings() ([]*Thing, error) {
	url := api.RequestURL(THINGS)
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
		var res []*Thing
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, err
		}
		return res, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) RetrieveThing(thingId string) (*Thing, error) {
	url := api.RequestURL(THINGS) + "/" + thingId
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
		res := &Thing{}
		if err := json.Unmarshal(data, res); err != nil {
			return nil, err
		}
		return res, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) ModifyThing(req *Thing) (*Thing, error) {
	url := api.RequestURL(THINGS) + "/" + req.ThingId
	resp, err := api.Put(url, req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 201, 204:
		return req, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) DeleteThing(thingId string) error {
	url := api.RequestURL(THINGS) + "/" + thingId
	resp, err := api.Delete(url)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 204:
		return nil
	default:
		return api.ErrorHandler(resp)
	}
}
