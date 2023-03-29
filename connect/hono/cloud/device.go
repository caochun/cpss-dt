package cloud

import (
	"encoding/json"
	"io"
)

type Device struct {
	Id string `json:"id,omitempty"`
}

type Devices RetrieveResult[Device]

func (api *API) RetrieveDevices(tenant string) (*Devices, error) {
	url := api.RequestURL(DEVICES) + "/" + tenant
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
		devices := &Devices{}
		err = json.Unmarshal(data, devices)
		if err != nil {
			return nil, err
		}
		return devices, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) RetrieveDevice(tenant, deviceId string) (*Device, error) {
	url := api.RequestURL(DEVICES) + "/" + tenant + "/" + deviceId
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
		device := &Device{}
		err = json.Unmarshal(data, device)
		if err != nil {
			return nil, err
		}
		return device, nil
	case 404:
		return nil, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) CreateDevice(tenant, deviceId string) error {
	url := api.RequestURL(DEVICES) + "/" + tenant + "/" + deviceId
	resp, err := api.Post(url, "", nil)
	if err != nil {
		return nil
	}
	switch resp.StatusCode {
	case 201:
		return nil
	default:
		return api.ErrorHandler(resp)
	}
}
