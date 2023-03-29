package cloud

import (
	"encoding/json"
	"io"
)

type Tenant struct {
	Id string `json:"id,omitempty"`
}

type Tenants RetrieveResult[Tenant]

func (api *API) RetrieveTenants() (*Tenants, error) {
	url := api.RequestURL(TENANTS)
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
		tenants := &Tenants{}
		err = json.Unmarshal(data, tenants)
		if err != nil {
			return nil, err
		}
		return tenants, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) ExistsTenant(tenant string) (bool, error) {
	url := api.RequestURL(TENANTS) + "/" + tenant
	resp, err := api.Get(url)
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, api.ErrorHandler(resp)
	}
}

func (api *API) CreateTenant(tenant string) error {
	url := api.RequestURL(TENANTS) + "/" + tenant
	resp, err := api.Post(url, "", nil)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 201:
		return nil
	default:
		return api.ErrorHandler(resp)
	}
}
