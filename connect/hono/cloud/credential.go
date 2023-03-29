package cloud

type Credential struct {
	Type    string           `json:"type"`
	AuthId  string           `json:"auth-id"`
	Secrets []map[string]any `json:"secrets"`
}

func (api *API) SetCredential(tenant, deviceId string, credentials []*Credential) error {
	url := api.RequestURL(CREDENTIALS) + "/" + tenant + "/" + deviceId
	resp, err := api.Put(url, credentials)
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
