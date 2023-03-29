package ditto

import (
	"encoding/json"
	"io"
)

const (
	RESOURCE_READ  = "READ"
	RESOURCE_WRITE = "WRITE"
)

// Partial updates are not supported
type Policy struct {
	PolicyId string                  `json:"policyId,omitempty"`
	Entries  map[string]PolicyEntry  `json:"entries,omitempty"`
	Imports  map[string]PolicyImport `json:"imports,omitempty"`
}

type PolicyEntry struct {
	Subjects   map[string]PolicySubject  `json:"subjects,omitempty"`
	Resources  map[string]PolicyResource `json:"resources,omitempty"`
	Importable string                    `json:"importable,omitempty"`
}

type PolicySubject struct {
	Type string `json:"type,omitempty"`
}

type PolicyResource struct {
	Grant  []string `json:"grant"`
	Revoke []string `json:"revoke"`
}

type PolicyImport struct {
	Entries []string `json:"entries"`
}

func (policy *Policy) String() string {
	data, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return policy.PolicyId
	}
	return string(data)
}

func (api *API) RetrievePolicy(policyId string) (*Policy, error) {
	url := api.RequestURL(POLICIES) + "/" + policyId
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
		policy := &Policy{}
		err = json.Unmarshal(data, policy)
		if err != nil {
			return nil, err
		}
		return policy, nil
	case 404:
		return nil, nil
	default:
		return nil, api.ErrorHandler(resp)
	}
}

func (api *API) ModifyPolicy(req *Policy) (*Policy, error) {
	url := api.RequestURL(POLICIES) + "/" + req.PolicyId
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

func (api *API) DeletePolicy(policyId string) error {
	url := api.RequestURL(POLICIES) + "/" + policyId
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
