package cloud

type EndPoint string

const API_VERSION = "/v1"

const (
	TENANTS     EndPoint = API_VERSION + "/tenants"
	DEVICES     EndPoint = API_VERSION + "/devices"
	CREDENTIALS EndPoint = API_VERSION + "/credentials"
)
