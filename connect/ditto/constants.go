package ditto

type EndPoint string

const API_VERSION = "/api/2"

const (
	CONNECTIONS    EndPoint = API_VERSION + "/connections"
	THINGS         EndPoint = API_VERSION + "/things"
	POLICIES       EndPoint = API_VERSION + "/policies"
	THING_MESSAGES          = "/inbox/messages"
)
