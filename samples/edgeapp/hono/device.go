package hono

import "fmt"

type Device struct {
	Tenant    string
	Namespace string
	Name      string
	AuthID    string
	Password  string
}

func (d *Device) DeviceID() string {
	return fmt.Sprintf("%s:%s", d.Namespace, d.Name)
}

func (d *Device) Prefix() string {
	return fmt.Sprintf("%s/%s", d.Namespace, d.Name)
}

func (d *Device) Credentials() (string, string) {
	return fmt.Sprintf("%s@%s", d.AuthID, d.Tenant), d.Password
}
