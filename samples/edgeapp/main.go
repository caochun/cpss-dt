package main

import (
	"context"
	"time"

	"edgeapp/hono"
	"edgeapp/service"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	device := &hono.Device{
		Tenant:    "org.i2ec.projects.c2e",
		Namespace: "org.i2ec.projects.c2e",
		Name:      "demo-device",
		AuthID:    "demo-device",
		Password:  "demo-secret",
	}
	conn, err := hono.NewConnection("localhost", 32672, device, ctx)
	if err != nil {
		panic(err)
	}

	defer cancel()
	defer conn.Close()

	service.StartTelemetrySender(conn)
	service.StartCommandHandler(conn)
	<-ctx.Done()
}
