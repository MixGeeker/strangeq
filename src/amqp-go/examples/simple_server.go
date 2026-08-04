//go:build ignore

package main

import (
	"log"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

func main() {
	// Create a new AMQP server. Build() is used directly rather than a
	// convenience constructor: it can REFUSE to start on a data directory whose
	// recovery cannot be completed safely (see --unsafe-recovery), and a
	// constructor that swallowed that error would boot an empty broker and
	// start confirming durable publishes it can never recover.
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":5672"
	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		log.Fatal("Failed to build server:", err)
	}

	// Start the server
	log.Println("Starting AMQP server on :5672...")
	if err := srv.Start(); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
