package main

import (
	"log"
	"net"

	"redis-go/internal/server"
	"redis-go/internal/store"
)

func main() {
	listener, err := net.Listen("tcp", ":4444")
	if err != nil {
		log.Fatal("error listening:", err)
	}

	kv := store.NewKeyValueStore()
	log.Println("Redis server started on :4444")

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
			continue
		}

		log.Println("client connected")
		go server.HandleConnection(c, kv)
	}
}