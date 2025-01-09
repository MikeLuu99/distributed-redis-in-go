package main

import (
	"log"
	"net"
)

func handleConnection(connection net.Conn) {
	defer connection.Close()
	log.Println("client connected on: %s\n", connection.RemoteAddr().String())

	for {
		var buffer []byte = make([]byte, 50*1024)
		n, err := connection.Read(buffer[:])
		if err != nil {
			connection.Close()
			log.Println("client disconnected with ", err.Error())
			break
		}
	}

	commands, err := ParseRESP(conn, buffer[:n])
	log.Println(commands)
	executeCommands(connection, commands, kv)
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Println("error listening")
	}

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection")
		}

		log.Println("client connected")
		go handleConnection(c)
	}
}
