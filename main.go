package main

import (
	"fmt"
	"log"
	"net"
	"sync"
)

func handleConnection(connection net.Conn, kv *KeyValueStore) {
	defer connection.Close()
	log.Printf("client connected on: %s\n", connection.RemoteAddr().String())

	for {
		var buffer []byte = make([]byte, 50*1024)
		n, err := connection.Read(buffer[:])
		if err != nil {
			connection.Close()
			log.Println("client disconnected with ", err.Error())
			break
		}

		commands, err := ParseRESP(connection, buffer[:n])
		if err != nil {
			log.Println("error parsing RESP:", err)
			continue
		}
		log.Println(commands)
		executeCommands(connection, commands, kv)
	}
}

type KeyValueStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
	}
}

func (kv *KeyValueStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.data[key]
	return value, exists
}

func executeCommands(conn net.Conn, commands interface{}, kv *KeyValueStore) {
	if commands == nil {
		return
	}

	array, ok := commands.([]interface{})
	if !ok || len(array) == 0 {
		return
	}

	cmdStr, ok := array[0].(string)
	if !ok {
		return
	}

	switch cmdStr {
	case "SET":
		if len(array) >= 3 {
			key, _ := array[1].(string)
			value, _ := array[2].(string)
			kv.Set(key, value)
			conn.Write([]byte("+OK\r\n"))
		}
	case "GET":
		if len(array) >= 2 {
			key, _ := array[1].(string)
			if value, exists := kv.Get(key); exists {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		}
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func main() {
	listener, err := net.Listen("tcp", ":4444")
	if err != nil {
		log.Fatal("error listening:", err)
	}

	kv := NewKeyValueStore()
	log.Println("Redis server started on :4444")

	for {
		c, err := listener.Accept()
		if err != nil {
			log.Println("error accepting connection:", err)
			continue
		}

		log.Println("client connected")
		go handleConnection(c, kv)
	}
}
