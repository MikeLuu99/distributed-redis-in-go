package server

import (
	"fmt"
	"log"
	"net"
	"strings"

	"redis-go/internal/resp"
	"redis-go/internal/store"
)

func HandleConnection(connection net.Conn, kv *store.KeyValueStore) {
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

		commands, err := resp.ParseRESP(connection, buffer[:n])
		if err != nil {
			log.Println("error parsing RESP:", err)
			continue
		}
		log.Println(commands)
		executeCommands(connection, commands, kv)
	}
}

func executeCommands(conn net.Conn, commands interface{}, kv *store.KeyValueStore) {
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

	cmdStr = strings.ToUpper(cmdStr)
	switch cmdStr {
	case "SET":
		if len(array) >= 3 {
			key, _ := array[1].(string)
			value, _ := array[2].(string)
			kv.Set(key, value)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
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
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		}
	case "DEL":
		if len(array) >= 2 {
			key, _ := array[1].(string)
			kv.Del(key)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
		}
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}
