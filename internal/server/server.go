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

func executeCommands(conn net.Conn, commands resp.RESPValue, kv *store.KeyValueStore) {
	if commands.Type != resp.RESPArray || len(commands.Array) == 0 {
		return
	}

	array := commands.Array
	if array[0].Type != resp.RESPBulkString {
		return
	}

	cmdStr := array[0].String

	cmdStr = strings.ToUpper(cmdStr)
	switch cmdStr {
	case "SET":
		if len(array) >= 3 && array[1].Type == resp.RESPBulkString && array[2].Type == resp.RESPBulkString {
			key := array[1].String
			value := array[2].String
			kv.Set(key, value)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		}
	case "GET":
		if len(array) >= 2 && array[1].Type == resp.RESPBulkString {
			key := array[1].String
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
		if len(array) >= 2 && array[1].Type == resp.RESPBulkString {
			key := array[1].String
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
