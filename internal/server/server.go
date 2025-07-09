package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"

	"redis-go/internal/config"
	"redis-go/internal/resp"
	"redis-go/internal/store"
)

// Response structures for JSON parsing
type GetResponse struct {
	Value string `json:"value"`
	Error string `json:"error,omitempty"`
}

type SetResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type DelResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func HandleConnection(connection net.Conn, kv *store.KeyValueStore, shards *config.Shards) {
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
		executeCommands(connection, commands, kv, shards)
	}
}

func executeCommands(conn net.Conn, commands resp.RESPValue, kv *store.KeyValueStore, shards *config.Shards) {
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
			
			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(conn, shards, shard, "SET", key, value); err != nil {
						conn.Write([]byte(fmt.Sprintf("-ERR routing failed: %v\r\n", err)))
					}
					return
				}
			}
			
			kv.Set(key, value)
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		}
	case "GET":
		if len(array) >= 2 && array[1].Type == resp.RESPBulkString {
			key := array[1].String
			
			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(conn, shards, shard, "GET", key, ""); err != nil {
						conn.Write([]byte(fmt.Sprintf("-ERR routing failed: %v\r\n", err)))
					}
					return
				}
			}
			
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
			
			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(conn, shards, shard, "DEL", key, ""); err != nil {
						conn.Write([]byte(fmt.Sprintf("-ERR routing failed: %v\r\n", err)))
					}
					return
				}
			}
			
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

// routeToShard routes a command to the appropriate shard via HTTP
func routeToShard(conn net.Conn, shards *config.Shards, shard int, cmd, key, value string) error {
	addr := shards.Addrs[shard]
	
	var httpURL string
	switch cmd {
	case "SET":
		values := url.Values{}
		values.Set("key", key)
		values.Set("value", value)
		httpURL = fmt.Sprintf("http://%s/set?%s", addr, values.Encode())
	case "GET":
		values := url.Values{}
		values.Set("key", key)
		httpURL = fmt.Sprintf("http://%s/get?%s", addr, values.Encode())
	case "DEL":
		values := url.Values{}
		values.Set("key", key)
		httpURL = fmt.Sprintf("http://%s/del?%s", addr, values.Encode())
	default:
		return fmt.Errorf("unsupported command for routing: %s", cmd)
	}
	
	resp, err := http.Get(httpURL)
	if err != nil {
		return fmt.Errorf("failed to route to shard %d: %v", shard, err)
	}
	defer resp.Body.Close()
	
	// Parse JSON response and convert to RESP format
	switch cmd {
	case "GET":
		var getResp GetResponse
		if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
			return fmt.Errorf("failed to parse GET response: %v", err)
		}
		
		if getResp.Error != "" {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", getResp.Error)))
		} else if getResp.Value == "" {
			conn.Write([]byte("$-1\r\n"))
		} else {
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(getResp.Value), getResp.Value)
			conn.Write([]byte(response))
		}
		
	case "SET":
		var setResp SetResponse
		if err := json.NewDecoder(resp.Body).Decode(&setResp); err != nil {
			return fmt.Errorf("failed to parse SET response: %v", err)
		}
		
		if setResp.Success {
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", setResp.Error)))
		}
		
	case "DEL":
		var delResp DelResponse
		if err := json.NewDecoder(resp.Body).Decode(&delResp); err != nil {
			return fmt.Errorf("failed to parse DEL response: %v", err)
		}
		
		if delResp.Success {
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", delResp.Error)))
		}
	}
	
	return nil
}
