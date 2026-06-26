package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/resp"
	"redis-go/internal/store"
)

const (
	clientReadTimeout   = 5 * time.Minute
	clientWriteTimeout  = 10 * time.Second
	internalHTTPTimeout = 3 * time.Second
)

// Response structures for JSON parsing
type GetResponse struct {
	Found bool   `json:"found"`
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

var httpClient = &http.Client{Timeout: internalHTTPTimeout}

func HandleConnection(connection net.Conn, kv *store.KeyValueStore, shards *config.Shards) {
	defer connection.Close()
	log.Printf("client connected on: %s\n", connection.RemoteAddr().String())

	for {
		var buffer []byte = make([]byte, 50*1024)
		if err := connection.SetReadDeadline(time.Now().Add(clientReadTimeout)); err != nil {
			log.Println("error setting read deadline:", err)
			return
		}
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
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			if err := kv.Set(key, value); err != nil {
				writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", err))
				return
			}
			writeRESP(conn, "+OK\r\n")
		} else {
			writeRESP(conn, "-ERR wrong number of arguments for 'set' command\r\n")
		}
	case "GET":
		if len(array) >= 2 && array[1].Type == resp.RESPBulkString {
			key := array[1].String

			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(conn, shards, shard, "GET", key, ""); err != nil {
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			value, exists, err := kv.Get(key)
			if err != nil {
				writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", err))
				return
			}
			if exists {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
				writeRESP(conn, response)
			} else {
				writeRESP(conn, "$-1\r\n")
			}
		} else {
			writeRESP(conn, "-ERR wrong number of arguments for 'get' command\r\n")
		}
	case "DEL":
		if len(array) >= 2 && array[1].Type == resp.RESPBulkString {
			key := array[1].String

			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(conn, shards, shard, "DEL", key, ""); err != nil {
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			if _, err := kv.Del(key); err != nil {
				writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", err))
				return
			}
			writeRESP(conn, "+OK\r\n")
		} else {
			writeRESP(conn, "-ERR wrong number of arguments for 'del' command\r\n")
		}
	case "PING":
		writeRESP(conn, "+PONG\r\n")
	default:
		writeRESP(conn, "-ERR unknown command\r\n")
	}
}

// routeToShard routes a command to the appropriate shard via HTTP
func routeToShard(conn net.Conn, shards *config.Shards, shard int, cmd, key, value string) error {
	addr := shards.Addrs[shard]

	var httpURL, method string
	switch cmd {
	case "SET":
		values := url.Values{}
		values.Set("key", key)
		values.Set("value", value)
		httpURL = fmt.Sprintf("http://%s/set?%s", addr, values.Encode())
		method = http.MethodPost
	case "GET":
		values := url.Values{}
		values.Set("key", key)
		httpURL = fmt.Sprintf("http://%s/get?%s", addr, values.Encode())
		method = http.MethodGet
	case "DEL":
		values := url.Values{}
		values.Set("key", key)
		httpURL = fmt.Sprintf("http://%s/del?%s", addr, values.Encode())
		method = http.MethodDelete
	default:
		return fmt.Errorf("unsupported command for routing: %s", cmd)
	}

	req, err := http.NewRequest(method, httpURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build routed request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to route to shard %d: %v", shard, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("shard %d returned HTTP %d", shard, resp.StatusCode)
	}

	// Parse JSON response and convert to RESP format
	switch cmd {
	case "GET":
		var getResp GetResponse
		if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
			return fmt.Errorf("failed to parse GET response: %v", err)
		}

		if getResp.Error != "" {
			writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", getResp.Error))
		} else if !getResp.Found {
			writeRESP(conn, "$-1\r\n")
		} else {
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(getResp.Value), getResp.Value)
			writeRESP(conn, response)
		}

	case "SET":
		var setResp SetResponse
		if err := json.NewDecoder(resp.Body).Decode(&setResp); err != nil {
			return fmt.Errorf("failed to parse SET response: %v", err)
		}

		if setResp.Success {
			writeRESP(conn, "+OK\r\n")
		} else {
			writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", setResp.Error))
		}

	case "DEL":
		var delResp DelResponse
		if err := json.NewDecoder(resp.Body).Decode(&delResp); err != nil {
			return fmt.Errorf("failed to parse DEL response: %v", err)
		}

		if delResp.Success {
			writeRESP(conn, "+OK\r\n")
		} else {
			writeRESP(conn, fmt.Sprintf("-ERR %s\r\n", delResp.Error))
		}
	}

	return nil
}

func writeRESP(conn net.Conn, value string) {
	if err := conn.SetWriteDeadline(time.Now().Add(clientWriteTimeout)); err != nil {
		log.Println("error setting write deadline:", err)
		return
	}
	if _, err := conn.Write([]byte(value)); err != nil {
		log.Println("error writing response:", err)
	}
}
