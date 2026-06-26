package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/metrics"
	"redis-go/internal/resp"
	"redis-go/internal/store"
)

const (
	clientReadTimeout   = 5 * time.Minute
	clientWriteTimeout  = 10 * time.Second
	internalHTTPTimeout = 3 * time.Second
	maxCommandArgs      = resp.MaxArrayElements
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
var internalAuthToken string

type Options struct {
	AuthToken string
}

func SetInternalAuthToken(token string) {
	internalAuthToken = token
}

func HandleConnection(connection net.Conn, kv *store.KeyValueStore, shards *config.Shards) {
	HandleConnectionWithOptions(connection, kv, shards, Options{})
}

func HandleConnectionWithOptions(connection net.Conn, kv *store.KeyValueStore, shards *config.Shards, opts Options) {
	defer connection.Close()
	metrics.ActiveConnections.Add(1)
	defer metrics.ActiveConnections.Add(-1)
	log.Printf("client connected on: %s\n", connection.RemoteAddr().String())

	parser := resp.NewParser(connection)
	authenticated := opts.AuthToken == ""
	for {
		if err := connection.SetReadDeadline(time.Now().Add(clientReadTimeout)); err != nil {
			log.Println("error setting read deadline:", err)
			return
		}

		commands, err := parser.Parse()
		if err != nil {
			log.Println("client disconnected or sent invalid RESP:", err)
			return
		}
		log.Println(commands)

		if !authenticated {
			if isAuthCommand(commands, opts.AuthToken) {
				authenticated = true
				writeRESP(connection, "+OK\r\n")
			} else {
				writeRESP(connection, "-NOAUTH Authentication required\r\n")
			}
			continue
		}

		executeCommands(connection, commands, kv, shards)
	}
}

func isAuthCommand(commands resp.RESPValue, token string) bool {
	if commands.Type != resp.RESPArray || len(commands.Array) != 2 {
		return false
	}
	if commands.Array[0].Type != resp.RESPBulkString || commands.Array[1].Type != resp.RESPBulkString {
		return false
	}
	return strings.EqualFold(commands.Array[0].String, "AUTH") && commands.Array[1].String == token
}

func executeCommands(conn net.Conn, commands resp.RESPValue, kv *store.KeyValueStore, shards *config.Shards) {
	if commands.Type != resp.RESPArray || len(commands.Array) == 0 {
		return
	}
	if len(commands.Array) > maxCommandArgs {
		writeRESP(conn, "-ERR command has too many arguments\r\n")
		return
	}

	array := commands.Array
	if array[0].Type != resp.RESPBulkString {
		return
	}

	cmdStr := array[0].String

	cmdStr = strings.ToUpper(cmdStr)
	metrics.AddCommand(cmdStr)
	defer metrics.ObserveCommand(cmdStr, time.Now())
	switch cmdStr {
	case "SET":
		if len(array) >= 3 && array[1].Type == resp.RESPBulkString && array[2].Type == resp.RESPBulkString {
			key := array[1].String
			value := array[2].String

			// Check if we need to route this to another shard
			if shards != nil {
				shard := shards.Index(key)
				if shard != shards.CurIdx {
					if err := routeToShard(context.Background(), conn, shards, shard, "SET", key, value); err != nil {
						metrics.RoutingFailuresTotal.Add(1)
						metrics.AddCommandError(cmdStr)
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			if err := kv.Set(key, value); err != nil {
				metrics.AddCommandError(cmdStr)
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
					if err := routeToShard(context.Background(), conn, shards, shard, "GET", key, ""); err != nil {
						metrics.RoutingFailuresTotal.Add(1)
						metrics.AddCommandError(cmdStr)
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			value, exists, err := kv.Get(key)
			if err != nil {
				metrics.AddCommandError(cmdStr)
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
					if err := routeToShard(context.Background(), conn, shards, shard, "DEL", key, ""); err != nil {
						metrics.RoutingFailuresTotal.Add(1)
						metrics.AddCommandError(cmdStr)
						writeRESP(conn, fmt.Sprintf("-ERR routing failed: %v\r\n", err))
					}
					return
				}
			}

			if _, err := kv.Del(key); err != nil {
				metrics.AddCommandError(cmdStr)
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
		metrics.AddCommandError(cmdStr)
		writeRESP(conn, "-ERR unknown command\r\n")
	}
}

// routeToShard routes a command to the appropriate shard via HTTP
func routeToShard(ctx context.Context, conn net.Conn, shards *config.Shards, shard int, cmd, key, value string) error {
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

	req, err := http.NewRequestWithContext(ctx, method, httpURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build routed request: %v", err)
	}
	if internalAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+internalAuthToken)
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
