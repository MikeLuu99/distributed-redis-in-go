package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"redis-go/internal/config"
	"redis-go/internal/db"
	"redis-go/internal/replication"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db     *db.Database
	shards *config.Shards
}

const internalHTTPTimeout = 3 * time.Second

var httpClient = &http.Client{Timeout: internalHTTPTimeout}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database, s *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	targetURL := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "redirecting from shard %d to shard %d (%q)\n", s.shards.CurIdx, shard, targetURL)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error building redirect request: %v", err)
		return
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, "Error redirecting the request: %v", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

// GetResponse represents the JSON response for GET operations
type GetResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value"`
	Error string `json:"error,omitempty"`
}

// GetHandler handles read requests from the database.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Index(key)

	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.GetKey(key)

	response := GetResponse{
		Found: value != nil,
		Value: string(value),
	}

	if err != nil {
		response.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// SetResponse represents the JSON response for SET operations
type SetResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// SetHandler handles write requests from the database.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.SetKey(key, []byte(value))

	response := SetResponse{
		Success: err == nil,
	}

	if err != nil {
		response.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DelResponse represents the JSON response for DEL operations
type DelResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// DelHandler handles delete requests from the database.
func (s *Server) DelHandler(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}

	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	_, err := s.db.DelKey(key)

	response := DelResponse{
		Success: err == nil,
	}

	if err != nil {
		response.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DeleteExtraKeysHandler deletes keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}

	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Index(key) != s.shards.CurIdx
	}))
}

// GetNextKeyForReplication returns the next key for replication.
func (s *Server) GetNextKeyForReplication(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	enc := json.NewEncoder(w)
	event, present, err := s.db.GetNextKeyForReplication()
	res := replication.NextKeyValue{
		Key:     event.Key,
		Value:   event.Value,
		Deleted: event.Deleted,
		Present: present,
	}
	if err != nil {
		res.Error = err.Error()
	}
	enc.Encode(&res)
}

// DeleteReplicationKey deletes the key from replica queue.
func (s *Server) DeleteReplicationKey(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}

	r.ParseForm()

	key := r.Form.Get("key")
	value := r.Form.Get("value")
	deleted, err := strconv.ParseBool(r.Form.Get("deleted"))
	if r.Form.Get("deleted") == "" {
		deleted = false
		err = nil
	}
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "error: %v", err)
		return
	}

	err = s.db.DeleteReplicationKey(key, value, deleted)
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}

	fmt.Fprintf(w, "ok")
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}
