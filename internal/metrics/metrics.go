package metrics

import (
	"expvar"
	"time"
)

var (
	CommandsTotal             = expvar.NewMap("redis_go_commands_total")
	CommandErrorsTotal        = expvar.NewMap("redis_go_command_errors_total")
	CommandLatencyNsTotal     = expvar.NewMap("redis_go_command_latency_ns_total")
	HTTPHandlersTotal         = expvar.NewMap("redis_go_http_handlers_total")
	HTTPHandlerLatencyNsTotal = expvar.NewMap("redis_go_http_handler_latency_ns_total")
	ActiveConnections         = expvar.NewInt("redis_go_active_connections")
	DBWritesTotal             = expvar.NewInt("redis_go_db_writes_total")
	ReplicationEventsTotal    = expvar.NewInt("redis_go_replication_events_total")
	ReplicationErrorsTotal    = expvar.NewInt("redis_go_replication_errors_total")
	ReplicationQueueDepth     = expvar.NewInt("redis_go_replication_queue_depth")
	RoutingFailuresTotal      = expvar.NewInt("redis_go_routing_failures_total")
)

func AddCommand(command string) {
	CommandsTotal.Add(command, 1)
}

func AddCommandError(command string) {
	CommandErrorsTotal.Add(command, 1)
}

func AddHTTPHandler(handler string) {
	HTTPHandlersTotal.Add(handler, 1)
}

func ObserveCommand(command string, start time.Time) {
	CommandLatencyNsTotal.Add(command, time.Since(start).Nanoseconds())
}

func ObserveHTTPHandler(handler string, start time.Time) {
	AddHTTPHandler(handler)
	HTTPHandlerLatencyNsTotal.Add(handler, time.Since(start).Nanoseconds())
}
