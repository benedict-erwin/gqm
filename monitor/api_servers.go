package monitor

import (
	"net/http"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
)

// handleListServers returns all registered server instances with their status.
func (m *Monitor) handleListServers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	serverIDs, err := m.rdb.SMembers(ctx, m.key("servers")).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list servers", "INTERNAL")
		return
	}
	sort.Strings(serverIDs)

	if len(serverIDs) == 0 {
		writeJSON(w, http.StatusOK, response{Data: []any{}})
		return
	}

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(serverIDs))
	for i, id := range serverIDs {
		cmds[i] = pipe.HGetAll(ctx, m.key("server", id))
	}
	pipe.Exec(ctx)

	servers := make([]map[string]any, 0, len(serverIDs))
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil || len(data) == 0 {
			continue
		}
		s := make(map[string]any, len(data)+1)
		for k, v := range data {
			if k == "pools" {
				s[k] = strings.Split(v, ",")
			} else {
				s[k] = v
			}
		}
		servers = append(servers, s)
	}

	writeJSON(w, http.StatusOK, response{Data: servers})
}

// handleGetServer returns details for a single server instance.
func (m *Monitor) handleGetServer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}

	serverKey := m.key("server", id)
	data, err := m.rdb.HGetAll(ctx, serverKey).Result()
	if err != nil || len(data) == 0 {
		writeError(w, http.StatusNotFound, "server not found", "NOT_FOUND")
		return
	}

	server := make(map[string]any, len(data))
	for k, v := range data {
		if k == "pools" {
			server[k] = strings.Split(v, ",")
		} else {
			server[k] = v
		}
	}

	writeJSON(w, http.StatusOK, response{Data: server})
}
