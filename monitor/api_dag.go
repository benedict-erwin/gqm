package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/redis/go-redis/v9"
)

// dagNode represents a node in the DAG graph response.
type dagNode struct {
	ID           string `json:"id"`
	Type         string `json:"type,omitempty"`
	Status       string `json:"status,omitempty"`
	Queue        string `json:"queue,omitempty"`
	AllowFailure bool   `json:"allow_failure"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// dagEdge represents a directed edge from parent to child.
type dagEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// dagGraphData is the data field for the graph response.
type dagGraphData struct {
	RootID    string    `json:"root_id"`
	Nodes     []dagNode `json:"nodes"`
	Edges     []dagEdge `json:"edges"`
	Truncated bool      `json:"truncated"`
}

// handleListDeferred returns paginated deferred jobs with pending dependency info.
//
//	GET /api/v1/dag/deferred?page=1&limit=20
func (m *Monitor) handleListDeferred(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	page, limit := pagination(r)

	deferredKey := m.key("deferred")

	// Total count.
	total, err := m.rdb.SCard(ctx, deferredKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count deferred jobs", "INTERNAL")
		return
	}

	if total == 0 {
		writeJSON(w, http.StatusOK, response{
			Data: []any{},
			Meta: &meta{Page: page, Limit: limit, Total: 0},
		})
		return
	}

	// Get all IDs, sort, then paginate in Go.
	ids, err := m.rdb.SMembers(ctx, deferredKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list deferred jobs", "INTERNAL")
		return
	}
	sort.Strings(ids)

	// Paginate.
	start := (page - 1) * limit
	if start >= len(ids) {
		writeJSON(w, http.StatusOK, response{
			Data: []any{},
			Meta: &meta{Page: page, Limit: limit, Total: int(total)},
		})
		return
	}
	end := start + limit
	if end > len(ids) {
		end = len(ids)
	}
	pageIDs := ids[start:end]

	// Pipeline: HGETALL per job + SMEMBERS pending_deps per job.
	jobs := m.fetchDeferredJobs(ctx, pageIDs)

	writeJSON(w, http.StatusOK, response{
		Data: jobs,
		Meta: &meta{Page: page, Limit: limit, Total: int(total)},
	})
}

// fetchDeferredJobs fetches job data and pending deps for deferred job IDs.
func (m *Monitor) fetchDeferredJobs(ctx context.Context, jobIDs []string) []map[string]any {
	if len(jobIDs) == 0 {
		return []map[string]any{}
	}

	pipe := m.rdb.Pipeline()

	hgetCmds := make([]*redis.MapStringStringCmd, len(jobIDs))
	pendCmds := make([]*redis.StringSliceCmd, len(jobIDs))

	for i, id := range jobIDs {
		hgetCmds[i] = pipe.HGetAll(ctx, m.key("job", id))
		pendCmds[i] = pipe.SMembers(ctx, m.key("job", id, "pending_deps"))
	}
	pipe.Exec(ctx)

	result := make([]map[string]any, 0, len(jobIDs))
	for i := range jobIDs {
		data, err := hgetCmds[i].Result()
		if err != nil || len(data) == 0 {
			continue
		}

		job := mapToJobResponse(data)
		pending, _ := pendCmds[i].Result()
		if pending == nil {
			pending = []string{}
		}
		job["pending_deps"] = pending
		result = append(result, job)
	}
	return result
}

// handleListDAGRoots discovers DAG parent jobs by scanning for dependents keys.
// Returns jobs that have at least one child (dependents set exists).
//
//	GET /api/v1/dag/roots?page=1&limit=20
func (m *Monitor) handleListDAGRoots(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	page, limit := pagination(r)

	// SCAN for keys matching prefix:job:*:dependents
	pattern := m.key("job", "*", "dependents")
	var allIDs []string
	seen := make(map[string]bool)

	iter := m.rdb.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		// Key format: prefix:job:{id}:dependents — extract the job ID.
		key := iter.Val()
		parts := extractJobIDFromDependentsKey(key, m.prefix)
		if parts != "" && !seen[parts] {
			seen[parts] = true
			allIDs = append(allIDs, parts)
		}
	}
	if err := iter.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to scan DAG roots", "INTERNAL")
		return
	}

	sort.Strings(allIDs)
	total := len(allIDs)

	// Paginate.
	start := (page - 1) * limit
	if start >= total {
		writeJSON(w, http.StatusOK, response{
			Data: []any{},
			Meta: &meta{Page: page, Limit: limit, Total: total},
		})
		return
	}
	end := start + limit
	if end > total {
		end = total
	}
	pageIDs := allIDs[start:end]

	// Pipeline: fetch job data + count dependents for each root.
	pipe := m.rdb.Pipeline()
	hgetCmds := make([]*redis.MapStringStringCmd, len(pageIDs))
	scardCmds := make([]*redis.IntCmd, len(pageIDs))
	for i, id := range pageIDs {
		hgetCmds[i] = pipe.HGetAll(ctx, m.key("job", id))
		scardCmds[i] = pipe.SCard(ctx, m.key("job", id, "dependents"))
	}
	pipe.Exec(ctx)

	jobs := make([]map[string]any, 0, len(pageIDs))
	for i := range pageIDs {
		data, err := hgetCmds[i].Result()
		if err != nil || len(data) == 0 {
			continue
		}
		job := mapToJobResponse(data)
		childCount, _ := scardCmds[i].Result()
		job["child_count"] = childCount
		jobs = append(jobs, job)
	}

	writeJSON(w, http.StatusOK, response{
		Data: jobs,
		Meta: &meta{Page: page, Limit: limit, Total: total},
	})
}

// extractJobIDFromDependentsKey extracts the job ID from a dependents key.
// Key format: "prefix:job:{id}:dependents" or "prefixjob:{id}:dependents".
// The prefix includes the trailing separator already via m.key().
func extractJobIDFromDependentsKey(key, prefix string) string {
	// m.key("job", "*", "dependents") produces "prefix:job:*:dependents"
	// so the actual key is "prefix:job:{id}:dependents"
	// We need to strip "prefix:job:" from the front and ":dependents" from the end.
	pfx := prefix + "job:"
	sfx := ":dependents"
	if len(key) <= len(pfx)+len(sfx) {
		return ""
	}
	if key[:len(pfx)] != pfx {
		return ""
	}
	if key[len(key)-len(sfx):] != sfx {
		return ""
	}
	return key[len(pfx) : len(key)-len(sfx)]
}

// handleDAGGraph returns the BFS graph traversal from a given job.
//
//	GET /api/v1/dag/graph/{id}?depth=10&max_nodes=50
func (m *Monitor) handleDAGGraph(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobID := r.PathValue("id")
	if !validatePathParam(w, "id", jobID) {
		return
	}

	maxDepth := queryInt(r, "depth", 10)
	if maxDepth < 1 {
		maxDepth = 1
	}
	if maxDepth > 20 {
		maxDepth = 20
	}

	maxNodes := queryInt(r, "max_nodes", 50)
	if maxNodes < 1 {
		maxNodes = 1
	}
	if maxNodes > 200 {
		maxNodes = 200
	}

	nodes, edges, truncated := m.bfsGraph(ctx, jobID, maxDepth, maxNodes)

	writeJSON(w, http.StatusOK, response{
		Data: dagGraphData{
			RootID:    jobID,
			Nodes:     nodes,
			Edges:     edges,
			Truncated: truncated,
		},
	})
}

// bfsGraph performs a BFS traversal from the given job ID, exploring both
// parents (via depends_on) and children (via dependents set).
func (m *Monitor) bfsGraph(ctx context.Context, rootID string, maxDepth, maxNodes int) ([]dagNode, []dagEdge, bool) {
	visited := make(map[string]bool)
	var nodes []dagNode
	var edges []dagEdge
	truncated := false

	// Queue for BFS: each entry is (jobID, currentDepth).
	type bfsEntry struct {
		id    string
		depth int
	}
	queue := []bfsEntry{{id: rootID, depth: 0}}
	visited[rootID] = true

	for len(queue) > 0 {
		// Process level by level via pipeline.
		var level []bfsEntry
		nextQueue := []bfsEntry{}

		// Collect all entries at the same depth.
		currentDepth := queue[0].depth
		for _, entry := range queue {
			if entry.depth == currentDepth {
				level = append(level, entry)
			} else {
				nextQueue = append(nextQueue, entry)
			}
		}
		queue = nextQueue

		if len(nodes)+len(level) > maxNodes {
			truncated = true
			// Add as many as we can.
			remaining := maxNodes - len(nodes)
			level = level[:remaining]
		}

		// Pipeline: HGETALL + SMEMBERS dependents for each node in this level.
		pipe := m.rdb.Pipeline()
		type levelCmd struct {
			id       string
			depth    int
			hget     *redis.MapStringStringCmd
			deptsCmd *redis.StringSliceCmd
		}
		cmds := make([]levelCmd, len(level))
		for i, entry := range level {
			cmds[i] = levelCmd{
				id:       entry.id,
				depth:    entry.depth,
				hget:     pipe.HGetAll(ctx, m.key("job", entry.id)),
				deptsCmd: pipe.SMembers(ctx, m.key("job", entry.id, "dependents")),
			}
		}
		pipe.Exec(ctx)

		for _, cmd := range cmds {
			data, _ := cmd.hget.Result()
			node := dagNode{ID: cmd.id}

			if len(data) == 0 {
				// Job doesn't exist — placeholder.
				node.Status = "unknown"
			} else {
				node.Type = data["type"]
				node.Status = data["status"]
				node.Queue = data["queue"]
				node.AllowFailure = data["allow_failure"] == "true" || data["allow_failure"] == "1"
				node.CreatedAt = data["created_at"]
			}
			nodes = append(nodes, node)

			if truncated {
				continue
			}

			// Explore parents (via depends_on in the job hash).
			if depsRaw, ok := data["depends_on"]; ok && depsRaw != "" {
				var parentIDs []string
				if err := json.Unmarshal([]byte(depsRaw), &parentIDs); err != nil {
					m.logger.Warn("dag: failed to parse depends_on", "job_id", cmd.id, "error", err)
				} else {
					for _, pid := range parentIDs {
						edges = append(edges, dagEdge{Source: pid, Target: cmd.id})
						if !visited[pid] && cmd.depth+1 <= maxDepth && len(nodes)+len(queue)+len(nextQueue) < maxNodes {
							visited[pid] = true
							nextQueue = append(nextQueue, bfsEntry{id: pid, depth: cmd.depth + 1})
						}
					}
				}
			}

			// Explore children (via dependents set).
			children, _ := cmd.deptsCmd.Result()
			for _, childID := range children {
				edges = append(edges, dagEdge{Source: cmd.id, Target: childID})
				if !visited[childID] && cmd.depth+1 <= maxDepth && len(nodes)+len(queue)+len(nextQueue) < maxNodes {
					visited[childID] = true
					nextQueue = append(nextQueue, bfsEntry{id: childID, depth: cmd.depth + 1})
				}
			}
		}

		if truncated {
			break
		}

		queue = append(nextQueue, queue...)
	}

	if nodes == nil {
		nodes = []dagNode{}
	}
	if edges == nil {
		edges = []dagEdge{}
	}

	// Deduplicate edges.
	edgeSet := make(map[string]bool)
	uniqueEdges := make([]dagEdge, 0, len(edges))
	for _, e := range edges {
		key := e.Source + "->" + e.Target
		if !edgeSet[key] {
			edgeSet[key] = true
			uniqueEdges = append(uniqueEdges, e)
		}
	}

	return nodes, uniqueEdges, truncated
}
