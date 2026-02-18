// Package tui provides a terminal UI for monitoring GQM queues.
package tui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is an HTTP client for the GQM monitoring API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new GQM API client.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// apiResponse is the standard JSON envelope from the GQM API.
type apiResponse struct {
	Data json.RawMessage `json:"data"`
}

// Queue represents a queue from the API.
type Queue struct {
	Name           string `json:"name"`
	Paused         bool   `json:"paused"`
	Ready          int64  `json:"ready"`
	Processing     int64  `json:"processing"`
	Completed      int64  `json:"completed"`
	DeadLetter     int64  `json:"dead_letter"`
	ProcessedTotal int64  `json:"processed_total"`
	FailedTotal    int64  `json:"failed_total"`
}

// Worker represents a worker/pool from the API (dynamic map from Redis hash).
type Worker map[string]any

func (w Worker) str(key string) string {
	if v, ok := w[key]; ok {
		switch t := v.(type) {
		case string:
			return t
		case float64:
			return fmt.Sprintf("%.0f", t)
		case []any:
			parts := make([]string, 0, len(t))
			for _, p := range t {
				if s, ok := p.(string); ok {
					parts = append(parts, s)
				}
			}
			return strings.Join(parts, ", ")
		}
	}
	return ""
}

// CronEntry represents a cron entry from the API (dynamic map).
type CronEntry map[string]any

func (e CronEntry) str(key string) string {
	if v, ok := e[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (e CronEntry) enabled() bool {
	if v, ok := e["enabled"]; ok {
		switch t := v.(type) {
		case bool:
			return t
		case string:
			return t == "true"
		}
	}
	return true // default enabled
}

// Job represents a job from the API (dynamic map from Redis hash).
type Job map[string]any

func (j Job) str(key string) string {
	if v, ok := j[key]; ok {
		switch t := v.(type) {
		case string:
			return t
		case float64:
			return fmt.Sprintf("%.0f", t)
		}
	}
	return ""
}

func (j Job) int64val(key string) int64 {
	if v, ok := j[key]; ok {
		switch t := v.(type) {
		case float64:
			return int64(t)
		case string:
			var n int64
			if _, err := fmt.Sscanf(t, "%d", &n); err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

// ListQueues fetches all queues.
func (c *Client) ListQueues() ([]Queue, error) {
	var queues []Queue
	if err := c.get("/api/v1/queues", &queues); err != nil {
		return nil, err
	}
	return queues, nil
}

// ListWorkers fetches all workers.
func (c *Client) ListWorkers() ([]Worker, error) {
	var workers []Worker
	if err := c.get("/api/v1/workers", &workers); err != nil {
		return nil, err
	}
	return workers, nil
}

// ListCron fetches all cron entries.
func (c *Client) ListCron() ([]CronEntry, error) {
	var entries []CronEntry
	if err := c.get("/api/v1/cron", &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// ListQueueJobs fetches jobs from a queue with optional status filter.
func (c *Client) ListQueueJobs(queue, status string) ([]Job, error) {
	path := fmt.Sprintf("/api/v1/queues/%s/jobs?status=%s", queue, status)
	var jobs []Job
	if err := c.get(path, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// ListDLQ fetches dead-letter jobs for a queue.
func (c *Client) ListDLQ(queue string) ([]Job, error) {
	path := fmt.Sprintf("/api/v1/queues/%s/dead-letter", queue)
	var jobs []Job
	if err := c.get(path, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// RetryJob triggers a retry for a failed job.
func (c *Client) RetryJob(jobID string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/retry", jobID)
	return c.post(path)
}

// PauseQueue pauses a queue.
func (c *Client) PauseQueue(queue string) error {
	return c.post(fmt.Sprintf("/api/v1/queues/%s/pause", queue))
}

// ResumeQueue resumes a paused queue.
func (c *Client) ResumeQueue(queue string) error {
	return c.post(fmt.Sprintf("/api/v1/queues/%s/resume", queue))
}

// TriggerCron triggers a cron entry immediately.
func (c *Client) TriggerCron(id string) error {
	return c.post(fmt.Sprintf("/api/v1/cron/%s/trigger", id))
}

// EnableCron enables a cron entry.
func (c *Client) EnableCron(id string) error {
	return c.post(fmt.Sprintf("/api/v1/cron/%s/enable", id))
}

// DisableCron disables a cron entry.
func (c *Client) DisableCron(id string) error {
	return c.post(fmt.Sprintf("/api/v1/cron/%s/disable", id))
}

// Health checks API connectivity.
func (c *Client) Health() error {
	return c.get("/health", nil)
}

func (c *Client) get(path string, result any) error {
	req, err := http.NewRequest("GET", c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized (check API key)")
	}
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("API error %d (failed to read body: %w)", resp.StatusCode, err)
		}
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	if result == nil {
		return nil
	}

	// API responses are wrapped in {"data": ...} envelope.
	var envelope apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, result); err != nil {
		return fmt.Errorf("decoding data: %w", err)
	}
	return nil
}

func (c *Client) post(path string) error {
	req, err := http.NewRequest("POST", c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized (check API key)")
	}
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("API error %d (failed to read body: %w)", resp.StatusCode, err)
		}
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) setHeaders(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
}
