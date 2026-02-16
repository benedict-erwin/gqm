package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

// --- Test helpers ---

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", testRedisAddr(), err)
	}
	return rdb
}

func testRedisAddr() string {
	return "localhost:6379"
}

func testPrefix(t *testing.T) string {
	return fmt.Sprintf("gqmtest:%s:", t.Name())
}

func testMonitor(t *testing.T, cfg Config) (*Monitor, *redis.Client) {
	t.Helper()
	rdb := testRedisClient(t)
	prefix := testPrefix(t)
	m := New(rdb, prefix, testLogger(), cfg)
	t.Cleanup(func() {
		// Clean up test keys
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		rdb.Close()
	})
	return m, rdb
}

func testLogger() *slog.Logger {
	return slog.Default()
}

func doRequest(m *Monitor, method, path string, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	return w
}

func doRequestWithAPIKey(m *Monitor, method, path, apiKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-API-Key", apiKey)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	return w
}

func doRequestWithCookie(m *Monitor, method, path, cookie string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Cookie", sessionCookieName+"="+cookie)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	return w
}

// --- Health endpoint ---

func TestHealth_OK(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/health", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("status = %v, want ok", resp["status"])
	}
	if resp["redis"] != true {
		t.Errorf("redis = %v, want true", resp["redis"])
	}
}

// --- Auth tests ---

func TestAuth_LoginSuccess(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)

	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: string(hash)},
		},
	})
	m.startedAt = time.Now()

	body := `{"username":"admin","password":"secret123"}`
	w := doRequest(m, "POST", "/auth/login", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["ok"] != true {
		t.Errorf("ok = %v", resp["ok"])
	}
	if resp["username"] != "admin" {
		t.Errorf("username = %v", resp["username"])
	}

	// Should have a Set-Cookie header
	cookies := w.Result().Cookies()
	found := false
	for _, c := range cookies {
		if c.Name == sessionCookieName {
			found = true
			if !c.HttpOnly {
				t.Error("cookie should be HttpOnly")
			}
		}
	}
	if !found {
		t.Error("session cookie not set")
	}
}

func TestAuth_LoginWrongPassword(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)

	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: string(hash)},
		},
	})
	m.startedAt = time.Now()

	body := `{"username":"admin","password":"wrong"}`
	w := doRequest(m, "POST", "/auth/login", body)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestAuth_LoginUnknownUser(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: "$2a$10$invalid"},
		},
	})
	m.startedAt = time.Now()

	body := `{"username":"unknown","password":"pass"}`
	w := doRequest(m, "POST", "/auth/login", body)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestAuth_MiddlewareBlocksWithoutAuth(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: "$2a$10$hash"},
		},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestAuth_APIKeyAuth(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: "$2a$10$x"}},
		APIKeys:        []AuthAPIKey{{Name: "test", Key: "gqm_ak_test123"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_test123")
	// Should not be 401 (might be 200 with empty queues)
	if w.Code == http.StatusUnauthorized {
		t.Fatalf("status = 401, API key should be accepted")
	}
}

func TestAuth_APIKeyInvalid(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: "$2a$10$x"}},
		APIKeys:        []AuthAPIKey{{Name: "test", Key: "gqm_ak_valid"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_invalid")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for invalid API key", w.Code)
	}
}

func TestAuth_SessionCookieAuth(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)

	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: string(hash)},
		},
	})
	m.startedAt = time.Now()

	// Login to get session
	loginBody := `{"username":"admin","password":"pass"}`
	loginW := doRequest(m, "POST", "/auth/login", loginBody)
	if loginW.Code != http.StatusOK {
		t.Fatalf("login status = %d", loginW.Code)
	}

	// Extract session cookie
	var sessionToken string
	for _, c := range loginW.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionToken = c.Value
			break
		}
	}
	if sessionToken == "" {
		t.Fatal("no session cookie returned")
	}

	// Verify session exists in Redis
	ctx := context.Background()
	username, err := rdb.Get(ctx, m.key("session", sessionToken)).Result()
	if err != nil {
		t.Fatalf("session not in Redis: %v", err)
	}
	if username != "admin" {
		t.Errorf("session username = %q", username)
	}

	// Use session cookie to access API
	w := doRequestWithCookie(m, "GET", "/api/v1/queues", sessionToken)
	if w.Code == http.StatusUnauthorized {
		t.Fatal("session cookie should authenticate")
	}
}

func TestAuth_Logout(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)

	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Login
	loginW := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"pass"}`)
	var sessionToken string
	for _, c := range loginW.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionToken = c.Value
		}
	}

	// Logout
	req := httptest.NewRequest("POST", "/auth/logout", nil)
	req.Header.Set("Cookie", sessionCookieName+"="+sessionToken)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("logout status = %d", w.Code)
	}

	// Session should be deleted from Redis
	ctx := context.Background()
	_, err := rdb.Get(ctx, m.key("session", sessionToken)).Result()
	if err != redis.Nil {
		t.Errorf("session should be deleted, got err=%v", err)
	}
}

func TestAuth_Me(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		APIKeys:     []AuthAPIKey{{Name: "mykey", Key: "gqm_ak_me"}},
		AuthUsers:   []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/auth/me", "gqm_ak_me")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["username"] != "apikey:mykey" {
		t.Errorf("username = %v", resp["username"])
	}
}

func TestAuth_NoAuthDisabled(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: false,
	})
	m.startedAt = time.Now()

	// All endpoints accessible without auth when disabled
	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code == http.StatusUnauthorized {
		t.Fatal("should not require auth when disabled")
	}
}

// --- Queue endpoints ---

func TestQueues_ListEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	// Data should be empty array
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("data len = %d, want 0", len(data))
	}
}

func TestQueues_ListWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Seed test data
	rdb.SAdd(ctx, m.key("queues"), "email", "default")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "job1", "job2")
	rdb.LPush(ctx, m.key("queue", "default", "ready"), "job3")

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 2 {
		t.Errorf("data len = %d, want 2", len(data))
	}
}

func TestQueues_GetNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestQueues_GetExisting(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "job1")

	w := doRequest(m, "GET", "/api/v1/queues/email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if data["name"] != "email" {
		t.Errorf("name = %v", data["name"])
	}
}

// --- Job endpoint ---

func TestJobs_GetNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/jobs/nonexistent-id", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestJobs_GetExisting(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("job", "job-123"),
		"id", "job-123",
		"type", "email.send",
		"queue", "default",
		"status", "completed",
		"payload", `{"to":"test@example.com"}`,
	)

	w := doRequest(m, "GET", "/api/v1/jobs/job-123", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if data["id"] != "job-123" {
		t.Errorf("id = %v", data["id"])
	}
	if data["type"] != "email.send" {
		t.Errorf("type = %v", data["type"])
	}
}

// --- Workers endpoint ---

func TestWorkers_ListEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/workers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestWorkers_GetNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/workers/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestWorkers_GetExisting(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("workers"), "email-pool")
	rdb.HSet(ctx, m.key("worker", "email-pool"),
		"id", "email-pool",
		"pool", "email-pool",
		"queues", "email,default",
		"status", "active",
		"concurrency", "5",
	)

	w := doRequest(m, "GET", "/api/v1/workers/email-pool", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if data["id"] != "email-pool" {
		t.Errorf("id = %v", data["id"])
	}
	// Queues should be split into array
	queues, ok := data["queues"].([]any)
	if !ok {
		t.Fatalf("queues type = %T", data["queues"])
	}
	if len(queues) != 2 {
		t.Errorf("queues len = %d, want 2", len(queues))
	}
}

// --- Stats endpoint ---

func TestStats_Overview(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "default")
	rdb.LPush(ctx, m.key("queue", "default", "ready"), "j1", "j2")
	rdb.Set(ctx, m.key("stats", "default", "processed_total"), "100", 0)
	rdb.Set(ctx, m.key("stats", "default", "failed_total"), "5", 0)

	w := doRequest(m, "GET", "/api/v1/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	// Check ready count (JSON numbers are float64)
	if ready, ok := data["ready"].(float64); !ok || ready != 2 {
		t.Errorf("ready = %v", data["ready"])
	}
	if pt, ok := data["processed_total"].(float64); !ok || pt != 100 {
		t.Errorf("processed_total = %v", data["processed_total"])
	}
}

func TestStats_Runtime(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/stats/runtime", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if _, ok := data["goroutines"]; !ok {
		t.Error("missing goroutines field")
	}
	if _, ok := data["go_version"]; !ok {
		t.Error("missing go_version field")
	}
}

func TestStats_Daily(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	today := time.Now().UTC().Format("2006-01-02")
	rdb.SAdd(ctx, m.key("queues"), "default")
	rdb.Set(ctx, m.key("stats", "default", "processed", today), "42", 0)
	rdb.Set(ctx, m.key("stats", "default", "failed", today), "3", 0)

	w := doRequest(m, "GET", "/api/v1/stats/daily?queue=default&days=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 1 {
		t.Fatalf("data len = %d, want 1", len(data))
	}
}

// --- Cron endpoint ---

func TestCron_ListEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestCron_GetNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestCron_GetExisting(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	entry := `{"id":"daily","name":"Daily Report","cron_expr":"0 0 3 * * *"}`
	rdb.HSet(ctx, m.key("cron", "entries"), "daily", entry)

	w := doRequest(m, "GET", "/api/v1/cron/daily", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if data["id"] != "daily" {
		t.Errorf("id = %v", data["id"])
	}
}

// --- Dashboard ---

func TestDashboard_Placeholder(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled:    true,
		DashPathPrefix: "/dashboard",
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("content-type = %q", ct)
	}
	if !strings.Contains(w.Body.String(), "GQM Dashboard") {
		t.Error("dashboard HTML missing expected content")
	}
}

// --- Pagination ---

func TestPagination_Defaults(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	page, limit := pagination(req)
	if page != 1 {
		t.Errorf("page = %d, want 1", page)
	}
	if limit != 20 {
		t.Errorf("limit = %d, want 20", limit)
	}
}

func TestPagination_Custom(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=3&limit=50", nil)
	page, limit := pagination(req)
	if page != 3 {
		t.Errorf("page = %d, want 3", page)
	}
	if limit != 50 {
		t.Errorf("limit = %d, want 50", limit)
	}
}

func TestPagination_Bounds(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=-1&limit=999", nil)
	page, limit := pagination(req)
	if page != 1 {
		t.Errorf("page = %d, want 1 (clamped)", page)
	}
	if limit != 100 {
		t.Errorf("limit = %d, want 100 (clamped)", limit)
	}
}

// --- DLQ endpoint ---

func TestDLQ_ListEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/default/dead-letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}
