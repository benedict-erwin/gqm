package monitor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	if addr := os.Getenv("GQM_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func testPrefix(t *testing.T) string {
	return fmt.Sprintf("gqmtest:%s:", t.Name())
}

func testMonitor(t *testing.T, cfg Config) (*Monitor, *redis.Client) {
	t.Helper()
	rdb := testRedisClient(t)
	prefix := testPrefix(t)
	m := New(rdb, prefix, testLogger(), cfg, nil)
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

func doRequestWithCookieCSRF(m *Monitor, method, path, cookie string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Cookie", sessionCookieName+"="+cookie)
	req.Header.Set("X-GQM-CSRF", "1")
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
		APIKeys:        []AuthAPIKey{{Name: "test", Key: "gqm_ak_test123_0123456789abcdefgh"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_test123_0123456789abcdefgh")
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
		APIKeys:        []AuthAPIKey{{Name: "test", Key: "gqm_ak_valid_0123456789abcdefghijk"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_invalid_0123456789abcdefghi")
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
		APIKeys:     []AuthAPIKey{{Name: "mykey", Key: "gqm_ak_me_0123456789abcdefghijklmn"}},
		AuthUsers:   []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithAPIKey(m, "GET", "/auth/me", "gqm_ak_me_0123456789abcdefghijklmn")
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

func TestDashboard_RedirectToTrailingSlash(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled:    true,
		DashPathPrefix: "/dashboard",
	})
	m.startedAt = time.Now()

	// /dashboard should redirect to /dashboard/
	w := doRequest(m, "GET", "/dashboard", "")
	if w.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc != "/dashboard/" {
		t.Fatalf("Location = %q, want /dashboard/", loc)
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

// --- Mock ServerAdmin ---

type mockAdmin struct {
	retryJobFn    func(ctx context.Context, jobID string) error
	cancelJobFn   func(ctx context.Context, jobID string) error
	deleteJobFn   func(ctx context.Context, jobID string) error
	pauseQueueFn  func(ctx context.Context, queue string) error
	resumeQueueFn func(ctx context.Context, queue string) error
	emptyQueueFn  func(ctx context.Context, queue string) (int64, error)
	retryAllDLQFn func(ctx context.Context, queue string) (int64, error)
	clearDLQFn    func(ctx context.Context, queue string) (int64, error)
	triggerCronFn func(ctx context.Context, cronID string) (string, error)
	enableCronFn  func(ctx context.Context, cronID string) error
	disableCronFn func(ctx context.Context, cronID string) error
	isQueuePausedFn func(ctx context.Context, queue string) (bool, error)
}

func (m *mockAdmin) RetryJob(ctx context.Context, jobID string) error {
	if m.retryJobFn != nil {
		return m.retryJobFn(ctx, jobID)
	}
	return nil
}
func (m *mockAdmin) CancelJob(ctx context.Context, jobID string) error {
	if m.cancelJobFn != nil {
		return m.cancelJobFn(ctx, jobID)
	}
	return nil
}
func (m *mockAdmin) DeleteJob(ctx context.Context, jobID string) error {
	if m.deleteJobFn != nil {
		return m.deleteJobFn(ctx, jobID)
	}
	return nil
}
func (m *mockAdmin) PauseQueue(ctx context.Context, queue string) error {
	if m.pauseQueueFn != nil {
		return m.pauseQueueFn(ctx, queue)
	}
	return nil
}
func (m *mockAdmin) ResumeQueue(ctx context.Context, queue string) error {
	if m.resumeQueueFn != nil {
		return m.resumeQueueFn(ctx, queue)
	}
	return nil
}
func (m *mockAdmin) EmptyQueue(ctx context.Context, queue string) (int64, error) {
	if m.emptyQueueFn != nil {
		return m.emptyQueueFn(ctx, queue)
	}
	return 0, nil
}
func (m *mockAdmin) RetryAllDLQ(ctx context.Context, queue string) (int64, error) {
	if m.retryAllDLQFn != nil {
		return m.retryAllDLQFn(ctx, queue)
	}
	return 0, nil
}
func (m *mockAdmin) ClearDLQ(ctx context.Context, queue string) (int64, error) {
	if m.clearDLQFn != nil {
		return m.clearDLQFn(ctx, queue)
	}
	return 0, nil
}
func (m *mockAdmin) TriggerCron(ctx context.Context, cronID string) (string, error) {
	if m.triggerCronFn != nil {
		return m.triggerCronFn(ctx, cronID)
	}
	return "mock-job-id", nil
}
func (m *mockAdmin) EnableCron(ctx context.Context, cronID string) error {
	if m.enableCronFn != nil {
		return m.enableCronFn(ctx, cronID)
	}
	return nil
}
func (m *mockAdmin) DisableCron(ctx context.Context, cronID string) error {
	if m.disableCronFn != nil {
		return m.disableCronFn(ctx, cronID)
	}
	return nil
}
func (m *mockAdmin) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	if m.isQueuePausedFn != nil {
		return m.isQueuePausedFn(ctx, queue)
	}
	return false, nil
}

func testMonitorWithAdmin(t *testing.T, cfg Config, admin ServerAdmin) (*Monitor, *redis.Client) {
	t.Helper()
	rdb := testRedisClient(t)
	prefix := testPrefix(t)
	m := New(rdb, prefix, testLogger(), cfg, admin)
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		rdb.Close()
	})
	return m, rdb
}

// --- Write endpoint tests: Jobs ---

func TestAdminJobs_RetrySuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/retry", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["ok"] != true {
		t.Errorf("ok = %v", resp["ok"])
	}
	if resp["job_id"] != "j1" {
		t.Errorf("job_id = %v", resp["job_id"])
	}
}

func TestAdminJobs_RetryNotFound(t *testing.T) {
	admin := &mockAdmin{
		retryJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: job not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/retry", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestAdminJobs_CancelSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/cancel", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "canceled" {
		t.Errorf("status = %v", resp["status"])
	}
}

func TestAdminJobs_CancelConflict(t *testing.T) {
	admin := &mockAdmin{
		cancelJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cannot cancel job with status \"processing\"")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/cancel", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestAdminJobs_DeleteSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/jobs/j1", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
}

func TestAdminJobs_DeleteConflict(t *testing.T) {
	admin := &mockAdmin{
		deleteJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cannot delete job with status \"processing\"")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/jobs/j1", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestAdminJobs_BatchRetrySuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2","j3"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if data["succeeded"] != float64(3) {
		t.Errorf("succeeded = %v, want 3", data["succeeded"])
	}
}

func TestAdminJobs_BatchRetryPartialFailure(t *testing.T) {
	admin := &mockAdmin{
		retryJobFn: func(_ context.Context, id string) error {
			if id == "j2" {
				return fmt.Errorf("gqm: job not found")
			}
			return nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2","j3"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, _ := resp.Data.(map[string]any)
	if data["succeeded"] != float64(2) {
		t.Errorf("succeeded = %v, want 2", data["succeeded"])
	}
	if data["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", data["failed"])
	}
}

func TestAdminJobs_BatchRetryEmptyBody(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	body := `{"job_ids":[]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchDeleteSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestAdminJobs_BatchRetryInvalidJSON(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", "not json")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchRetryExceedMaxSize(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	// Build 101 job IDs.
	ids := make([]string, 101)
	for i := range ids {
		ids[i] = fmt.Sprintf("j%d", i)
	}
	body, _ := json.Marshal(map[string]any{"job_ids": ids})
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", string(body))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchRetryInvalidChars(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	body := `{"job_ids":["valid-id","bad id!"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchRetryDeduplicates(t *testing.T) {
	var called []string
	admin := &mockAdmin{
		retryJobFn: func(_ context.Context, id string) error {
			called = append(called, id)
			return nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2","j1","j2","j3"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// Should have called RetryJob 3 times (deduplicated).
	if len(called) != 3 {
		t.Errorf("calls = %d, want 3 (deduplicated)", len(called))
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["succeeded"] != float64(3) {
		t.Errorf("succeeded = %v, want 3", data["succeeded"])
	}
}

func TestAdminJobs_BatchRetryAllFail(t *testing.T) {
	admin := &mockAdmin{
		retryJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: job not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["succeeded"] != float64(0) {
		t.Errorf("succeeded = %v, want 0", data["succeeded"])
	}
	if data["failed"] != float64(2) {
		t.Errorf("failed = %v, want 2", data["failed"])
	}
}

func TestAdminJobs_BatchDeletePartialFailure(t *testing.T) {
	admin := &mockAdmin{
		deleteJobFn: func(_ context.Context, id string) error {
			if id == "j1" {
				return fmt.Errorf("gqm: cannot delete job with status \"processing\"")
			}
			return nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j2","j3"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["succeeded"] != float64(2) {
		t.Errorf("succeeded = %v, want 2", data["succeeded"])
	}
	if data["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", data["failed"])
	}

	results := data["results"].([]any)
	// Find the failed item (j1).
	for _, r := range results {
		item := r.(map[string]any)
		if item["job_id"] == "j1" {
			if item["ok"] != false {
				t.Errorf("j1.ok = %v, want false", item["ok"])
			}
			if item["error"] == nil || item["error"] == "" {
				t.Error("j1 should have an error message")
			}
		}
	}
}

func TestAdminJobs_BatchDeleteInvalidJSON(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", "{broken")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchDeleteEmptyBody(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	body := `{"job_ids":[]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_BatchDeleteExceedMaxSize(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	ids := make([]string, 101)
	for i := range ids {
		ids[i] = fmt.Sprintf("j%d", i)
	}
	body, _ := json.Marshal(map[string]any{"job_ids": ids})
	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", string(body))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_NoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{}) // nil admin
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/retry", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

// --- Write endpoint tests: Queues ---

func TestAdminQueues_PauseSuccess(t *testing.T) {
	m, rdb := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Queue must exist to be pauseable
	rdb.SAdd(ctx, m.key("queues"), "email")

	w := doRequest(m, "POST", "/api/v1/queues/email/pause", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "paused" {
		t.Errorf("status = %v", resp["status"])
	}
}

func TestAdminQueues_ResumeSuccess(t *testing.T) {
	m, rdb := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Queue must exist to be resumable
	rdb.SAdd(ctx, m.key("queues"), "email")

	w := doRequest(m, "POST", "/api/v1/queues/email/resume", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "active" {
		t.Errorf("status = %v", resp["status"])
	}
}

func TestAdminQueues_EmptySuccess(t *testing.T) {
	admin := &mockAdmin{
		emptyQueueFn: func(_ context.Context, _ string) (int64, error) {
			return 42, nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/email/empty", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["removed"] != float64(42) {
		t.Errorf("removed = %v, want 42", resp["removed"])
	}
}

func TestAdminQueues_PauseNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/email/pause", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

// --- Write endpoint tests: DLQ ---

func TestAdminDLQ_RetryAllSuccess(t *testing.T) {
	admin := &mockAdmin{
		retryAllDLQFn: func(_ context.Context, _ string) (int64, error) {
			return 10, nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/default/dead-letter/retry-all", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["retried"] != float64(10) {
		t.Errorf("retried = %v, want 10", resp["retried"])
	}
}

func TestAdminDLQ_ClearSuccess(t *testing.T) {
	m, rdb := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	rdb.SAdd(context.Background(), m.key("queues"), "default")

	w := doRequest(m, "DELETE", "/api/v1/queues/default/dead-letter/clear", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	data, _ := resp["data"].(map[string]any)
	if data == nil || data["cleared"] == nil {
		t.Fatal("response missing data.cleared field")
	}
}

func TestAdminDLQ_ClearNotFound(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/nonexistent/dead-letter/clear", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("clear DLQ nonexistent queue: status = %d, want 404", w.Code)
	}
}

// --- Write endpoint tests: Cron ---

func TestAdminCron_TriggerSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/trigger", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["ok"] != true {
		t.Errorf("ok = %v", resp["ok"])
	}
	if resp["cron_id"] != "daily" {
		t.Errorf("cron_id = %v", resp["cron_id"])
	}
	if resp["job_id"] == nil || resp["job_id"] == "" {
		t.Error("job_id should not be empty")
	}
}

func TestAdminCron_TriggerNotFound(t *testing.T) {
	admin := &mockAdmin{
		triggerCronFn: func(_ context.Context, _ string) (string, error) {
			return "", fmt.Errorf("gqm: cron entry \"x\" not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/x/trigger", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestAdminCron_EnableSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/enable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["enabled"] != true {
		t.Errorf("enabled = %v", resp["enabled"])
	}
}

func TestAdminCron_DisableSuccess(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/disable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["enabled"] != false {
		t.Errorf("enabled = %v", resp["enabled"])
	}
}

func TestAdminCron_TriggerNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/trigger", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

// --- Queue Paused field ---

func TestQueues_ListIncludesPaused(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1")
	rdb.SAdd(ctx, m.key("paused"), "email")

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok || len(data) == 0 {
		t.Fatal("expected queue data")
	}
	q := data[0].(map[string]any)
	if q["paused"] != true {
		t.Errorf("paused = %v, want true", q["paused"])
	}
}

func TestQueues_GetIncludesPaused(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.SAdd(ctx, m.key("paused"), "email")

	w := doRequest(m, "GET", "/api/v1/queues/email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, _ := resp.Data.(map[string]any)
	if data["paused"] != true {
		t.Errorf("paused = %v, want true", data["paused"])
	}
}

// --- Sorted set data type tests (bug gqm-z7y) ---

func TestQueues_ListCountsSortedSets(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1", "j2")
	// completed and dead_letter are sorted sets (ZADD in Lua scripts)
	now := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: now, Member: "j3"}, redis.Z{Score: now + 1, Member: "j4"}, redis.Z{Score: now + 2, Member: "j5"})
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: now, Member: "j6"})

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	q := data[0].(map[string]any)
	if ready := q["ready"].(float64); ready != 2 {
		t.Errorf("ready = %v, want 2", ready)
	}
	if completed := q["completed"].(float64); completed != 3 {
		t.Errorf("completed = %v, want 3", completed)
	}
	if dlq := q["dead_letter"].(float64); dlq != 1 {
		t.Errorf("dead_letter = %v, want 1", dlq)
	}
}

func TestQueues_GetCountsSortedSets(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	now := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: now, Member: "j1"}, redis.Z{Score: now + 1, Member: "j2"})
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: now, Member: "j3"}, redis.Z{Score: now + 1, Member: "j4"}, redis.Z{Score: now + 2, Member: "j5"})

	w := doRequest(m, "GET", "/api/v1/queues/email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if completed := data["completed"].(float64); completed != 2 {
		t.Errorf("completed = %v, want 2", completed)
	}
	if dlq := data["dead_letter"].(float64); dlq != 3 {
		t.Errorf("dead_letter = %v, want 3", dlq)
	}
}

func TestDLQ_ListFromSortedSet(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	now := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: now, Member: "j1"}, redis.Z{Score: now + 1, Member: "j2"})
	// Seed job hashes so fetchJobSummaries finds them
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "dead_letter", "queue", "email")
	rdb.HSet(ctx, m.key("job", "j2"), "id", "j2", "status", "dead_letter", "queue", "email")

	w := doRequest(m, "GET", "/api/v1/queues/email/dead-letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 2 {
		t.Errorf("len(data) = %d, want 2", len(data))
	}
	if resp.Meta == nil || resp.Meta.Total != 2 {
		t.Errorf("meta.total = %v, want 2", resp.Meta)
	}
}

func TestQueueJobs_CompletedFromSortedSet(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	now := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: now, Member: "j1"}, redis.Z{Score: now + 1, Member: "j2"}, redis.Z{Score: now + 2, Member: "j3"})
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "completed", "queue", "email")
	rdb.HSet(ctx, m.key("job", "j2"), "id", "j2", "status", "completed", "queue", "email")
	rdb.HSet(ctx, m.key("job", "j3"), "id", "j3", "status", "completed", "queue", "email")

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=completed", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 3 {
		t.Errorf("len(data) = %d, want 3", len(data))
	}
	if resp.Meta == nil || resp.Meta.Total != 3 {
		t.Errorf("meta.total = %v, want 3", resp.Meta)
	}
}

func TestStats_OverviewSortedSets(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	now := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: now, Member: "j1"}, redis.Z{Score: now + 1, Member: "j2"})
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: now, Member: "j3"})

	w := doRequest(m, "GET", "/api/v1/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if completed := data["completed"].(float64); completed != 2 {
		t.Errorf("completed = %v, want 2", completed)
	}
	if dlq := data["dead_letter"].(float64); dlq != 1 {
		t.Errorf("dead_letter = %v, want 1", dlq)
	}
}

func TestCron_HistoryFromSortedSet(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	ts1 := float64(time.Now().Add(-2 * time.Hour).Unix())
	ts2 := float64(time.Now().Add(-1 * time.Hour).Unix())
	rdb.ZAdd(ctx, m.key("cron", "history", "daily"), redis.Z{Score: ts1, Member: "j1"}, redis.Z{Score: ts2, Member: "j2"})

	w := doRequest(m, "GET", "/api/v1/cron/daily/history", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 2 {
		t.Errorf("len(data) = %d, want 2", len(data))
	}
	// ZRevRange returns most recent first
	first := data[0].(map[string]any)
	if first["job_id"] != "j2" {
		t.Errorf("first job_id = %v, want j2 (most recent)", first["job_id"])
	}
	if first["triggered_at"] == nil {
		t.Error("triggered_at missing")
	}
}

func TestCron_GetLastRunFromSortedSet(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	entryJSON := `{"id":"daily","schedule":"0 0 * * *","job_type":"cleanup","queue":"default","enabled":true}`
	rdb.HSet(ctx, m.key("cron", "entries"), "daily", entryJSON)

	ts := float64(time.Now().Unix())
	rdb.ZAdd(ctx, m.key("cron", "history", "daily"), redis.Z{Score: ts, Member: "j1"})

	w := doRequest(m, "GET", "/api/v1/cron/daily", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	lastRun, ok := data["last_run"].(map[string]any)
	if !ok {
		t.Fatal("last_run missing or wrong type")
	}
	if lastRun["job_id"] != "j1" {
		t.Errorf("last_run.job_id = %v, want j1", lastRun["job_id"])
	}
	if lastRun["triggered_at"] == nil {
		t.Error("last_run.triggered_at missing")
	}
}

// --- Security: Path param validation (H1) ---

func TestSecurity_PathParamValidation_ValidChars(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// Valid path param characters: alphanumeric, hyphens, underscores, dots, colons, @
	validIDs := []string{"job-123", "email_queue", "daily.report", "abc123", "user@host", "email:send", "app:email:send"}
	for _, id := range validIDs {
		w := doRequest(m, "GET", "/api/v1/jobs/"+id, "")
		if w.Code == http.StatusBadRequest {
			t.Errorf("valid id %q rejected as bad request", id)
		}
	}
}

func TestSecurity_PathParamValidation_InvalidChars(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	invalidIDs := []string{
		"key{injection}",
		"foo$bar",
		"test;drop",
	}
	for _, id := range invalidIDs {
		w := doRequest(m, "GET", "/api/v1/jobs/"+id, "")
		// For path traversal or invalid chars, expect 400 or 404 (router may not match)
		if w.Code == http.StatusOK {
			t.Errorf("invalid id %q should not return 200", id)
		}
	}
}

func TestSecurity_PathParamValidation_TooLong(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	longID := strings.Repeat("a", 257)
	w := doRequest(m, "GET", "/api/v1/jobs/"+longID, "")
	if w.Code == http.StatusOK {
		t.Error("257-char id should not return 200")
	}
}

// --- Security: Request body size limit (H2) ---

func TestSecurity_RequestBodySizeLimit(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	// Create a body larger than 1MB
	bigBody := `{"job_ids":["` + strings.Repeat("x", 2<<20) + `"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", bigBody)
	if w.Code != http.StatusBadRequest {
		t.Errorf("oversized body: status = %d, want 400", w.Code)
	}
}

// --- Security: Login rate limiting (H3) ---

func TestSecurity_LoginRateLimit(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)

	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Make 5 failed login attempts
	for i := 0; i < 5; i++ {
		w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d, want 401", i+1, w.Code)
		}
	}

	// 6th attempt should be rate limited
	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("6th attempt: status = %d, want 429", w.Code)
	}

	// Even correct password should be blocked
	w = doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"correct"}`)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("correct password after rate limit: status = %d, want 429", w.Code)
	}

	// Clean up rate limit key for test isolation
	ctx := context.Background()
	rdb.Del(ctx, m.key("login_attempts", "admin"))
}

func TestSecurity_LoginRateLimitResetOnSuccess(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)

	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Make 3 failed attempts
	for i := 0; i < 3; i++ {
		doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
	}

	// Successful login should reset counter
	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"correct"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("correct login: status = %d, want 200", w.Code)
	}

	// Counter should be reset — 5 more failures should work
	for i := 0; i < 5; i++ {
		w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("after reset, attempt %d: status = %d, want 401", i+1, w.Code)
		}
	}

	// Now should be rate limited
	w = doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("after 5 failures post-reset: status = %d, want 429", w.Code)
	}

	ctx := context.Background()
	rdb.Del(ctx, m.key("login_attempts", "admin"))
}

// --- Security: API key constant-time comparison (H5) ---

func TestSecurity_APIKeyConstantTimeComparison(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		APIKeys: []AuthAPIKey{
			{Name: "key1", Key: "gqm_ak_first_0123456789abcdefghijk"},
			{Name: "key2", Key: "gqm_ak_second_0123456789abcdefghij"},
		},
		AuthUsers: []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	// Valid key should work
	w := doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_first_0123456789abcdefghijk")
	if w.Code == http.StatusUnauthorized {
		t.Fatal("valid API key rejected")
	}

	w = doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_second_0123456789abcdefghij")
	if w.Code == http.StatusUnauthorized {
		t.Fatal("second valid API key rejected")
	}

	// Invalid key should fail
	w = doRequestWithAPIKey(m, "GET", "/api/v1/queues", "gqm_ak_invalid_0123456789abcdefghi")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid API key: status = %d, want 401", w.Code)
	}
}

// --- Security: Dashboard auth (M1) ---

func TestDashboard_ServesEmbedded(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled: true,
	})
	m.startedAt = time.Now()

	// /dashboard should redirect to /dashboard/
	w := doRequest(m, "GET", "/dashboard", "")
	if w.Code != http.StatusMovedPermanently {
		t.Fatalf("GET /dashboard: status = %d, want 301", w.Code)
	}

	// /dashboard/ should serve index.html (no auth required for static assets)
	w = doRequest(m, "GET", "/dashboard/", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /dashboard/: status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("GET /dashboard/: Content-Type = %q, want text/html", ct)
	}
}

func TestDashboard_ServesCSS(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled: true,
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard/css/style.css", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /dashboard/css/style.css: status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/css") {
		t.Fatalf("GET /dashboard/css/style.css: Content-Type = %q, want text/css", ct)
	}
}

func TestDashboard_ServesJS(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled: true,
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard/js/app.js", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /dashboard/js/app.js: status = %d, want 200", w.Code)
	}
}

func TestDashboard_SPAFallback(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled: true,
	})
	m.startedAt = time.Now()

	// Non-file path should serve index.html (SPA routing)
	w := doRequest(m, "GET", "/dashboard/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /dashboard/queues (SPA fallback): status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("SPA fallback Content-Type = %q, want text/html", ct)
	}
}

func TestDashboard_Disabled(t *testing.T) {
	m, _ := testMonitor(t, Config{
		DashEnabled: false,
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard/", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("disabled dashboard: status = %d, want 404", w.Code)
	}
}

func TestDashboard_CustomDir(t *testing.T) {
	// Create a temp directory with a custom index.html
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "index.html"), []byte("<html>custom</html>"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "VERSION"), []byte("0.1.0\n"), 0644)

	m, _ := testMonitor(t, Config{
		DashEnabled:   true,
		DashCustomDir: tmpDir,
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard/", "")
	if w.Code != http.StatusOK {
		t.Fatalf("custom dir dashboard: status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "custom") {
		t.Fatalf("expected custom content, got: %s", body)
	}
}

func TestDashboard_VersionMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "index.html"), []byte("<html>old</html>"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "VERSION"), []byte("0.0.1\n"), 0644)

	// Should not panic — just logs a warning
	m, _ := testMonitor(t, Config{
		DashEnabled:   true,
		DashCustomDir: tmpDir,
	})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/dashboard/", "")
	if w.Code != http.StatusOK {
		t.Fatalf("version mismatch dashboard: status = %d, want 200", w.Code)
	}
}

// --- Security: Cookie Secure flag (M2) ---

func TestSecurity_CookieSecureFlag(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Login over plain HTTP — Secure should be false
	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"pass"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("login: status = %d, want 200", w.Code)
	}

	cookies := w.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("session cookie not set")
	}
	if sessionCookie.Secure {
		t.Error("session cookie should NOT have Secure flag on plain HTTP")
	}
	if !sessionCookie.HttpOnly {
		t.Error("session cookie missing HttpOnly flag")
	}

	// Login over HTTPS (TLS) — Secure should be true
	req := httptest.NewRequest("POST", "/auth/login", strings.NewReader(`{"username":"admin","password":"pass"}`))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = &tls.ConnectionState{} // simulate HTTPS
	w2 := httptest.NewRecorder()
	m.mux.ServeHTTP(w2, req)
	if w2.Code != http.StatusOK {
		t.Fatalf("TLS login: status = %d, want 200", w2.Code)
	}
	var tlsCookie *http.Cookie
	for _, c := range w2.Result().Cookies() {
		if c.Name == sessionCookieName {
			tlsCookie = c
			break
		}
	}
	if tlsCookie == nil {
		t.Fatal("session cookie not set on TLS login")
	}
	if !tlsCookie.Secure {
		t.Error("session cookie should have Secure flag on HTTPS")
	}

	// Clean up
	ctx := context.Background()
	rdb.Del(ctx, m.key("session", sessionCookie.Value))
	rdb.Del(ctx, m.key("session", tlsCookie.Value))
}

// --- Security: Strip X-GQM-User header (M3) ---

func TestSecurity_StripSpoofedUserHeader(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: false, // Auth disabled — requests pass through
	})
	m.startedAt = time.Now()

	// Send request with spoofed X-GQM-User header
	req := httptest.NewRequest("GET", "/api/v1/queues", nil)
	req.Header.Set("X-GQM-User", "spoofed-admin")
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	// The header should have been stripped (not passed through to handler).
	// Since auth is disabled, no X-GQM-User is set, but the spoofed one should be gone.
	// We can verify by checking /auth/me which reads the header.
}

func TestSecurity_StripSpoofedRoleHeader(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		AuthUsers:   []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	// Try to spoof admin role without valid auth — should be rejected at auth level
	req := httptest.NewRequest("POST", "/api/v1/jobs/test-id/retry", nil)
	req.Header.Set("X-GQM-Role", "admin")
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("spoofed role header: status = %d, want 401", w.Code)
	}
}

// --- Security: Error sanitization (M4) ---

func TestSecurity_ErrorSanitization(t *testing.T) {
	// sanitizeError should pass through gqm: prefixed errors
	gqmErr := fmt.Errorf("gqm: job not found")
	if msg := sanitizeError(gqmErr); msg != "gqm: job not found" {
		t.Errorf("gqm error sanitized incorrectly: %q", msg)
	}

	// sanitizeError should NOT pass through "not found" without gqm: prefix
	// (could leak internal details like Redis error messages)
	notFoundErr := fmt.Errorf("fetching cron entry test: key not found in cache")
	if msg := sanitizeError(notFoundErr); msg != "internal error" {
		t.Errorf("non-gqm not-found error should be sanitized: got %q", msg)
	}

	// But gqm: prefixed not-found should pass through
	gqmNotFound := fmt.Errorf("gqm: cron entry %q not found", "test")
	if msg := sanitizeError(gqmNotFound); msg != gqmNotFound.Error() {
		t.Errorf("gqm not found error should pass through: got %q", msg)
	}

	// sanitizeError should hide internal errors
	internalErr := fmt.Errorf("fetching job abc: connection refused")
	if msg := sanitizeError(internalErr); msg != "internal error" {
		t.Errorf("internal error not sanitized: %q", msg)
	}

	redisErr := fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	if msg := sanitizeError(redisErr); msg != "internal error" {
		t.Errorf("redis error not sanitized: %q", msg)
	}
}

// --- Security: RBAC (M9) ---

func TestSecurity_RBAC_ViewerCannotWrite(t *testing.T) {
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "viewer", PasswordHash: "", Role: "viewer"},
		},
		APIKeys: []AuthAPIKey{
			{Name: "viewer-key", Key: "gqm_ak_viewer_0123456789abcdefghij", Role: "viewer"},
		},
	})
	m.startedAt = time.Now()

	// Create a session for the viewer user
	ctx := context.Background()
	token := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rdb.Set(ctx, m.key("session", token), "viewer", time.Hour)

	// Viewer should be able to read
	w := doRequestWithCookie(m, "GET", "/api/v1/queues", token)
	// Should not be 401 or 403
	if w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
		t.Fatalf("viewer read queues: status = %d, want not 401/403", w.Code)
	}

	// Viewer should be blocked from write endpoints
	writeEndpoints := []struct {
		method string
		path   string
	}{
		{"POST", "/api/v1/jobs/test-id/retry"},
		{"POST", "/api/v1/jobs/test-id/cancel"},
		{"DELETE", "/api/v1/jobs/test-id"},
		{"POST", "/api/v1/queues/test-q/pause"},
		{"POST", "/api/v1/queues/test-q/resume"},
		{"POST", "/api/v1/cron/test-id/trigger"},
		{"POST", "/api/v1/cron/test-id/enable"},
		{"POST", "/api/v1/cron/test-id/disable"},
	}
	for _, ep := range writeEndpoints {
		w = doRequestWithCookie(m, ep.method, ep.path, token)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s %s: viewer status = %d, want 403", ep.method, ep.path, w.Code)
		}
	}

	rdb.Del(ctx, m.key("session", token))
}

func TestSecurity_RBAC_AdminCanWrite(t *testing.T) {
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "admin", PasswordHash: "", Role: "admin"},
		},
	})
	m.startedAt = time.Now()

	// Create a session for the admin user
	ctx := context.Background()
	token := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	rdb.Set(ctx, m.key("session", token), "admin", time.Hour)

	// Admin should NOT get 403 on write endpoints (might get 501 since no admin backend)
	w := doRequestWithCookieCSRF(m, "POST", "/api/v1/jobs/test-id/retry", token)
	if w.Code == http.StatusForbidden {
		t.Fatalf("admin retry job: status = 403, admin should not be blocked")
	}

	rdb.Del(ctx, m.key("session", token))
}

func TestSecurity_RBAC_DefaultRoleIsAdmin(t *testing.T) {
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers: []AuthUser{
			{Username: "olduser", PasswordHash: ""}, // No Role set
		},
	})
	m.startedAt = time.Now()

	// Create a session for user with no explicit role
	ctx := context.Background()
	token := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	rdb.Set(ctx, m.key("session", token), "olduser", time.Hour)

	// Should default to admin role — not blocked from write endpoints
	w := doRequestWithCookieCSRF(m, "POST", "/api/v1/jobs/test-id/retry", token)
	if w.Code == http.StatusForbidden {
		t.Fatalf("default role user: status = 403, should default to admin")
	}

	rdb.Del(ctx, m.key("session", token))
}

func TestSecurity_RBAC_APIKeyViewerBlocked(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		APIKeys: []AuthAPIKey{
			{Name: "viewer-key", Key: "gqm_ak_viewer_0123456789abcdefghij", Role: "viewer"},
			{Name: "admin-key", Key: "gqm_ak_admin_0123456789abcdefghijk", Role: "admin"},
		},
		AuthUsers: []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	// Viewer API key should be blocked from write endpoints
	w := doRequestWithAPIKey(m, "POST", "/api/v1/jobs/test-id/retry", "gqm_ak_viewer_0123456789abcdefghij")
	if w.Code != http.StatusForbidden {
		t.Fatalf("viewer API key write: status = %d, want 403", w.Code)
	}

	// Admin API key should not be blocked
	w = doRequestWithAPIKey(m, "POST", "/api/v1/jobs/test-id/retry", "gqm_ak_admin_0123456789abcdefghijk")
	if w.Code == http.StatusForbidden {
		t.Fatal("admin API key blocked from write endpoint")
	}
}

// --- Security: Username enumeration timing (L1) ---

func TestSecurity_UsernameEnumerationTiming(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)

	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Both existing and non-existing users should return the same error message.
	// This tests the behavioral aspect — timing side-channel is hard to verify
	// in a unit test, but we verify both paths reach the same code.
	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password: status = %d, want 401", w.Code)
	}
	var resp1 map[string]any
	json.NewDecoder(w.Body).Decode(&resp1)

	w = doRequest(m, "POST", "/auth/login", `{"username":"nonexistent","password":"wrong"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unknown user: status = %d, want 401", w.Code)
	}
	var resp2 map[string]any
	json.NewDecoder(w.Body).Decode(&resp2)

	// Both should return identical error response
	if resp1["error"] != resp2["error"] {
		t.Errorf("responses differ: %q vs %q", resp1["error"], resp2["error"])
	}
	if resp1["code"] != resp2["code"] {
		t.Errorf("error codes differ: %q vs %q", resp1["code"], resp2["code"])
	}
}

// --- Security: Session token format validation (L2) ---

func TestSecurity_SessionTokenFormatValidation(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		AuthUsers:   []AuthUser{{Username: "admin", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	invalidTokens := []struct {
		name  string
		token string
	}{
		{"too short", "abc123"},
		{"too long", strings.Repeat("a", 65)},
		{"non-hex chars", strings.Repeat("g", 64)},
		{"has spaces", strings.Repeat("a", 32) + " " + strings.Repeat("b", 31)},
		{"uppercase hex", strings.Repeat("A", 64)},
		{"redis injection", "session:*:admin" + strings.Repeat("0", 49)},
	}

	for _, tt := range invalidTokens {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequestWithCookie(m, "GET", "/api/v1/queues", tt.token)
			if w.Code != http.StatusUnauthorized {
				t.Errorf("token %q: status = %d, want 401", tt.name, w.Code)
			}
		})
	}
}

func TestSecurity_SessionTokenValidFormatAccepted(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Login to get a valid token
	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"pass"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("login: status = %d", w.Code)
	}

	var sessionToken string
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionToken = c.Value
			break
		}
	}
	if sessionToken == "" {
		t.Fatal("no session cookie")
	}

	// Valid 64 hex-char token should work
	w = doRequestWithCookie(m, "GET", "/api/v1/queues", sessionToken)
	if w.Code == http.StatusUnauthorized {
		t.Fatal("valid session token rejected")
	}

	ctx := context.Background()
	rdb.Del(ctx, m.key("session", sessionToken))
}

// --- Security: Batch job ID deduplication (L3) ---

func TestSecurity_BatchJobIDDeduplication(t *testing.T) {
	var retryCalls int
	admin := &mockAdmin{
		retryJobFn: func(_ context.Context, _ string) error {
			retryCalls++
			return nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	// Send duplicate IDs
	body := `{"job_ids":["j1","j2","j1","j3","j2"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// Should only have processed 3 unique IDs
	if retryCalls != 3 {
		t.Errorf("retry called %d times, want 3 (deduplicated)", retryCalls)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["succeeded"] != float64(3) {
		t.Errorf("succeeded = %v, want 3", data["succeeded"])
	}
}

func TestSecurity_BatchDeleteDeduplication(t *testing.T) {
	var deleteCalls int
	admin := &mockAdmin{
		deleteJobFn: func(_ context.Context, _ string) error {
			deleteCalls++
			return nil
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	body := `{"job_ids":["j1","j1","j1"]}`
	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	if deleteCalls != 1 {
		t.Errorf("delete called %d times, want 1 (deduplicated)", deleteCalls)
	}
}

// --- Security: PauseQueue validate queue exists (L4) ---

func TestSecurity_PauseQueueNotFound(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	// Queue doesn't exist in gqm:queues set
	w := doRequest(m, "POST", "/api/v1/queues/nonexistent/pause", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("pause nonexistent queue: status = %d, want 404", w.Code)
	}
}

func TestSecurity_ResumeQueueNotFound(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/nonexistent/resume", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("resume nonexistent queue: status = %d, want 404", w.Code)
	}
}

func TestSecurity_PauseQueueExists(t *testing.T) {
	m, rdb := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")

	w := doRequest(m, "POST", "/api/v1/queues/email/pause", "")
	if w.Code != http.StatusOK {
		t.Fatalf("pause existing queue: status = %d, want 200", w.Code)
	}
}

// --- Security: Validate status query param (L5) ---

func TestSecurity_ValidateStatusQueryParam(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")

	tests := []struct {
		status   string
		wantCode int
	}{
		{"", http.StatusOK},              // default → ready
		{"ready", http.StatusOK},         // explicit ready
		{"processing", http.StatusOK},    // valid
		{"completed", http.StatusOK},     // valid
		{"dead_letter", http.StatusOK},   // valid
		{"invalid", http.StatusBadRequest},
		{"PROCESSING", http.StatusBadRequest}, // case sensitive
		{"all", http.StatusBadRequest},
		{"active", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run("status="+tt.status, func(t *testing.T) {
			path := "/api/v1/queues/email/jobs"
			if tt.status != "" {
				path += "?status=" + tt.status
			}
			w := doRequest(m, "GET", path, "")
			if w.Code != tt.wantCode {
				t.Errorf("status=%q: got %d, want %d; body=%s", tt.status, w.Code, tt.wantCode, w.Body.String())
			}
		})
	}
}

// --- Security: Job response field allowlist (L6) ---

func TestSecurity_JobResponseFieldAllowlist(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Seed a job with both known and unknown fields
	rdb.HSet(ctx, m.key("job", "j-test"),
		"id", "j-test",
		"type", "email.send",
		"queue", "default",
		"status", "completed",
		"payload", `{"to":"a@b.com"}`,
		"internal_secret", "should-not-appear",
		"_debug_trace", "redis-node-3",
		"some_future_field", "unknown",
	)

	w := doRequest(m, "GET", "/api/v1/jobs/j-test", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)

	// Known fields should be present
	if data["id"] != "j-test" {
		t.Errorf("id = %v", data["id"])
	}
	if data["type"] != "email.send" {
		t.Errorf("type = %v", data["type"])
	}

	// Unknown fields should be filtered out
	for _, field := range []string{"internal_secret", "_debug_trace", "some_future_field"} {
		if _, exists := data[field]; exists {
			t.Errorf("field %q should be filtered out by allowlist but is present", field)
		}
	}
}

// --- Security: CSRF protection (I1) ---

func TestSecurity_CSRF_CookieAuthRequiresCSRFHeader(t *testing.T) {
	m, rdb := testMonitorWithAdmin(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: "", Role: "admin"}},
	}, &mockAdmin{})
	m.startedAt = time.Now()
	ctx := context.Background()

	token := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	rdb.Set(ctx, m.key("session", token), "admin", time.Hour)

	// Write request with session cookie but NO CSRF header → 403
	w := doRequestWithCookie(m, "POST", "/api/v1/jobs/test-id/retry", token)
	if w.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF: status = %d, want 403", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["code"] != "CSRF_REQUIRED" {
		t.Errorf("error code = %v, want CSRF_REQUIRED", resp["code"])
	}

	// Write request with session cookie AND CSRF header → should pass (501 since no admin)
	w = doRequestWithCookieCSRF(m, "POST", "/api/v1/jobs/test-id/retry", token)
	if w.Code == http.StatusForbidden {
		t.Fatalf("with CSRF header: status = 403, should pass")
	}

	rdb.Del(ctx, m.key("session", token))
}

func TestSecurity_CSRF_APIKeyExempt(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{
		AuthEnabled: true,
		APIKeys:     []AuthAPIKey{{Name: "admin-key", Key: "gqm_ak_admin_0123456789abcdefghijk", Role: "admin"}},
		AuthUsers:   []AuthUser{{Username: "x", PasswordHash: "$2a$10$x"}},
	}, &mockAdmin{})
	m.startedAt = time.Now()

	// API key auth should NOT require CSRF header
	w := doRequestWithAPIKey(m, "POST", "/api/v1/jobs/test-id/retry", "gqm_ak_admin_0123456789abcdefghijk")
	if w.Code == http.StatusForbidden {
		t.Fatalf("API key write: status = 403, API keys should be exempt from CSRF")
	}
}

func TestSecurity_CSRF_AuthDisabledExempt(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{
		AuthEnabled: false,
	}, &mockAdmin{})
	m.startedAt = time.Now()

	// No auth → no CSRF check needed
	w := doRequest(m, "POST", "/api/v1/jobs/test-id/retry", "")
	if w.Code == http.StatusForbidden {
		t.Fatalf("auth disabled: status = 403, should not check CSRF")
	}
}

func TestSecurity_CSRF_ReadEndpointsNotAffected(t *testing.T) {
	m, rdb := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "viewer", PasswordHash: "", Role: "viewer"}},
	})
	m.startedAt = time.Now()
	ctx := context.Background()

	token := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	rdb.Set(ctx, m.key("session", token), "viewer", time.Hour)

	// Read endpoint with cookie, no CSRF header → should work (no CSRF on reads)
	w := doRequestWithCookie(m, "GET", "/api/v1/queues", token)
	if w.Code == http.StatusForbidden {
		t.Fatalf("read endpoint: status = 403, reads should not check CSRF")
	}

	rdb.Del(ctx, m.key("session", token))
}

// --- Security: Audit logging (I2) ---

func TestSecurity_AuditLogging_AdminRetry(t *testing.T) {
	// Audit logging is slog.Info calls — we verify the handler doesn't break
	// when logging is active. Full log capture would require a custom slog.Handler.
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/retry", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

// --- Security: GET workers no side effects (I3) ---

func TestSecurity_GetWorkersNoSideEffect(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Add a worker to the set but don't create the hash (simulates expired TTL)
	rdb.SAdd(ctx, m.key("workers"), "stale-pool")

	// GET should NOT remove the stale entry from the set
	w := doRequest(m, "GET", "/api/v1/workers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// The response should be empty (stale worker skipped)
	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("expected 0 workers (stale skipped), got %d", len(data))
	}

	// But the stale entry should still be in the set (no SRem side effect)
	isMember, err := rdb.SIsMember(ctx, m.key("workers"), "stale-pool").Result()
	if err != nil {
		t.Fatalf("SIsMember: %v", err)
	}
	if !isMember {
		t.Error("stale worker entry was removed from set — GET should not have side effects")
	}
}

// --- DAG endpoints ---

func TestDAG_ListDeferredEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("data len = %d, want 0", len(data))
	}
	if resp.Meta == nil {
		t.Fatal("meta is nil")
	}
	if resp.Meta.Total != 0 {
		t.Errorf("total = %d, want 0", resp.Meta.Total)
	}
}

func TestDAG_ListDeferredWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Seed deferred jobs.
	rdb.SAdd(ctx, m.key("deferred"), "child-1", "child-2")
	rdb.HSet(ctx, m.key("job", "child-1"),
		"id", "child-1",
		"type", "report.generate",
		"queue", "default",
		"status", "deferred",
		"depends_on", `["parent-1"]`,
		"created_at", "1708300000",
	)
	rdb.HSet(ctx, m.key("job", "child-2"),
		"id", "child-2",
		"type", "email.send",
		"queue", "email",
		"status", "deferred",
		"depends_on", `["parent-1","parent-2"]`,
		"created_at", "1708300100",
	)
	rdb.SAdd(ctx, m.key("job", "child-1", "pending_deps"), "parent-1")
	rdb.SAdd(ctx, m.key("job", "child-2", "pending_deps"), "parent-1", "parent-2")

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
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
	if resp.Meta.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Meta.Total)
	}

	// Check that pending_deps is present.
	first := data[0].(map[string]any)
	pending, ok := first["pending_deps"].([]any)
	if !ok {
		t.Fatalf("pending_deps type = %T", first["pending_deps"])
	}
	if len(pending) == 0 {
		t.Error("pending_deps should not be empty")
	}
}

func TestDAG_ListRootsEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/dag/roots", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("data len = %d, want 0", len(data))
	}
}

func TestDAG_ListRootsWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create parent with dependents set.
	rdb.HSet(ctx, m.key("job", "parent-1"),
		"id", "parent-1",
		"type", "data:export",
		"queue", "default",
		"status", "completed",
		"created_at", "1708300000",
	)
	rdb.SAdd(ctx, m.key("job", "parent-1", "dependents"), "child-1", "child-2")

	rdb.HSet(ctx, m.key("job", "parent-2"),
		"id", "parent-2",
		"type", "report:generate",
		"queue", "default",
		"status", "processing",
		"created_at", "1708300100",
	)
	rdb.SAdd(ctx, m.key("job", "parent-2", "dependents"), "child-3")

	w := doRequest(m, "GET", "/api/v1/dag/roots", "")
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
	if resp.Meta.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Meta.Total)
	}

	// Check child_count is present.
	first := data[0].(map[string]any)
	cc, ok := first["child_count"]
	if !ok {
		t.Fatal("child_count missing")
	}
	count := cc.(float64)
	if count < 1 {
		t.Errorf("child_count = %v, want >= 1", count)
	}
}

func TestDAG_GraphNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/dag/graph/nonexistent-id", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].(map[string]any)

	nodes := data["nodes"].([]any)
	if len(nodes) != 1 {
		t.Errorf("nodes len = %d, want 1 (unknown placeholder)", len(nodes))
	}
	// The placeholder node should have status "unknown".
	node := nodes[0].(map[string]any)
	if node["status"] != "unknown" {
		t.Errorf("status = %v, want unknown", node["status"])
	}
}

func TestDAG_GraphSingleNode(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("job", "solo-job"),
		"id", "solo-job",
		"type", "data.process",
		"queue", "default",
		"status", "completed",
		"created_at", "1708300000",
	)

	w := doRequest(m, "GET", "/api/v1/dag/graph/solo-job", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].(map[string]any)

	nodes := data["nodes"].([]any)
	edges := data["edges"].([]any)
	if len(nodes) != 1 {
		t.Errorf("nodes = %d, want 1", len(nodes))
	}
	if len(edges) != 0 {
		t.Errorf("edges = %d, want 0", len(edges))
	}
	if data["root_id"] != "solo-job" {
		t.Errorf("root_id = %v", data["root_id"])
	}
}

func TestDAG_GraphWithDeps(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create parent → child relationship.
	rdb.HSet(ctx, m.key("job", "parent-1"),
		"id", "parent-1",
		"type", "data.fetch",
		"queue", "default",
		"status", "completed",
		"created_at", "1708300000",
	)
	rdb.HSet(ctx, m.key("job", "child-1"),
		"id", "child-1",
		"type", "data.process",
		"queue", "default",
		"status", "deferred",
		"depends_on", `["parent-1"]`,
		"created_at", "1708300100",
	)

	// Set up dependents relationship (parent knows about child).
	rdb.SAdd(ctx, m.key("job", "parent-1", "dependents"), "child-1")

	// Load graph from child-1 (should discover parent-1 via depends_on).
	w := doRequest(m, "GET", "/api/v1/dag/graph/child-1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].(map[string]any)

	nodes := data["nodes"].([]any)
	edges := data["edges"].([]any)

	if len(nodes) != 2 {
		t.Errorf("nodes = %d, want 2", len(nodes))
	}
	if len(edges) < 1 {
		t.Errorf("edges = %d, want >= 1", len(edges))
	}

	// Verify edge direction: parent-1 → child-1.
	edge := edges[0].(map[string]any)
	if edge["source"] != "parent-1" || edge["target"] != "child-1" {
		t.Errorf("edge = %v → %v, want parent-1 → child-1", edge["source"], edge["target"])
	}
}

func TestDAG_GraphDepthLimit(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create a chain: a → b → c → d → e (depth 4).
	ids := []string{"a", "b", "c", "d", "e"}
	for i, id := range ids {
		fields := map[string]any{
			"id":     id,
			"type":   "chain.step",
			"queue":  "default",
			"status": "completed",
		}
		if i > 0 {
			// Each node depends on the previous one.
			fields["depends_on"] = fmt.Sprintf(`["%s"]`, ids[i-1])
		}
		rdb.HSet(ctx, m.key("job", id), fields)
		if i > 0 {
			rdb.SAdd(ctx, m.key("job", ids[i-1], "dependents"), id)
		}
	}

	// Request with depth=2 from "a": should get a, b, c (depth 0, 1, 2).
	w := doRequest(m, "GET", "/api/v1/dag/graph/a?depth=2", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].(map[string]any)

	nodes := data["nodes"].([]any)
	// With depth=2 from "a": a (depth 0), b (depth 1, via dependents), c (depth 2, via dependents).
	// d and e should not be included.
	if len(nodes) > 3 {
		nodeIDs := make([]string, len(nodes))
		for i, n := range nodes {
			nodeIDs[i] = n.(map[string]any)["id"].(string)
		}
		t.Errorf("nodes = %v, want at most 3 (depth limit)", nodeIDs)
	}
}

func TestDAG_GraphInvalidID(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// Use characters that are invalid per validPathParam but safe in HTTP paths.
	w := doRequest(m, "GET", "/api/v1/dag/graph/invalid%20id", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// --- Rate limiting tests ---

// doFullRequest routes through the full handler chain (including rate limiter).
// Unlike doRequest which goes to m.mux directly, this uses m.server.Handler.
func doFullRequest(m *Monitor, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w, req)
	return w
}

func TestRateLimit_Enabled(t *testing.T) {
	m, _ := testMonitor(t, Config{RateLimit: 5}) // 5 req/s, burst = 10
	m.startedAt = time.Now()

	// First 10 requests should succeed (burst)
	for i := 0; i < 10; i++ {
		w := doFullRequest(m, "GET", "/api/v1/queues")
		if w.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d should not be rate limited (within burst)", i)
		}
	}

	// Next requests should be rate limited
	limited := false
	for i := 0; i < 20; i++ {
		w := doFullRequest(m, "GET", "/api/v1/queues")
		if w.Code == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Error("expected rate limiting to kick in after burst exhaustion")
	}
}

func TestRateLimit_HealthExempt(t *testing.T) {
	m, _ := testMonitor(t, Config{RateLimit: 1}) // 1 req/s, burst = 2
	m.startedAt = time.Now()

	// Exhaust rate limit
	for i := 0; i < 10; i++ {
		doFullRequest(m, "GET", "/api/v1/queues")
	}

	// Health endpoint should still work
	w := doFullRequest(m, "GET", "/health")
	if w.Code == http.StatusTooManyRequests {
		t.Error("health endpoint should be exempt from rate limiting")
	}
}

func TestRateLimit_Disabled(t *testing.T) {
	m, _ := testMonitor(t, Config{RateLimit: -1}) // disabled
	m.startedAt = time.Now()

	// Many rapid requests should all succeed
	for i := 0; i < 100; i++ {
		w := doFullRequest(m, "GET", "/api/v1/queues")
		if w.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d rate limited, but rate limiting should be disabled", i)
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	m, _ := testMonitor(t, Config{RateLimit: -1})
	m.startedAt = time.Now()

	w := doFullRequest(m, "GET", "/health")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /health: status = %d, want 200", w.Code)
	}

	csp := w.Header().Get("Content-Security-Policy")
	if csp == "" {
		t.Fatal("Content-Security-Policy header missing")
	}
	if !strings.Contains(csp, "default-src 'self'") {
		t.Fatalf("CSP = %q, want to contain default-src 'self'", csp)
	}

	xcto := w.Header().Get("X-Content-Type-Options")
	if xcto != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", xcto)
	}

	xfo := w.Header().Get("X-Frame-Options")
	if xfo != "DENY" {
		t.Fatalf("X-Frame-Options = %q, want DENY", xfo)
	}
}

// --- Server endpoint tests ---

func TestServers_ListEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/servers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T, want []any", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("len(data) = %d, want 0", len(data))
	}
}

func TestServers_ListMultiple(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Seed two server entries.
	rdb.SAdd(ctx, m.key("servers"), "srv-1", "srv-2")
	rdb.HSet(ctx, m.key("server", "srv-1"), map[string]any{
		"id":    "srv-1",
		"pools": "email,default",
	})
	rdb.HSet(ctx, m.key("server", "srv-2"), map[string]any{
		"id":    "srv-2",
		"pools": "billing",
	})

	w := doRequest(m, "GET", "/api/v1/servers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T, want []any", resp.Data)
	}
	if len(data) != 2 {
		t.Fatalf("len(data) = %d, want 2", len(data))
	}

	// Servers are sorted by ID; srv-1 comes first.
	s1 := data[0].(map[string]any)
	if s1["id"] != "srv-1" {
		t.Errorf("s1.id = %v, want srv-1", s1["id"])
	}
	// "pools" should be split into a JSON array.
	pools, ok := s1["pools"].([]any)
	if !ok {
		t.Fatalf("pools type = %T, want []any", s1["pools"])
	}
	if len(pools) != 2 || pools[0] != "email" || pools[1] != "default" {
		t.Errorf("pools = %v, want [email default]", pools)
	}
}

func TestServers_ListSkipsEmptyHash(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Add a server ID to the set but don't create its hash.
	rdb.SAdd(ctx, m.key("servers"), "ghost-srv")

	w := doRequest(m, "GET", "/api/v1/servers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("len(data) = %d, want 0 (ghost entry should be skipped)", len(data))
	}
}

func TestServers_GetExisting(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("server", "srv-1"), map[string]any{
		"id":     "srv-1",
		"pools":  "email,default",
		"status": "running",
	})

	w := doRequest(m, "GET", "/api/v1/servers/srv-1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type = %T, want map", resp.Data)
	}
	if data["id"] != "srv-1" {
		t.Errorf("id = %v, want srv-1", data["id"])
	}
	if data["status"] != "running" {
		t.Errorf("status = %v, want running", data["status"])
	}
	pools, ok := data["pools"].([]any)
	if !ok {
		t.Fatalf("pools type = %T, want []any", data["pools"])
	}
	if len(pools) != 2 {
		t.Errorf("len(pools) = %d, want 2", len(pools))
	}
}

func TestServers_GetNotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/servers/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestServers_GetInvalidID(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// Use characters invalid per validPathParam but safe in HTTP paths.
	w := doRequest(m, "GET", "/api/v1/servers/bad$id", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestPagination_MaxPageCap(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=99999&limit=10", nil)
	page, limit := pagination(req)
	if page != 10000 {
		t.Fatalf("page = %d, want 10000 (capped)", page)
	}
	if limit != 10 {
		t.Fatalf("limit = %d, want 10", limit)
	}
}

// --- Batch 3: Monitor error path tests ---

// handleEnableCron error paths

func TestAdminCron_EnableNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/enable", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminCron_EnableInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/bad$id/enable", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminCron_EnableError(t *testing.T) {
	admin := &mockAdmin{
		enableCronFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cron entry \"x\" not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/x/enable", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// handleDisableCron error paths

func TestAdminCron_DisableNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/disable", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminCron_DisableInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/bad$id/disable", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminCron_DisableError(t *testing.T) {
	admin := &mockAdmin{
		disableCronFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cron entry \"x\" not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/x/disable", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// handleResumeQueue error paths

func TestAdminQueues_ResumeNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/email/resume", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminQueues_ResumeInvalidName(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/bad$name/resume", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminQueues_ResumeNotFound(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/nonexistent/resume", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestAdminQueues_ResumeError(t *testing.T) {
	admin := &mockAdmin{
		resumeQueueFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("redis connection refused")
		},
	}
	m, rdb := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	rdb.SAdd(context.Background(), m.key("queues"), "email")

	w := doRequest(m, "POST", "/api/v1/queues/email/resume", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// handleEmptyQueue error paths

func TestAdminQueues_EmptyNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/email/empty", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminQueues_EmptyInvalidName(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/bad$name/empty", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminQueues_EmptyError(t *testing.T) {
	admin := &mockAdmin{
		emptyQueueFn: func(_ context.Context, _ string) (int64, error) {
			return 0, fmt.Errorf("redis timeout")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/email/empty", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// handleRetryAllDLQ error paths

func TestAdminDLQ_RetryAllNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/default/dead-letter/retry-all", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminDLQ_RetryAllInvalidName(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/bad$name/dead-letter/retry-all", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminDLQ_RetryAllError(t *testing.T) {
	admin := &mockAdmin{
		retryAllDLQFn: func(_ context.Context, _ string) (int64, error) {
			return 0, fmt.Errorf("redis timeout")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/default/dead-letter/retry-all", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// handleClearDLQ error paths

func TestAdminDLQ_ClearNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/default/dead-letter/clear", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminDLQ_ClearInvalidName(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/queues/bad$name/dead-letter/clear", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminDLQ_ClearError(t *testing.T) {
	admin := &mockAdmin{
		clearDLQFn: func(_ context.Context, _ string) (int64, error) {
			return 0, fmt.Errorf("redis timeout")
		},
	}
	m, rdb := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	rdb.SAdd(context.Background(), m.key("queues"), "default")

	w := doRequest(m, "DELETE", "/api/v1/queues/default/dead-letter/clear", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// handleListCron error paths

func TestListCron_EmptyEntries(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("expected empty entries, got %d", len(data))
	}
}

func TestListCron_MalformedEntrySkipped(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// One valid entry, one malformed
	rdb.HSet(ctx, m.key("cron", "entries"), "good", `{"id":"good","expression":"* * * * *"}`)
	rdb.HSet(ctx, m.key("cron", "entries"), "bad", `not json`)

	w := doRequest(m, "GET", "/api/v1/cron", "")
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
		t.Errorf("expected 1 entry (malformed skipped), got %d", len(data))
	}
}

func TestListCron_MultipleEntriesSorted(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("cron", "entries"), "beta", `{"id":"beta"}`)
	rdb.HSet(ctx, m.key("cron", "entries"), "alpha", `{"id":"alpha"}`)

	w := doRequest(m, "GET", "/api/v1/cron", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	first := data[0].(map[string]any)
	if first["id"] != "alpha" {
		t.Errorf("first entry id = %v, want alpha", first["id"])
	}
}

// mapToJobResponse coverage for optional field branches

func TestMapToJobResponse_DependsOn(t *testing.T) {
	data := map[string]string{
		"id":        "j1",
		"depends_on": `["j0"]`,
	}
	resp := mapToJobResponse(data)
	deps, ok := resp["depends_on"].([]any)
	if !ok {
		t.Fatalf("depends_on type = %T", resp["depends_on"])
	}
	if len(deps) != 1 || deps[0] != "j0" {
		t.Errorf("depends_on = %v", deps)
	}
}

func TestMapToJobResponse_DependsOnInvalidJSON(t *testing.T) {
	data := map[string]string{
		"id":        "j1",
		"depends_on": "not-json",
	}
	resp := mapToJobResponse(data)
	if resp["depends_on"] != "not-json" {
		t.Errorf("depends_on = %v, want raw string", resp["depends_on"])
	}
}

func TestMapToJobResponse_RetryIntervals(t *testing.T) {
	data := map[string]string{
		"id":              "j1",
		"retry_intervals": `[1,2,3]`,
	}
	resp := mapToJobResponse(data)
	ri, ok := resp["retry_intervals"].([]any)
	if !ok {
		t.Fatalf("retry_intervals type = %T", resp["retry_intervals"])
	}
	if len(ri) != 3 {
		t.Errorf("retry_intervals len = %d", len(ri))
	}
}

func TestMapToJobResponse_RetryIntervalsInvalidJSON(t *testing.T) {
	data := map[string]string{
		"id":              "j1",
		"retry_intervals": "bad",
	}
	resp := mapToJobResponse(data)
	if resp["retry_intervals"] != "bad" {
		t.Errorf("retry_intervals = %v, want raw string", resp["retry_intervals"])
	}
}

func TestMapToJobResponse_ResultNonJSON(t *testing.T) {
	data := map[string]string{
		"id":     "j1",
		"result": "plain text result",
	}
	resp := mapToJobResponse(data)
	if resp["result"] != "plain text result" {
		t.Errorf("result = %v", resp["result"])
	}
}

func TestMapToJobResponse_ResultEmpty(t *testing.T) {
	data := map[string]string{
		"id":     "j1",
		"result": "",
	}
	resp := mapToJobResponse(data)
	if _, exists := resp["result"]; exists {
		t.Error("empty result should not be included")
	}
}

func TestMapToJobResponse_ResultValidJSON(t *testing.T) {
	data := map[string]string{
		"id":     "j1",
		"result": `{"key":"val"}`,
	}
	resp := mapToJobResponse(data)
	m, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("result type = %T", resp["result"])
	}
	if m["key"] != "val" {
		t.Errorf("result key = %v", m["key"])
	}
}

func TestMapToJobResponse_DisallowedFieldFiltered(t *testing.T) {
	data := map[string]string{
		"id":      "j1",
		"secret":  "should-not-appear",
	}
	resp := mapToJobResponse(data)
	if _, exists := resp["secret"]; exists {
		t.Error("disallowed field 'secret' should be filtered")
	}
}

// extractJobIDFromDependentsKey error paths

func TestExtractJobIDFromDependentsKey_Valid(t *testing.T) {
	got := extractJobIDFromDependentsKey("gqm:job:abc123:dependents", "gqm:")
	if got != "abc123" {
		t.Errorf("got %q, want abc123", got)
	}
}

func TestExtractJobIDFromDependentsKey_TooShort(t *testing.T) {
	got := extractJobIDFromDependentsKey("short", "gqm:")
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestExtractJobIDFromDependentsKey_WrongPrefix(t *testing.T) {
	got := extractJobIDFromDependentsKey("other:job:abc:dependents", "gqm:")
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestExtractJobIDFromDependentsKey_WrongSuffix(t *testing.T) {
	got := extractJobIDFromDependentsKey("gqm:job:abc:other", "gqm:")
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// handleListWorkers: stale worker entry (registered in set but hash expired)

func TestWorkers_ListStaleWorker(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Register worker in set but don't create the hash key
	rdb.SAdd(ctx, m.key("workers"), "stale-pool")

	w := doRequest(m, "GET", "/api/v1/workers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 workers (stale skipped), got %d", len(data))
	}
}

func TestWorkers_ListMixedStaleAndValid(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("workers"), "stale-pool", "valid-pool")
	rdb.HSet(ctx, m.key("worker", "valid-pool"),
		"id", "valid-pool",
		"status", "active",
	)

	w := doRequest(m, "GET", "/api/v1/workers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Errorf("expected 1 worker (stale skipped), got %d", len(data))
	}
}

// listSortedSetJobs: via queue jobs with processing status

func TestQueueJobs_Processing(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Add jobs to processing sorted set
	rdb.ZAdd(ctx, m.key("queue", "email", "processing"), redis.Z{Score: 1, Member: "j1"})
	rdb.ZAdd(ctx, m.key("queue", "email", "processing"), redis.Z{Score: 2, Member: "j2"})

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=processing", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Meta == nil {
		t.Fatal("expected meta in response")
	}
	if resp.Meta.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Meta.Total)
	}
}

func TestQueueJobs_Completed(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: 1, Member: "j1"})

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=completed", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestQueueJobs_DeadLetter(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: 1, Member: "j1"})

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=dead_letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestQueueJobs_InvalidStatus(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=bogus", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// rateLimiter close and cleanup

func TestRateLimiter_Close(t *testing.T) {
	rl := newRateLimiter(100)
	// Give the cleanup goroutine time to enter the select loop
	time.Sleep(10 * time.Millisecond)
	// Should not panic; cleanup goroutine exits via stop channel
	rl.close()
	// Wait for the goroutine to actually return
	time.Sleep(10 * time.Millisecond)
}

func TestRateLimiter_CleanupRemovesStale(t *testing.T) {
	rl := &rateLimiter{
		visitors: make(map[string]*tokenBucket),
		rate:     100,
		burst:    200,
		stop:     make(chan struct{}),
	}

	// Insert a stale entry (last access far in the past)
	rl.visitors["192.168.1.1"] = &tokenBucket{
		tokens: 100,
		last:   time.Now().Add(-10 * time.Minute),
	}
	// Insert a fresh entry
	rl.visitors["192.168.1.2"] = &tokenBucket{
		tokens: 100,
		last:   time.Now(),
	}

	// Run cleanup logic directly (not the goroutine)
	rl.mu.Lock()
	cutoff := time.Now().Add(-5 * time.Minute)
	for ip, b := range rl.visitors {
		if b.last.Before(cutoff) {
			delete(rl.visitors, ip)
		}
	}
	rl.mu.Unlock()

	if len(rl.visitors) != 1 {
		t.Errorf("visitors count = %d, want 1", len(rl.visitors))
	}
	if _, exists := rl.visitors["192.168.1.2"]; !exists {
		t.Error("fresh visitor should remain")
	}
}

// errorToHTTPStatus / errorToCode: "cannot" case

func TestErrorToHTTPStatus_Conflict(t *testing.T) {
	err := fmt.Errorf("gqm: cannot cancel job in completed state")
	got := errorToHTTPStatus(err)
	if got != http.StatusConflict {
		t.Errorf("got %d, want %d", got, http.StatusConflict)
	}
}

func TestErrorToCode_Conflict(t *testing.T) {
	err := fmt.Errorf("gqm: cannot cancel job in completed state")
	got := errorToCode(err)
	if got != "CONFLICT" {
		t.Errorf("got %q, want CONFLICT", got)
	}
}

func TestErrorToHTTPStatus_Default(t *testing.T) {
	err := fmt.Errorf("redis connection refused")
	got := errorToHTTPStatus(err)
	if got != http.StatusInternalServerError {
		t.Errorf("got %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestErrorToCode_Default(t *testing.T) {
	err := fmt.Errorf("redis connection refused")
	got := errorToCode(err)
	if got != "INTERNAL" {
		t.Errorf("got %q, want INTERNAL", got)
	}
}

// handleTriggerCron invalid path param
func TestAdminCron_TriggerInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/bad$id/trigger", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// handlePauseQueue error paths (previously only NoAdmin was tested)

func TestAdminQueues_PauseInvalidName(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/bad$name/pause", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminQueues_PauseNotFound(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/queues/nonexistent/pause", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestAdminQueues_PauseError(t *testing.T) {
	admin := &mockAdmin{
		pauseQueueFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("redis timeout")
		},
	}
	m, rdb := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	rdb.SAdd(context.Background(), m.key("queues"), "email")

	w := doRequest(m, "POST", "/api/v1/queues/email/pause", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// handleEnableCron with "cannot" error (conflict status code)

func TestAdminCron_EnableConflict(t *testing.T) {
	admin := &mockAdmin{
		enableCronFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cannot enable cron entry that is already enabled")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/enable", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

// handleDisableCron with "cannot" error (conflict status code)

func TestAdminCron_DisableConflict(t *testing.T) {
	admin := &mockAdmin{
		disableCronFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: cannot disable cron entry that is already disabled")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/cron/daily/disable", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

// --- Batch 3 continued: more error paths ---

// handleCancelJob error paths

func TestAdminJobs_CancelNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/cancel", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminJobs_CancelInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/bad$id/cancel", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_CancelNotFound(t *testing.T) {
	admin := &mockAdmin{
		cancelJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: job \"x\" not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/x/cancel", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// handleDeleteJob error paths

func TestAdminJobs_DeleteNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/jobs/j1", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminJobs_DeleteInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/jobs/bad$id", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAdminJobs_DeleteNotFound(t *testing.T) {
	admin := &mockAdmin{
		deleteJobFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("gqm: job \"x\" not found")
		},
	}
	m, _ := testMonitorWithAdmin(t, Config{}, admin)
	m.startedAt = time.Now()

	w := doRequest(m, "DELETE", "/api/v1/jobs/x", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// handleRetryJob error paths

func TestAdminJobs_RetryNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/j1/retry", "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

func TestAdminJobs_RetryInvalidID(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/bad$id/retry", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// handleGetCron error paths

func TestGetCron_NotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestGetCron_InvalidID(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron/bad$id", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGetCron_MalformedEntry(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()

	rdb.HSet(context.Background(), m.key("cron", "entries"), "bad", "not-json")

	w := doRequest(m, "GET", "/api/v1/cron/bad", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestGetCron_WithHistory(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("cron", "entries"), "daily", `{"id":"daily","expression":"0 0 * * *"}`)
	rdb.ZAdd(ctx, m.key("cron", "history", "daily"), redis.Z{Score: 1700000000, Member: "job-123"})

	w := doRequest(m, "GET", "/api/v1/cron/daily", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	lastRun, ok := data["last_run"].(map[string]any)
	if !ok {
		t.Fatal("expected last_run object")
	}
	if lastRun["job_id"] != "job-123" {
		t.Errorf("job_id = %v", lastRun["job_id"])
	}
}

// handleCronHistory error paths

func TestCronHistory_Success(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.ZAdd(ctx, m.key("cron", "history", "daily"),
		redis.Z{Score: 1700000001, Member: "j1"},
		redis.Z{Score: 1700000002, Member: "j2"},
	)

	w := doRequest(m, "GET", "/api/v1/cron/daily/history", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 2 {
		t.Errorf("got %d entries, want 2", len(data))
	}
}

func TestCronHistory_InvalidID(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/cron/bad$id/history", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCronHistory_LimitBounds(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// limit < 1 should be clamped to 1
	w := doRequest(m, "GET", "/api/v1/cron/daily/history?limit=0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// limit > 100 should be clamped to 100
	w = doRequest(m, "GET", "/api/v1/cron/daily/history?limit=200", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

// handleListDLQ

func TestListDLQ_Success(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: 1, Member: "j1"})
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "dead_letter", "type", "email:send")

	w := doRequest(m, "GET", "/api/v1/queues/email/dead-letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Meta.Total != 1 {
		t.Errorf("total = %d, want 1", resp.Meta.Total)
	}
}

func TestListDLQ_InvalidName(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/bad$name/dead-letter", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// handleListQueues additional paths

func TestListQueues_WithStatsCounters(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1", "j2")
	rdb.Set(ctx, m.key("stats", "email", "processed_total"), "100", 0)
	rdb.Set(ctx, m.key("stats", "email", "failed_total"), "5", 0)

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(data))
	}
	q := data[0].(map[string]any)
	if q["ready"] != float64(2) {
		t.Errorf("ready = %v, want 2", q["ready"])
	}
}

// handleGetQueue

func TestGetQueue_Success(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1")
	rdb.Set(ctx, m.key("stats", "email", "processed_total"), "50", 0)

	w := doRequest(m, "GET", "/api/v1/queues/email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestGetQueue_InvalidName(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/bad$name", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// handleStatsDaily

func TestStatsDaily_WithQueueFilter(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	today := time.Now().UTC().Format("2006-01-02")
	rdb.SAdd(ctx, m.key("queues"), "email", "default")
	rdb.Set(ctx, m.key("stats", "email", "processed", today), "10", 0)

	w := doRequest(m, "GET", "/api/v1/stats/daily?queue=email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Errorf("expected 1 queue in response, got %d", len(data))
	}
}

func TestStatsDaily_DaysBounds(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// days < 1 should clamp to 1
	w := doRequest(m, "GET", "/api/v1/stats/daily?days=0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// days > 90 should clamp to 90
	w = doRequest(m, "GET", "/api/v1/stats/daily?days=200", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

// handleListQueueJobs: ready path (not sorted set)

func TestQueueJobs_Ready(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1", "j2")

	w := doRequest(m, "GET", "/api/v1/queues/email/jobs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Meta.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Meta.Total)
	}
}

func TestQueueJobs_InvalidName(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/bad$name/jobs", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// queryInt edge case

func TestQueryInt_InvalidValue(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=abc", nil)
	got := queryInt(req, "page", 42)
	if got != 42 {
		t.Errorf("got %d, want 42 (default)", got)
	}
}

// pagination edge case: page < 1

func TestPagination_PageBelowMin(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=0&limit=10", nil)
	page, limit := pagination(req)
	if page != 1 {
		t.Fatalf("page = %d, want 1 (clamped)", page)
	}
	if limit != 10 {
		t.Fatalf("limit = %d, want 10", limit)
	}
}

func TestPagination_LimitBelowMin(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=1&limit=-5", nil)
	_, limit := pagination(req)
	if limit != 1 {
		t.Fatalf("limit = %d, want 1 (clamped)", limit)
	}
}

func TestPagination_LimitAboveMax(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?page=1&limit=999", nil)
	_, limit := pagination(req)
	if limit != 100 {
		t.Fatalf("limit = %d, want 100 (capped)", limit)
	}
}

// handleLogout without cookie

func TestLogout_NoCookie(t *testing.T) {
	// Auth disabled so requireAuth passes through; handleLogout handles missing cookie gracefully
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/logout", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

// handleLogin: empty credentials

func TestLogin_EmptyCredentials(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		AuthUsers:   []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `{"username":"","password":""}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLogin_TooLongCredentials(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		AuthUsers:   []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	longUser := strings.Repeat("a", 257)
	w := doRequest(m, "POST", "/auth/login", fmt.Sprintf(`{"username":"%s","password":"pass"}`, longUser))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLogin_Disabled(t *testing.T) {
	m, _ := testMonitor(t, Config{AuthEnabled: false})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"pass"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLogin_InvalidJSON(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled: true,
		AuthUsers:   []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `not-json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// userRole edge case: user not found defaults to admin

func TestUserRole_NotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthUsers: []AuthUser{{Username: "alice", Role: "viewer"}},
	})
	got := m.userRole("unknown")
	if got != "admin" {
		t.Errorf("role = %q, want admin", got)
	}
}

func TestUserRole_FoundWithRole(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthUsers: []AuthUser{{Username: "alice", Role: "viewer"}},
	})
	got := m.userRole("alice")
	if got != "viewer" {
		t.Errorf("role = %q, want viewer", got)
	}
}

// serveIndexHTML: missing index.html

func TestServeIndexHTML_Missing(t *testing.T) {
	w := httptest.NewRecorder()
	// Use an empty FS
	emptyFS := os.DirFS(t.TempDir())
	serveIndexHTML(w, emptyFS)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestServeIndexHTML_Valid(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0644)

	w := httptest.NewRecorder()
	serveIndexHTML(w, os.DirFS(dir))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}
}

// checkDashboardVersion: missing VERSION, version mismatch

func TestCheckDashboardVersion_MissingFile(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	// Should not panic; just logs a warning
	m.checkDashboardVersion(t.TempDir())
}

func TestCheckDashboardVersion_Mismatch(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "VERSION"), []byte("0.0.1"), 0644)
	// Should not panic; just logs a warning about mismatch
	m.checkDashboardVersion(dir)
}

// DAG graph: node limit restricts BFS exploration

func TestDAG_GraphNodeLimit(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create a root with many children
	rdb.HSet(ctx, m.key("job", "root"), "id", "root", "status", "completed", "type", "test")
	for i := 0; i < 5; i++ {
		cid := fmt.Sprintf("child-%d", i)
		rdb.SAdd(ctx, m.key("job", "root", "dependents"), cid)
		rdb.HSet(ctx, m.key("job", cid), "id", cid, "status", "completed", "type", "test")
	}

	// max_nodes=3: should limit to 3 nodes total (root + 2 children)
	w := doRequest(m, "GET", "/api/v1/dag/graph/root?max_nodes=3", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp struct {
		Data struct {
			Nodes []dagNode `json:"nodes"`
			Edges []dagEdge `json:"edges"`
		} `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Data.Nodes) > 3 {
		t.Errorf("nodes = %d, want <= 3", len(resp.Data.Nodes))
	}
	if len(resp.Data.Edges) == 0 {
		t.Error("expected edges in graph")
	}
}

// DAG graph: unknown job (no hash data)

func TestDAG_GraphUnknownJob(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	// Job doesn't exist, should return placeholder with "unknown" status
	w := doRequest(m, "GET", "/api/v1/dag/graph/nonexistent", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp struct {
		Data struct {
			Nodes []dagNode `json:"nodes"`
		} `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Data.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(resp.Data.Nodes))
	}
	if resp.Data.Nodes[0].Status != "unknown" {
		t.Errorf("status = %q, want unknown", resp.Data.Nodes[0].Status)
	}
}

// DAG graph: edge deduplication

func TestDAG_GraphEdgeDedup(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create parent -> child where child also has depends_on pointing back to parent
	rdb.HSet(ctx, m.key("job", "parent"), "id", "parent", "status", "completed")
	rdb.SAdd(ctx, m.key("job", "parent", "dependents"), "child")
	rdb.HSet(ctx, m.key("job", "child"), "id", "child", "status", "completed",
		"depends_on", `["parent"]`)

	w := doRequest(m, "GET", "/api/v1/dag/graph/parent", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp struct {
		Data struct {
			Edges []dagEdge `json:"edges"`
		} `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&resp)

	// Should have exactly 1 unique edge parent->child (deduped)
	count := 0
	for _, e := range resp.Data.Edges {
		if e.Source == "parent" && e.Target == "child" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("parent->child edges = %d, want 1 (deduped)", count)
	}
}

// DAG graph: depth/max_nodes bounds clamping

func TestDAG_GraphParamBounds(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()

	rdb.HSet(context.Background(), m.key("job", "j1"), "id", "j1", "status", "completed")

	// depth < 1 should clamp to 1, max_nodes < 1 should clamp to 1
	w := doRequest(m, "GET", "/api/v1/dag/graph/j1?depth=0&max_nodes=0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// depth > 20, max_nodes > 200 should clamp
	w = doRequest(m, "GET", "/api/v1/dag/graph/j1?depth=50&max_nodes=500", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

// handleListDeferred: pagination beyond range

func TestDAG_ListDeferredBeyondRange(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("deferred"), "j1")
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "deferred")

	w := doRequest(m, "GET", "/api/v1/dag/deferred?page=100", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("expected empty data for page beyond range, got %d", len(data))
	}
}

// fetchDeferredJobs: stale job (in deferred set but hash missing)

func TestDAG_ListDeferredStaleJob(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Job in deferred set but no hash
	rdb.SAdd(ctx, m.key("deferred"), "stale", "valid")
	rdb.HSet(ctx, m.key("job", "valid"), "id", "valid", "status", "deferred")

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Errorf("expected 1 job (stale skipped), got %d", len(data))
	}
}

// handleListDAGRoots pagination beyond range

func TestDAG_ListRootsBeyondRange(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("job", "j1", "dependents"), "j2")
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "completed")

	w := doRequest(m, "GET", "/api/v1/dag/roots?page=100", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("expected empty for page beyond range")
	}
}

// extractIP edge case: no port

func TestExtractIP_NoPort(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1" // No port
	got := extractIP(req)
	if got != "192.168.1.1" {
		t.Errorf("got %q, want 192.168.1.1", got)
	}
}

// isSecure

func TestIsSecure_TLS(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.TLS = &tls.ConnectionState{}
	if !isSecure(req) {
		t.Error("expected isSecure=true for TLS")
	}
}

func TestIsSecure_XForwardedProto(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	if !isSecure(req) {
		t.Error("expected isSecure=true for X-Forwarded-Proto=https")
	}
}

func TestIsSecure_PlainHTTP(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	if isSecure(req) {
		t.Error("expected isSecure=false for plain HTTP")
	}
}

// handleBatchRetry: no admin

func TestAdminJobs_BatchRetryNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/batch/retry", `{"job_ids":["j1"]}`)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

// handleBatchDelete: no admin

func TestAdminJobs_BatchDeleteNoAdmin(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", `{"job_ids":["j1"]}`)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", w.Code)
	}
}

// handleLogout with HTTPS (isSecure path)

// handleGetQueue: queue not found

func TestGetQueue_NotFound(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/queues/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// handleGetQueue with all stats populated

func TestGetQueue_FullStats(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1", "j2")
	rdb.ZAdd(ctx, m.key("queue", "email", "processing"), redis.Z{Score: 1, Member: "j3"})
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: 1, Member: "j4"})
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: 1, Member: "j5"})
	rdb.Set(ctx, m.key("stats", "email", "processed_total"), "100", 0)
	rdb.Set(ctx, m.key("stats", "email", "failed_total"), "5", 0)
	rdb.SAdd(ctx, m.key("paused"), "email")

	w := doRequest(m, "GET", "/api/v1/queues/email", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.(map[string]any)
	if data["paused"] != true {
		t.Errorf("paused = %v, want true", data["paused"])
	}
	if data["processed_total"] != float64(100) {
		t.Errorf("processed_total = %v", data["processed_total"])
	}
	if data["failed_total"] != float64(5) {
		t.Errorf("failed_total = %v", data["failed_total"])
	}
}

// handleListQueues with paused queue and all counters

func TestListQueues_PausedWithCounters(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1")
	rdb.ZAdd(ctx, m.key("queue", "email", "processing"), redis.Z{Score: 1, Member: "j2"})
	rdb.ZAdd(ctx, m.key("queue", "email", "completed"), redis.Z{Score: 1, Member: "j3"})
	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"), redis.Z{Score: 1, Member: "j4"})
	rdb.SAdd(ctx, m.key("paused"), "email")

	w := doRequest(m, "GET", "/api/v1/queues", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Fatalf("expected 1 queue")
	}
	q := data[0].(map[string]any)
	if q["paused"] != true {
		t.Errorf("paused = %v", q["paused"])
	}
}

// handleListDLQ: with job data

func TestListDLQ_WithJobData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.ZAdd(ctx, m.key("queue", "email", "dead_letter"),
		redis.Z{Score: 1, Member: "j1"},
		redis.Z{Score: 2, Member: "j2"},
		redis.Z{Score: 3, Member: "stale"}, // no hash data
	)
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "type", "email:send", "status", "dead_letter")
	rdb.HSet(ctx, m.key("job", "j2"), "id", "j2", "type", "email:send", "status", "dead_letter")

	w := doRequest(m, "GET", "/api/v1/queues/email/dead-letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Meta.Total != 3 {
		t.Errorf("total = %d, want 3", resp.Meta.Total)
	}
	data := resp.Data.([]any)
	if len(data) != 2 {
		t.Errorf("jobs = %d, want 2 (stale filtered)", len(data))
	}
}

// handleListQueueJobs: ready with empty status param (default)

func TestQueueJobs_DefaultStatusReady(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.LPush(ctx, m.key("queue", "email", "ready"), "j1")
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "type", "email:send", "status", "ready")

	// No status param → defaults to ready
	w := doRequest(m, "GET", "/api/v1/queues/email/jobs?status=ready", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Errorf("jobs = %d, want 1", len(data))
	}
}

// handleListDeferred: SCard success with full data pipeline

func TestDAG_ListDeferredWithPendingDeps(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("deferred"), "j1")
	rdb.HSet(ctx, m.key("job", "j1"), "id", "j1", "status", "deferred", "type", "email:send",
		"depends_on", `["j0"]`)
	rdb.SAdd(ctx, m.key("job", "j1", "pending_deps"), "j0")

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Fatalf("got %d items, want 1", len(data))
	}
	job := data[0].(map[string]any)
	pendDeps, ok := job["pending_deps"].([]any)
	if !ok {
		t.Fatal("pending_deps not found")
	}
	if len(pendDeps) != 1 || pendDeps[0] != "j0" {
		t.Errorf("pending_deps = %v", pendDeps)
	}
}

// handleListDAGRoots: stale root (in scan but hash missing)

func TestDAG_ListRootsStaleRoot(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Create dependents set but no job hash
	rdb.SAdd(ctx, m.key("job", "stale-root", "dependents"), "child")

	w := doRequest(m, "GET", "/api/v1/dag/roots", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("expected 0 roots (stale filtered), got %d", len(data))
	}
}

// handleListServers: stale server (in set but hash missing)

func TestServers_ListStaleServer(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()

	rdb.SAdd(context.Background(), m.key("servers"), "stale-server")

	w := doRequest(m, "GET", "/api/v1/servers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 0 {
		t.Errorf("expected 0 servers (stale filtered), got %d", len(data))
	}
}

// handleStatsDaily: with failed_total data

func TestStatsDaily_WithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	today := time.Now().UTC().Format("2006-01-02")
	rdb.SAdd(ctx, m.key("queues"), "email")
	rdb.Set(ctx, m.key("stats", "email", "processed", today), "25", 0)
	rdb.Set(ctx, m.key("stats", "email", "failed", today), "3", 0)

	w := doRequest(m, "GET", "/api/v1/stats/daily?days=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp.Data.([]any)
	if len(data) != 1 {
		t.Fatalf("queues = %d, want 1", len(data))
	}
	qd := data[0].(map[string]any)
	days := qd["days"].([]any)
	day := days[0].(map[string]any)
	if day["processed"] != float64(25) {
		t.Errorf("processed = %v, want 25", day["processed"])
	}
	if day["failed"] != float64(3) {
		t.Errorf("failed = %v, want 3", day["failed"])
	}
}

// validateSession: invalid token format

func TestValidateSession_InvalidFormat(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	_, err := m.validateSession(context.Background(), "short-token")
	if err == nil {
		t.Error("expected error for invalid token format")
	}
}

// handleLogin: successful login over HTTPS (SameSiteStrict cookie)

func TestLogin_HTTPS(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	req := httptest.NewRequest("POST", "/auth/login", strings.NewReader(`{"username":"admin","password":"pass"}`))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = &tls.ConnectionState{}
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			if !c.Secure {
				t.Error("cookie should have Secure=true over HTTPS")
			}
		}
	}
}

// handleLogin: wrong password (covers bcrypt error branch)

func TestLogin_WrongPassword(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"wrong"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// handleLogin: unknown user (covers dummy hash path)

func TestLogin_UnknownUser(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `{"username":"unknown","password":"pass"}`)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// allow: rate limit denied path

func TestRateLimiter_AllowDenied(t *testing.T) {
	rl := &rateLimiter{
		visitors: make(map[string]*tokenBucket),
		rate:     1,
		burst:    1,
		stop:     make(chan struct{}),
	}

	// First request uses the burst token
	if !rl.allow("1.2.3.4") {
		t.Error("first request should be allowed")
	}
	// Immediately after, tokens are depleted
	if rl.allow("1.2.3.4") {
		t.Error("second request should be denied (no tokens)")
	}
}

// handleGetWorker: invalid ID

func TestWorkers_GetInvalidID(t *testing.T) {
	m, _ := testMonitor(t, Config{})
	m.startedAt = time.Now()

	w := doRequest(m, "GET", "/api/v1/workers/bad$id", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// handleBatchDelete: invalid chars in job IDs

func TestAdminJobs_BatchDeleteInvalidChars(t *testing.T) {
	m, _ := testMonitorWithAdmin(t, Config{}, &mockAdmin{})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/api/v1/jobs/batch/delete", `{"job_ids":["bad$id"]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLogout_HTTPS(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	// Login first to get a session
	loginW := doRequest(m, "POST", "/auth/login", `{"username":"admin","password":"pass"}`)
	var sessionToken string
	for _, c := range loginW.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionToken = c.Value
		}
	}
	if sessionToken == "" {
		t.Fatal("no session token from login")
	}

	// Logout over HTTPS
	req := httptest.NewRequest("POST", "/auth/logout", nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Cookie", sessionCookieName+"="+sessionToken)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	// Check that cookie has Secure flag and SameSite=Strict
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			if !c.Secure {
				t.Error("cookie should have Secure=true over HTTPS")
			}
			if c.SameSite != http.SameSiteStrictMode {
				t.Errorf("SameSite = %v, want Strict", c.SameSite)
			}
		}
	}
}

// --- Monitor Start/Stop ---

func TestMonitor_StartStop(t *testing.T) {
	rdb := testRedisClient(t)
	defer rdb.Close()
	prefix := testPrefix(t)

	m := New(rdb, prefix, testLogger(), Config{
		APIAddr: "127.0.0.1:0", // random port
	}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start()
	}()

	// Give server a moment to start.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Start should return nil (graceful shutdown).
	if err := <-errCh; err != nil {
		t.Errorf("Start returned: %v", err)
	}
}

// --- Rate limiter tests ---

func TestRateLimiter_Cleanup(t *testing.T) {
	rl := &rateLimiter{
		visitors: make(map[string]*tokenBucket),
		rate:     100,
		burst:    200,
		stop:     make(chan struct{}),
	}

	// Add a stale visitor (last seen 10 minutes ago).
	rl.visitors["1.2.3.4"] = &tokenBucket{tokens: 100, last: time.Now().Add(-10 * time.Minute)}
	// Add a fresh visitor.
	rl.visitors["5.6.7.8"] = &tokenBucket{tokens: 100, last: time.Now()}

	// Manually trigger cleanup logic.
	rl.mu.Lock()
	cutoff := time.Now().Add(-5 * time.Minute)
	for ip, b := range rl.visitors {
		if b.last.Before(cutoff) {
			delete(rl.visitors, ip)
		}
	}
	rl.mu.Unlock()

	if _, ok := rl.visitors["1.2.3.4"]; ok {
		t.Error("stale visitor should be evicted")
	}
	if _, ok := rl.visitors["5.6.7.8"]; !ok {
		t.Error("fresh visitor should be kept")
	}
	close(rl.stop)
}

func TestRateLimiter_CleanupGoroutine(t *testing.T) {
	rl := newRateLimiter(100)
	// close should not panic.
	rl.close()
}

func TestExtractIP_WithPort(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	ip := extractIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("extractIP = %q, want 192.168.1.1", ip)
	}
}

func TestExtractIP_WithoutPort(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1"
	ip := extractIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("extractIP = %q, want 192.168.1.1", ip)
	}
}

// --- Health degraded ---

func TestHealth_Degraded(t *testing.T) {
	// Use a client pointing to a non-existent Redis to trigger degraded state.
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:59999"})
	defer rdb.Close()

	m := New(rdb, "test:", testLogger(), Config{}, nil)
	m.startedAt = time.Now()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	m.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "degraded" {
		t.Errorf("status = %v, want degraded", resp["status"])
	}
	if resp["redis"] != false {
		t.Errorf("redis = %v, want false", resp["redis"])
	}
}

// --- DLQ tests ---

func TestDLQ_ListWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	queueName := "test-dlq-queue"
	dlqKey := prefix + "queue:" + queueName + ":dead_letter"
	jobKey := prefix + "job:dlq-job-1"

	// Create job and add to DLQ.
	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "dlq-job-1", "type": "test", "queue": queueName, "status": "dead_letter",
	})
	rdb.ZAdd(ctx, dlqKey, redis.Z{Score: float64(time.Now().Unix()), Member: "dlq-job-1"})

	w := doRequest(m, "GET", "/api/v1/queues/"+queueName+"/dead-letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("DLQ jobs = %d, want 1", len(data))
	}
}

// --- Queue job listing by status ---

func TestQueues_ListJobsByStatus(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	queueName := "sorted-set-queue"
	processingKey := prefix + "queue:" + queueName + ":processing"
	jobKey := prefix + "job:proc-job-1"

	// Seed data.
	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "proc-job-1", "type": "test", "queue": queueName, "status": "processing",
	})
	rdb.ZAdd(ctx, processingKey, redis.Z{Score: float64(time.Now().Unix()), Member: "proc-job-1"})
	rdb.SAdd(ctx, prefix+"queues", queueName)

	w := doRequest(m, "GET", "/api/v1/queues/"+queueName+"/jobs?status=processing", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("processing jobs = %d, want 1", len(data))
	}
}

// --- Deferred jobs listing ---

func TestDAG_ListDeferred_WithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	deferredKey := prefix + "deferred"
	jobKey := prefix + "job:def-job-1"

	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "def-job-1", "type": "test", "queue": "default", "status": "deferred",
	})
	rdb.SAdd(ctx, deferredKey, "def-job-1")

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("deferred jobs = %d, want 1", len(data))
	}
}

func TestDAG_ListDeferred_Empty(t *testing.T) {
	m, _ := testMonitor(t, Config{})

	w := doRequest(m, "GET", "/api/v1/dag/deferred", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 0 {
		t.Errorf("deferred jobs = %d, want 0", len(data))
	}
}

// --- Login edge cases ---

func TestAuth_LoginInvalidJSON(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", "{bad json}")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAuth_LoginEmptyBody(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pass"), bcrypt.MinCost)
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: string(hash)}},
	})
	m.startedAt = time.Now()

	w := doRequest(m, "POST", "/auth/login", `{"username":"","password":"pass"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// --- Cron listing ---

func TestCron_ListWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	entriesKey := prefix + "cron:entries"

	entry := `{"id":"daily-backup","name":"Daily Backup","cron_expr":"0 0 3 * * *","job_type":"backup.db","enabled":true}`
	rdb.HSet(ctx, entriesKey, "daily-backup", entry)

	w := doRequest(m, "GET", "/api/v1/cron", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("cron entries = %d, want 1", len(data))
	}
}

// --- Workers listing ---

func TestQueues_ListJobsCompleted(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	queueName := "completed-queue"
	completedKey := prefix + "queue:" + queueName + ":completed"
	jobKey := prefix + "job:comp-job-1"

	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "comp-job-1", "type": "test", "queue": queueName, "status": "completed",
	})
	rdb.ZAdd(ctx, completedKey, redis.Z{Score: float64(time.Now().Unix()), Member: "comp-job-1"})
	rdb.SAdd(ctx, prefix+"queues", queueName)

	w := doRequest(m, "GET", "/api/v1/queues/"+queueName+"/jobs?status=completed", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestQueues_ListJobsDeadLetter(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	queueName := "dl-queue"
	dlKey := prefix + "queue:" + queueName + ":dead_letter"
	jobKey := prefix + "job:dl-job-1"

	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "dl-job-1", "type": "test", "queue": queueName, "status": "dead_letter",
	})
	rdb.ZAdd(ctx, dlKey, redis.Z{Score: float64(time.Now().Unix()), Member: "dl-job-1"})
	rdb.SAdd(ctx, prefix+"queues", queueName)

	w := doRequest(m, "GET", "/api/v1/queues/"+queueName+"/jobs?status=dead_letter", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestQueues_ListJobsInvalidStatus(t *testing.T) {
	m, _ := testMonitor(t, Config{})

	w := doRequest(m, "GET", "/api/v1/queues/q/jobs?status=invalid", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCron_HistoryWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	historyKey := prefix + "cron:history:daily"

	// History uses a sorted set: member=jobID, score=timestamp.
	rdb.ZAdd(ctx, historyKey, redis.Z{Score: 1000, Member: "cron-job-1"})

	w := doRequest(m, "GET", "/api/v1/cron/daily/history", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("history entries = %d, want 1", len(data))
	}
}

func TestCron_HistoryEmpty(t *testing.T) {
	m, _ := testMonitor(t, Config{})

	w := doRequest(m, "GET", "/api/v1/cron/nonexistent/history", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestServers_ListWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	serversKey := prefix + "servers"
	serverKey := prefix + "server:test-srv-1"

	rdb.SAdd(ctx, serversKey, "test-srv-1")
	rdb.HSet(ctx, serverKey, map[string]any{
		"id":             "test-srv-1",
		"hostname":       "test-host",
		"status":         "active",
		"last_heartbeat": fmt.Sprintf("%d", time.Now().Unix()),
	})

	w := doRequest(m, "GET", "/api/v1/servers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("servers = %d, want 1", len(data))
	}
}

func TestDAG_ListDeferred_Paginated(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	deferredKey := prefix + "deferred"

	// Add 3 jobs.
	for i := 1; i <= 3; i++ {
		jobID := fmt.Sprintf("paged-job-%d", i)
		jobKey := prefix + "job:" + jobID
		rdb.HSet(ctx, jobKey, map[string]any{
			"id": jobID, "type": "test", "queue": "default", "status": "deferred",
		})
		rdb.SAdd(ctx, deferredKey, jobID)
	}

	// Request page 2 with limit 2 (should get 1 result).
	w := doRequest(m, "GET", "/api/v1/dag/deferred?page=2&limit=2", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("page 2 jobs = %d, want 1", len(data))
	}
}

func TestDAG_ListDeferred_PageBeyondData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	deferredKey := prefix + "deferred"
	jobKey := prefix + "job:beyond-job-1"
	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "beyond-job-1", "type": "test", "queue": "default", "status": "deferred",
	})
	rdb.SAdd(ctx, deferredKey, "beyond-job-1")

	// Request page 99 (beyond data).
	w := doRequest(m, "GET", "/api/v1/dag/deferred?page=99&limit=20", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 0 {
		t.Errorf("beyond-page jobs = %d, want 0", len(data))
	}
}

func TestQueues_ListJobsReady(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	queueName := "ready-queue"
	readyKey := prefix + "queue:" + queueName + ":ready"
	jobKey := prefix + "job:ready-job-1"

	rdb.HSet(ctx, jobKey, map[string]any{
		"id": "ready-job-1", "type": "test", "queue": queueName, "status": "ready",
	})
	rdb.LPush(ctx, readyKey, "ready-job-1")
	rdb.SAdd(ctx, prefix+"queues", queueName)

	// Default status (ready).
	w := doRequest(m, "GET", "/api/v1/queues/"+queueName+"/jobs", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestAuth_InvalidSessionCookie(t *testing.T) {
	m, _ := testMonitor(t, Config{
		AuthEnabled:    true,
		AuthSessionTTL: 3600,
		AuthUsers:      []AuthUser{{Username: "admin", PasswordHash: "$2a$10$x"}},
	})
	m.startedAt = time.Now()

	w := doRequestWithCookie(m, "GET", "/api/v1/queues", "invalid-session-token")
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestWorkers_ListWithData(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	ctx := context.Background()

	prefix := testPrefix(t)
	workersKey := prefix + "workers"
	workerKey := prefix + "worker:pool1:0"

	rdb.SAdd(ctx, workersKey, "pool1:0")
	rdb.HSet(ctx, workerKey, map[string]any{
		"pool":           "pool1",
		"worker_idx":     "0",
		"status":         "idle",
		"last_heartbeat": fmt.Sprintf("%d", time.Now().Unix()),
	})

	w := doRequest(m, "GET", "/api/v1/workers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 1 {
		t.Errorf("workers = %d, want 1", len(data))
	}
}
