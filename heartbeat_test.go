package gqm

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestHeartbeat_SendHeartbeat_WritesPoolData(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:hb:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.rc.Close()

	err = server.Pool(PoolConfig{
		Name:        "hb-pool",
		JobTypes:    []string{"email.send"},
		Queues:      []string{"email"},
		Concurrency: 3,
	})
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}

	p := server.pools[0]
	ctx := context.Background()

	// Call sendHeartbeat directly
	p.sendHeartbeat(ctx)

	// Verify pool was added to workers set
	isMember, err := server.rc.rdb.SIsMember(ctx, server.rc.Key("workers"), "hb-pool").Result()
	if err != nil {
		t.Fatalf("SIsMember: %v", err)
	}
	if !isMember {
		t.Error("pool not added to workers set")
	}

	// Verify worker hash fields
	data, err := server.rc.rdb.HGetAll(ctx, server.rc.Key("worker", "hb-pool")).Result()
	if err != nil {
		t.Fatalf("HGetAll worker: %v", err)
	}

	if data["id"] != "hb-pool" {
		t.Errorf("id = %q, want %q", data["id"], "hb-pool")
	}
	if data["pool"] != "hb-pool" {
		t.Errorf("pool = %q, want %q", data["pool"], "hb-pool")
	}
	if data["queues"] != "email" {
		t.Errorf("queues = %q, want %q", data["queues"], "email")
	}
	if data["status"] != "active" {
		t.Errorf("status = %q, want %q", data["status"], "active")
	}
	if data["concurrency"] != "3" {
		t.Errorf("concurrency = %q, want %q", data["concurrency"], "3")
	}
	if data["last_heartbeat"] == "" {
		t.Error("last_heartbeat not set")
	}

	// Verify TTL is set (3x heartbeat interval)
	ttl, err := server.rc.rdb.TTL(ctx, server.rc.Key("worker", "hb-pool")).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("worker key TTL = %v, want > 0", ttl)
	}
	expectedTTL := 3 * defaultHeartbeatInterval
	if ttl > expectedTTL+time.Second {
		t.Errorf("worker key TTL = %v, want <= %v", ttl, expectedTTL)
	}
}

func TestHeartbeat_SendHeartbeat_UpdatesActiveJobs(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:hbj:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.rc.Close()

	err = server.Pool(PoolConfig{
		Name:        "hbj-pool",
		JobTypes:    []string{"test.job"},
		Queues:      []string{"default"},
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}

	p := server.pools[0]
	ctx := context.Background()

	// Create a job hash in Redis that the heartbeat will update
	jobKey := server.rc.Key("job", "job-active-1")
	server.rc.rdb.HSet(ctx, jobKey, "id", "job-active-1", "status", "processing")

	// Simulate active job: set worker 0 to job-active-1
	p.mu.Lock()
	p.activeJobs[0] = "job-active-1"
	p.mu.Unlock()

	// Send heartbeat — should update job's last_heartbeat
	p.sendHeartbeat(ctx)

	hb, err := server.rc.rdb.HGet(ctx, jobKey, "last_heartbeat").Result()
	if err != nil {
		t.Fatalf("HGet last_heartbeat: %v", err)
	}
	if hb == "" {
		t.Error("job last_heartbeat not updated by heartbeat")
	}
}

func TestHeartbeat_SendHeartbeat_SkipsEmptyActiveJobs(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:hbe:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.rc.Close()

	err = server.Pool(PoolConfig{
		Name:        "hbe-pool",
		JobTypes:    []string{"test.job"},
		Queues:      []string{"default"},
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}

	p := server.pools[0]
	ctx := context.Background()

	// No active jobs — sendHeartbeat should still succeed (no panic/error)
	p.sendHeartbeat(ctx)

	// Verify worker hash is still written
	exists, err := server.rc.rdb.Exists(ctx, server.rc.Key("worker", "hbe-pool")).Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists != 1 {
		t.Error("worker key should exist even with no active jobs")
	}
}

func TestHeartbeat_SendHeartbeat_MultipleQueues(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:hbm:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.rc.Close()

	err = server.Pool(PoolConfig{
		Name:        "multi-pool",
		JobTypes:    []string{"email.send", "sms.send"},
		Queues:      []string{"email", "sms", "default"},
		Concurrency: 5,
	})
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}

	p := server.pools[0]
	ctx := context.Background()

	p.sendHeartbeat(ctx)

	queues, err := server.rc.rdb.HGet(ctx, server.rc.Key("worker", "multi-pool"), "queues").Result()
	if err != nil {
		t.Fatalf("HGet queues: %v", err)
	}
	if queues != "email,sms,default" {
		t.Errorf("queues = %q, want %q", queues, "email,sms,default")
	}
}

func TestHeartbeat_Loop_ContextCancellation(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:hbl:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.rc.Close()

	err = server.Pool(PoolConfig{
		Name:        "hbl-pool",
		JobTypes:    []string{"test.job"},
		Queues:      []string{"default"},
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Pool: %v", err)
	}

	p := server.pools[0]
	ctx, cancel := context.WithCancel(context.Background())

	// Run heartbeatLoop in a goroutine
	done := make(chan struct{})
	go func() {
		p.heartbeatLoop(ctx)
		close(done)
	}()

	// Cancel quickly — heartbeatLoop should exit
	cancel()
	select {
	case <-done:
		// OK — heartbeatLoop exited
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit after context cancellation")
	}
}
