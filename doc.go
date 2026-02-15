// Package gqm provides a Redis-based task queue library for Go.
//
// GQM (Go Queue Manager) offers worker pool management with configurable
// concurrency, timeout hierarchy, panic recovery, and graceful shutdown.
//
// Quick start:
//
//	// Producer: enqueue jobs
//	client := gqm.NewClient(gqm.WithRedisAddr("localhost:6379"))
//	client.Enqueue("email.send", gqm.Payload{"to": "user@example.com"})
//
//	// Consumer: process jobs
//	server := gqm.NewServer(gqm.WithRedisAddr("localhost:6379"))
//	server.Handle("email.send", emailHandler)
//	server.Start()
package gqm
