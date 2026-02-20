package gqm

import (
	"context"
	"testing"
)

func TestIsFailure_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil predicate")
		}
	}()
	IsFailure(nil)
}

func TestIsFailure_Valid(t *testing.T) {
	fn := func(err error) bool { return true }
	opt := IsFailure(fn)
	cfg := &handlerConfig{}
	opt(cfg)
	if cfg.isFailure == nil {
		t.Error("isFailure should be set")
	}
}

func TestOnSuccess_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil callback")
		}
	}()
	OnSuccess(nil)
}

func TestOnSuccess_Valid(t *testing.T) {
	fn := func(ctx context.Context, job *Job) {}
	opt := OnSuccess(fn)
	cfg := &handlerConfig{}
	opt(cfg)
	if cfg.onSuccess == nil {
		t.Error("onSuccess should be set")
	}
}

func TestOnFailure_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil callback")
		}
	}()
	OnFailure(nil)
}

func TestOnFailure_Valid(t *testing.T) {
	fn := func(ctx context.Context, job *Job, err error) {}
	opt := OnFailure(fn)
	cfg := &handlerConfig{}
	opt(cfg)
	if cfg.onFailure == nil {
		t.Error("onFailure should be set")
	}
}

func TestOnComplete_NilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil callback")
		}
	}()
	OnComplete(nil)
}

func TestOnComplete_Valid(t *testing.T) {
	fn := func(ctx context.Context, job *Job, err error) {}
	opt := OnComplete(fn)
	cfg := &handlerConfig{}
	opt(cfg)
	if cfg.onComplete == nil {
		t.Error("onComplete should be set")
	}
}

func TestWorkers_Valid(t *testing.T) {
	opt := Workers(10)
	cfg := &handlerConfig{}
	opt(cfg)
	if cfg.workers != 10 {
		t.Errorf("workers = %d, want 10", cfg.workers)
	}
}
