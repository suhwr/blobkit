package blobkit

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleflightGroup_Do(t *testing.T) {
	var g singleflightGroup
	var calls int32

	fn := func() (any, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	const concurrency = 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	results := make([]any, concurrency)
	errs := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = g.Do("test-key", fn)
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("call %d returned unexpected error: %v", i, errs[i])
		}
		if results[i] != "result" {
			t.Fatalf("call %d returned unexpected result: %v", i, results[i])
		}
	}

	// Sequential call after completion should execute again
	val, err := g.Do("test-key", func() (any, error) {
		atomic.AddInt32(&calls, 1)
		return "result2", nil
	})
	if err != nil || val != "result2" {
		t.Fatalf("subsequent call failed: %v, %v", val, err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls total, got %d", calls)
	}
}

func TestSingleflightGroup_DoError(t *testing.T) {
	var g singleflightGroup
	testErr := errors.New("sentinel failure")

	const concurrency = 5
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			val, err := g.Do("err-key", func() (any, error) {
				time.Sleep(20 * time.Millisecond)
				return nil, testErr
			})
			if val != nil {
				t.Errorf("expected nil val, got %v", val)
			}
			if !errors.Is(err, testErr) {
				t.Errorf("expected %v, got %v", testErr, err)
			}
		}()
	}

	wg.Wait()
}

func TestSingleflightGroup_PanicSafety(t *testing.T) {
	var g singleflightGroup
	key := "panic-key"

	// 1. Initial call panics
	assertPanics := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic, got none")
			}
		}()
		_, _ = g.Do(key, func() (any, error) {
			panic("intentional panic inside singleflight")
		})
	}
	assertPanics()

	// 2. Subsequent call for the SAME key must NOT deadlock or wedge
	done := make(chan bool, 1)
	go func() {
		val, err := g.Do(key, func() (any, error) {
			return "recovered-val", nil
		})
		if err != nil || val != "recovered-val" {
			t.Errorf("expected recovered-val, got %v, %v", val, err)
		}
		done <- true
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(1 * time.Second):
		t.Fatalf("singleflight wedged/deadlocked after panic on key %q", key)
	}
}

func TestSingleflightGroup_PanicPropagationToFollowers(t *testing.T) {
	var g singleflightGroup
	key := "panic-follower-key"

	started := make(chan struct{})
	release := make(chan struct{})
	followerDone := make(chan struct{})
	var followerErr error
	var followerVal any

	var wg sync.WaitGroup
	wg.Add(1)
	var leaderPanicked bool

	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				leaderPanicked = true
			}
		}()
		_, _ = g.Do(key, func() (any, error) {
			close(started)
			<-release
			panic("leader exploded")
		})
	}()

	<-started

	go func() {
		defer close(followerDone)
		followerVal, followerErr = g.Do(key, func() (any, error) {
			return "should not be called", nil
		})
	}()

	time.Sleep(10 * time.Millisecond)
	close(release)

	wg.Wait()
	<-followerDone

	if !leaderPanicked {
		t.Fatal("expected leader to panic")
	}
	if followerErr == nil {
		t.Fatal("expected follower to receive error when leader panics, got nil (phantom success)")
	}
	if followerVal != nil {
		t.Fatalf("expected follower to receive nil val, got %v", followerVal)
	}
}
