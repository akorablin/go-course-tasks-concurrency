package main

import (
	"strings"
	"sync"
	"testing"
)

// === Тесты ===

func testFooBar(t *testing.T, n int, foo func(func()), bar func(func())) {
	var mu sync.Mutex
	var sb strings.Builder
	var wg sync.WaitGroup
	wg.Add(2)

	go func() { defer wg.Done(); foo(func() { mu.Lock(); sb.WriteString("Foo"); mu.Unlock() }) }()
	go func() { defer wg.Done(); bar(func() { mu.Lock(); sb.WriteString("Bar"); mu.Unlock() }) }()

	wg.Wait()
	result := sb.String()
	expected := strings.Repeat("FooBar", n)
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestFooBarChan(t *testing.T) {
	fb := NewFooBarChan(5)
	testFooBar(t, 5, fb.Foo, fb.Bar)
}

func TestFooBarMutex(t *testing.T) {
	fb := NewFooBarMutex(5)
	testFooBar(t, 5, fb.Foo, fb.Bar)
}
