package main

import (
	"strings"
	"sync"
	"testing"
)

// === Тесты ===

func runInOrder(first, second, third func(func())) string {
	var sb strings.Builder
	var wg sync.WaitGroup
	wg.Add(3)

	// Запускаем в "неправильном" порядке
	go func() { defer wg.Done(); third(func() { sb.WriteString("third") }) }()
	go func() { defer wg.Done(); second(func() { sb.WriteString("second") }) }()
	go func() { defer wg.Done(); first(func() { sb.WriteString("first") }) }()

	wg.Wait()
	return sb.String()
}

func TestOrderChan(t *testing.T) {
	p := NewOrderedPrinterChan()
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}

func TestOrderWG(t *testing.T) {
	p := NewOrderedPrinterWG()
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}

func TestOrderAtomic(t *testing.T) {
	p := &OrderedPrinterAtomic{}
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}
