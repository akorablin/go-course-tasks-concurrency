package main

import (
	"sort"
	"testing"
)

func TestProducerConsumer(t *testing.T) {
	results := producerConsumerChan(3, 4, 20, 5)
	sort.Ints(results)

	if len(results) != 20 {
		t.Fatalf("ожидали 20 результатов, получили %d", len(results))
	}

	// Проверяем что это квадраты чисел 0..19
	for i, v := range results {
		want := i * i
		if v != want {
			t.Errorf("[%d] = %d, want %d", i, v, want)
		}
	}
}

func TestProducerConsumerCond(t *testing.T) {
	results := producerConsumerCond(3, 4, 20, 5)
	sort.Ints(results)

	if len(results) != 20 {
		t.Fatalf("ожидали 20 результатов, получили %d", len(results))
	}
	for i, v := range results {
		if v != i*i {
			t.Errorf("[%d] = %d, want %d", i, v, i*i)
		}
	}
}
