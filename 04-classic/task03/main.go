// ============================================================
// Задача: Print In Order — LeetCode 1114  🟢 Junior
// ============================================================
//
// Задача с LeetCode, часто задают на собесах Junior-уровня.
//
// Три метода: first(), second(), third() запускаются в произвольном порядке
// в разных горутинах. Гарантируй что они выполнятся строго в порядке: 1 → 2 → 3.
//
// Реализуй через:
//   A) каналы (простейший способ)
//   B) sync.WaitGroup
//   C) atomic + spin (для понимания, не для продакшна)
//
// Проверь через тест что порядок всегда правильный при любом порядке запуска горутин.

package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// === Вариант A: через каналы ===

type OrderedPrinterChan struct {
	after1 chan struct{}
	after2 chan struct{}
}

// TODO: реализуй NewOrderedPrinterChan
// Подсказка: нужны сигналы "первая уже отработала" и "вторая уже отработала"
func NewOrderedPrinterChan() *OrderedPrinterChan {
	return &OrderedPrinterChan{
		after1: make(chan struct{}),
		after2: make(chan struct{}),
	}
}

// TODO: реализуй First — вызови fn и сигнализируй что можно запускать Second
func (p *OrderedPrinterChan) First(fn func()) {
	fn()
	close(p.after1)
}

// TODO: реализуй Second — дождись сигнала от First, вызови fn, сигнализируй Third
func (p *OrderedPrinterChan) Second(fn func()) {
	<-p.after1
	fn()
	close(p.after2)
}

// TODO: реализуй Third — дождись сигнала от Second и вызови fn
func (p *OrderedPrinterChan) Third(fn func()) {
	<-p.after2
	fn()
}

// === Вариант B: через WaitGroup ===

type OrderedPrinterWG struct {
	wg1 sync.WaitGroup
	wg2 sync.WaitGroup
}

func NewOrderedPrinterWG() *OrderedPrinterWG {
	p := &OrderedPrinterWG{}
	p.wg1.Add(1)
	p.wg2.Add(1)
	return p
}

// TODO: реализуй First, Second, Third через WaitGroup
func (p *OrderedPrinterWG) First(fn func()) {
	fn()
	p.wg1.Done()
}
func (p *OrderedPrinterWG) Second(fn func()) {
	p.wg1.Wait()
	fn()
	p.wg2.Done()
}
func (p *OrderedPrinterWG) Third(fn func()) {
	p.wg2.Wait()
	fn()
}

// === Вариант C: через atomic ===

type OrderedPrinterAtomic struct {
	state atomic.Int32
}

func NewOrderedPrinterAtomic() *OrderedPrinterAtomic {
	return &OrderedPrinterAtomic{}
}

// TODO: реализуй через spin-ожидание atomic
func (p *OrderedPrinterAtomic) First(fn func()) {
	fn()
	p.state.CompareAndSwap(0, 1)
}
func (p *OrderedPrinterAtomic) Second(fn func()) {
	for p.state.Load() != 1 {
		runtime.Gosched()
	}
	fn()
	p.state.CompareAndSwap(1, 2)
}
func (p *OrderedPrinterAtomic) Third(fn func()) {
	for p.state.Load() != 2 {
		runtime.Gosched()
	}
	fn()
}

func main() {
	p1 := NewOrderedPrinterChan()
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); p1.Third(func() { fmt.Print("third ") }) }()
	go func() { defer wg.Done(); p1.First(func() { fmt.Print("first ") }) }()
	go func() { defer wg.Done(); p1.Second(func() { fmt.Print("second ") }) }()

	wg.Wait()
	fmt.Println()

	// --------------------

	p2 := NewOrderedPrinterWG()
	wg.Add(3)

	go func() { defer wg.Done(); p2.Third(func() { fmt.Print("third ") }) }()
	go func() { defer wg.Done(); p2.First(func() { fmt.Print("first ") }) }()
	go func() { defer wg.Done(); p2.Second(func() { fmt.Print("second ") }) }()

	wg.Wait()
	fmt.Println()

	// --------------------

	p3 := NewOrderedPrinterAtomic()
	wg.Add(3)

	go func() { defer wg.Done(); p3.Third(func() { fmt.Print("third ") }) }()
	go func() { defer wg.Done(); p3.First(func() { fmt.Print("first ") }) }()
	go func() { defer wg.Done(); p3.Second(func() { fmt.Print("second ") }) }()

	wg.Wait()
	fmt.Println()

}
