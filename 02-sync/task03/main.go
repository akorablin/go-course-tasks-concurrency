// ============================================================
// Задача: Взвешенный семафор  🟡 Middle
// ============================================================
//
// Вопрос с собесов уровня Middle.
//
// Реализуй Semaphore с поддержкой "веса" (weighted semaphore):
//   - Ресурс имеет ёмкость N
//   - Acquire(n) захватывает n единиц. Блокируется если доступно < n.
//   - Release(n) возвращает n единиц.
//   - TryAcquire(n) — non-blocking: захватывает или возвращает false
//
// Примеры использования:
//   - Ограничение числа параллельных HTTP-запросов (каждый = 1 единица)
//   - Управление памятью (запрос на 10МБ = 10 единиц)
//   - Rate limiting по "стоимости" операции
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Semaphore struct {
	mu sync.Mutex
	ch chan struct{}
}

// NewSemaphore создаёт семафор с ёмкостью n.
func NewSemaphore(n int) *Semaphore {
	ch := make(chan struct{}, n)
	for range n {
		ch <- struct{}{}
	}
	return &Semaphore{ch: ch}
}

// Acquire блокирующий захват n единиц.
// TODO: реализуй через цикл с чтением из ch
func (s *Semaphore) Acquire(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for range n {
		<-s.ch
	}
}

// AcquireContext захват с контекстом — можно отменить.
// TODO: реализуй — если ctx отменён до получения всех n единиц,
//
//	верни уже захваченные обратно и вернуть ctx.Err()
func (s *Semaphore) AcquireContext(ctx context.Context, n int) error {
	acquired := 0
	for range n {
		select {
		case <-s.ch:
			acquired++
		case <-ctx.Done():
			// Возвращаем уже захваченное
			s.Release(acquired)
			return ctx.Err()
		}
	}
	return nil
}

// TryAcquire non-blocking захват. Возвращает false если доступно < n.
// TODO: реализуй
func (s *Semaphore) TryAcquire(n int) bool {
	// Подсказка: проверь len(s.ch), потом попробуй захватить
	if len(s.ch) < n {
		return false
	}
	// TODO: захвати через default в select
	for range n {
		select {
		case <-s.ch:
		default:
			s.Release(n - 1) // вернём уже взятые
			return false
		}
	}
	return true
}

// Release возвращает n единиц.
func (s *Semaphore) Release(n int) {
	for range n {
		s.ch <- struct{}{}
	}
}

// Available возвращает количество свободных единиц.
func (s *Semaphore) Available() int {
	return len(s.ch)
}

func main() {
	sem := NewSemaphore(3)
	var wg sync.WaitGroup

	// 5 задач, каждая занимает 1 единицу, не более 3 одновременно
	for i := 0; i < 5; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()
			sem.Acquire(1)
			defer sem.Release(1)
			fmt.Printf("задача %d выполняется (доступно: %d)\n", n, sem.Available())
			time.Sleep(100 * time.Millisecond)
		}()
	}
	wg.Wait()
}
