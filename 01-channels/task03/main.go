// ============================================================
// Задача: Done Channel — отмена цепочки горутин  🟡 Middle
// ============================================================
//
// Классический вопрос на собеседованиях уровня Middle.
//
// У тебя есть пайплайн из трёх стадий. При вызове cancel()
// весь пайплайн должен немедленно завершиться без утечек горутин.
//
// Реализуй:
//   1. withDone(done <-chan struct{}, in <-chan int) <-chan int
//      Оборачивает любой канал — останавливает чтение при закрытии done
//
//   2. Перепиши generate, square, filterEven используя withDone
//      (или добавь done параметр напрямую)
//
// Сценарии:
//   A) Читаем 3 значения, потом cancel() — остальные значения дропаются
//   B) cancel() до начала чтения — ни одного значения не получаем
//
// Проверь:
//   go test -race -v ./...
//
// Ожидаемый вывод:
//   Получено 3 значения: [4 16 36]
//   Горутин после отмены: N (должно быть близко к начальному)

package main

import (
	"fmt"
	"runtime"
	"time"
)

// withDone оборачивает канал in — при закрытии done прекращает чтение.
// TODO: реализуй
func withDone(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-done:
					return
				}
			}
		}
	}()
	return out
}

func generate(done <-chan struct{}, nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for _, n := range nums {
			select {
			case out <- n:
			case <-done:
				return
			}
		}
	}()
	return out
}

func filterEven(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			if n%2 == 0 {
				select {
				case out <- n:
				case <-done:
					return
				}
			}
		}
	}()
	return out
}

// TODO: добавь параметр done в square
func square(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			// TODO: проверяй done перед отправкой
			select {
			case out <- n * n:
			case <-done:
				return
			}
		}
	}()
	return out
}

func main() {
	goroutinesBefore := runtime.NumGoroutine()
	fmt.Printf("Горутин в начале: %d\n", goroutinesBefore)

	done := make(chan struct{})

	nums := make([]int, 100)
	for i := range nums {
		nums[i] = i + 1
	}

	results := square(done, filterEven(done, generate(done, nums...)))

	// Читаем только 3 значения, потом отменяем
	var got []int
	for v := range results {
		got = append(got, v)
		if len(got) == 3 {
			close(done) // отменяем пайплайн
			break
		}
	}

	fmt.Printf("Получено %d значений: %v\n", len(got), got)

	// Даём горутинам время завершиться
	time.Sleep(50 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	fmt.Printf("Горутин после отмены: %d (было %d)\n", goroutinesAfter, goroutinesBefore)

	if goroutinesAfter > goroutinesBefore+2 {
		fmt.Println("⚠ Возможна утечка горутин!")
	} else {
		fmt.Println("✓ Утечек горутин нет")
	}
}
