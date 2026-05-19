// ============================================================
// Задача: Producer-Consumer с bounded buffer  🟡 Middle
// ============================================================
//
// Классика на собесах Junior/Middle уровня.
//
// Реализуй через каналы:
//   - M производителей генерируют числа 0..N
//   - K потребителей читают, возводят в квадрат, пишут в results
//   - Буфер между ними ограничен (размер B)
//
// Требования:
//   - Потребители завершаются когда производители закончили И буфер пуст
//   - Нет утечек горутин
//   - Все числа должны быть обработаны ровно один раз
//
// Реализуй ДВА варианта:
//   1. Через каналы (идиоматично в Go)
//   2. Через sync.Cond (для понимания классических примитивов)
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// === Вариант 1: через каналы ===

// TODO: реализуй producerConsumerChan
// Подсказка: два буферизованных канала и два WaitGroup — для производителей и потребителей
func producerConsumerChan(producers, consumers, n, bufSize int) []int {
	dataChan := make(chan int, bufSize)
	resultsChan := make(chan int, n)

	var wgProducers sync.WaitGroup
	var wgConsumers sync.WaitGroup

	// Глобальный счетчик для условия "M производителей генерируют числа 0..N"
	var globalCounter int64 = 0

	// Производители
	for i := 0; i < producers; i++ {
		wgProducers.Add(1)
		go func() {
			defer wgProducers.Done()
			for {
				val := atomic.AddInt64(&globalCounter, 1) - 1
				if val >= int64(n) {
					break
				}
				dataChan <- int(val)
			}
		}()
	}

	// Потребители
	for i := 0; i < consumers; i++ {
		wgConsumers.Add(1)
		go func() {
			defer wgConsumers.Done()
			for val := range dataChan {
				resultsChan <- val * val
			}
		}()
	}

	go func() {
		wgProducers.Wait()
		close(dataChan)
	}()

	wgConsumers.Wait()
	close(resultsChan)

	var result []int
	for res := range resultsChan {
		result = append(result, res)
	}

	return result
}

// === Вариант 2: через sync.Cond ===

// TODO: реализуй producerConsumerCond
// Подсказка: буфер — обычный срез; производители ждут пока буфер полон, потребители — пока пуст
func producerConsumerCond(producers, consumers, n, bufSize int) []int {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	buffer := make([]int, 0, bufSize)
	results := make([]int, 0, n)

	var wgProducers sync.WaitGroup
	var wgConsumers sync.WaitGroup

	var globalCounter int64 = 0
	var producersFinished bool

	// Производители
	for i := 0; i < producers; i++ {
		wgProducers.Add(1)
		go func() {
			defer wgProducers.Done()
			for {
				val := atomic.AddInt64(&globalCounter, 1) - 1
				if val >= int64(n) {
					break
				}

				mu.Lock()
				for len(buffer) == bufSize {
					cond.Wait()
				}

				buffer = append(buffer, int(val))

				cond.Broadcast()
				mu.Unlock()
			}
		}()
	}

	// Потребители
	for i := 0; i < consumers; i++ {
		wgConsumers.Add(1)
		go func() {
			defer wgConsumers.Done()
			for {
				mu.Lock()
				for len(buffer) == 0 && producersFinished == false {
					cond.Wait()
				}

				if len(buffer) == 0 && producersFinished == true {
					mu.Unlock()
					return
				}

				val := buffer[0]
				buffer = buffer[1:]
				val *= val

				results = append(results, val)

				cond.Broadcast()
				mu.Unlock()
			}
		}()
	}

	go func() {
		wgProducers.Wait()
		mu.Lock()
		producersFinished = true
		cond.Broadcast()
		mu.Unlock()
	}()

	wgConsumers.Wait()

	return results
}

func main() {
	resultsChan := producerConsumerChan(2, 3, 10, 3)
	sort.Ints(resultsChan)
	fmt.Println("Результаты:", resultsChan)

	resultsCond := producerConsumerCond(2, 3, 10, 3)
	sort.Ints(resultsCond)
	fmt.Println("Результаты:", resultsCond)
}
