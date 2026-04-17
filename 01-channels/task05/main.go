// ============================================================
// Задача: Merge N Channels  🟡 Middle
// ============================================================
//
// Реализуй три варианта слияния каналов:
//
//   1. merge2(a, b <-chan int) <-chan int
//      Сливает ровно 2 канала. Используй select.
//
//   2. mergeN(channels ...<-chan int) <-chan int
//      Сливает произвольное количество каналов.
//      Закрывает выходной канал когда все входные закрыты.
//
//   3. mergeOrdered(channels ...<-chan int) <-chan int
//      Сливает N каналов СОХРАНЯЯ ОТНОСИТЕЛЬНЫЙ ПОРЯДОК внутри каждого.
//      Т.е. если channel[0] выдаёт 1,3,5 и channel[1] выдаёт 2,4,6,
//      то выходной может дать 1,2,3,4,5,6 или 1,3,2,4,5,6 (нет гарантий между каналами)
//      но никогда 3,1,...  (нарушение порядка внутри одного канала)
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sort"
	"sync"
)

// TODO: реализуй merge2
func merge2(a, b <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		// TODO: используй for + select с nil-каналами для завершения
		for a != nil || b != nil {
			select {
			case v, ok := <-a:
				if !ok {
					a = nil // nil-канал никогда не выбирается в select
					continue
				}
				out <- v
			case v, ok := <-b:
				if !ok {
					b = nil
					continue
				}
				out <- v
			}
		}
	}()
	return out
}

// TODO: реализуй mergeN через sync.WaitGroup
func mergeN(channels ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup

	// TODO: на каждый канал — горутина
	for i := range channels {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for channels[i] != nil {
				v, ok := <-channels[i]
				if !ok {
					channels[i] = nil
					continue
				}
				out <- v
			}
		}()
	}

	// TODO: WaitGroup.Wait() в отдельной горутине, потом close(out)
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// TODO: реализуй mergeOrdered
// Подсказка: для каждого входного канала запусти горутину
// которая читает значения последовательно — это гарантирует порядок внутри канала
func mergeOrdered(channels ...<-chan int) <-chan int {
	// По сути то же что mergeN — горутины на каждый канал уже гарантируют
	// что значения из одного канала не переставятся
	return mergeN(channels...)
}

func sourceChan(nums ...int) <-chan int {
	ch := make(chan int, len(nums))
	for _, n := range nums {
		ch <- n
	}
	close(ch)
	return ch
}

func main() {
	a := sourceChan(1, 3, 5)
	b := sourceChan(2, 4, 6)

	var result []int
	for v := range merge2(a, b) {
		result = append(result, v)
	}
	sort.Ints(result)
	fmt.Println("merge2:", result) // [1 2 3 4 5 6]

	channels := make([]<-chan int, 4)
	for i := range channels {
		start := i*5 + 1
		nums := make([]int, 5)
		for j := range nums {
			nums[j] = start + j
		}
		channels[i] = sourceChan(nums...)
	}

	var result2 []int
	for v := range mergeN(channels...) {
		result2 = append(result2, v)
	}
	sort.Ints(result2)
	fmt.Println("mergeN:", result2) // [1 2 3 ... 20]
}
