// ============================================================
// Задача: Fan-Out / Fan-In  🟡 Middle
// ============================================================
//
// Реализуй паттерн "распределить и собрать":
//
//   1. fanOut(in <-chan int, n int) []<-chan int
//      Распределяет задачи из одного канала по n воркерам.
//      Каждый воркер получает примерно равное количество задач.
//
//   2. fanIn(channels ...<-chan int) <-chan int
//      Сливает несколько каналов в один.
//      Закрывает выходной канал когда все входные закрыты.
//
//   3. process(in <-chan int) <-chan int
//      Воркер: умножает число на 2. Имитирует задержку.
//
// Схема:
//   source → fanOut → [worker1, worker2, worker3] → fanIn → results
//
// Требования:
//   - Порядок результатов не важен (параллельная обработка)
//   - Нет утечек горутин (проверяй через runtime.NumGoroutine)
//   - Работает с -race без ошибок
//
// Ожидаемый вывод (порядок может отличаться):
//   Обработано 10 задач. Сумма: 110  (1+2+...+10)*2 = 110

package main

import (
	"fmt"
	"sync"
	"time"
)

// process имитирует "тяжёлую" работу: удваивает число
func process(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			time.Sleep(10 * time.Millisecond) // имитация работы
			out <- n * 2
		}
	}()
	return out
}

// TODO: реализуй fanOut — раздай задачи n воркерам
// Подсказка: используй sync.WaitGroup чтобы закрыть каналы воркеров
func fanOut(in <-chan int, n int) []<-chan int {
	channels := make([]<-chan int, n)
	// TODO: создай n каналов
	// TODO: запусти горутину которая распределяет данные из in по каналам round-robin
	// TODO: закрой все каналы когда in закрыт
	_ = in
	return channels
}

// TODO: реализуй fanIn — слей все каналы в один
// Подсказка: на каждый входной канал запусти горутину
// Используй sync.WaitGroup чтобы закрыть выходной канал
func fanIn(channels ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup

	// TODO: для каждого канала запусти горутину которая читает и пишет в out
	_ = wg
	_ = channels

	// TODO: когда все горутины завершатся — закрой out

	return out
}

func main() {
	const numJobs = 10
	const numWorkers = 3

	// Источник задач
	source := make(chan int, numJobs)
	for i := 1; i <= numJobs; i++ {
		source <- i
	}
	close(source)

	// Распределяем и обрабатываем
	workers := fanOut(source, numWorkers)
	var processedChans []<-chan int
	for _, w := range workers {
		processedChans = append(processedChans, process(w))
	}

	// Собираем результаты
	sum := 0
	count := 0
	for result := range fanIn(processedChans...) {
		sum += result
		count++
	}

	fmt.Printf("Обработано %d задач. Сумма: %d\n", count, sum)
	// Ожидаемо: Обработано 10 задач. Сумма: 110
}
