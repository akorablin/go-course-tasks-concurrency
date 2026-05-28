// ============================================================
// Задача: Tee Channel — раздвоение потока  🟡 Middle
// ============================================================
//
// Реализуй аналог unix-команды `tee` для каналов:
//
//   func Tee[T any](done <-chan struct{}, in <-chan T) (<-chan T, <-chan T)
//
// Каждое значение из in должно попасть В ОБА выходных канала.
// При закрытии in — оба выхода тоже закрываются.
// При закрытии done — горутина Tee завершается без утечки.
//
// Важно: медленный читатель одного из выходов НЕ должен влиять на скорость
// отправки в другой больше чем нужно — но при этом значение всё равно должно
// попасть ОБА. Т.е. мы ждём пока оба прочитают текущее значение, потом читаем
// следующее из in. (Это простейший вариант — без буфера.)
//
// Более продвинутый вариант (бонус):
//   func TeeN[T any](done <-chan struct{}, in <-chan T, n int) []<-chan T
//   раздвоение в N выходов.
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"reflect"
	"sync"
)

// TODO: реализуй Tee
// Подсказка: наивное "out1 <- v; out2 <- v" сериализует получателей.
// Подумай как через select отправить в оба канала независимо
// (поиск: "nil channel trick" если застрял).
func Tee[T any](done <-chan struct{}, in <-chan T) (<-chan T, <-chan T) {
	out1 := make(chan T)
	out2 := make(chan T)

	go func() {
		defer close(out1)
		defer close(out2)

		for {
			select {
			case <-done:
				return
			case v, ok := <-in:
				if !ok {
					return
				}

				ch1, ch2 := out1, out2

				for ch1 != nil || ch2 != nil {
					select {
					case <-done:
						return
					case ch1 <- v:
						ch1 = nil
					case ch2 <- v:
						ch2 = nil
					}
				}
			}
		}
	}()

	return out1, out2
}

func TeeN[T any](done <-chan struct{}, in <-chan T, n int) []<-chan T {
	outs := make([]chan T, n)
	routs := make([]<-chan T, n)
	for i := range n {
		outs[i] = make(chan T)
		routs[i] = outs[i]
	}

	go func() {
		defer func() {
			for _, ch := range outs {
				close(ch)
			}
		}()

		doneCase := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)}
		inCase := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(in)}

		outValues := make([]reflect.Value, n)
		for i := range n {
			outValues[i] = reflect.ValueOf(outs[i])
		}

		var currentVal reflect.Value
		hasValue := false       // Флаг, чтобы уложиться в один цикл for
		sent := make([]bool, n) // Срез для учета выходных каналов
		pendingCount := 0       // Дополнительная проверка, что обработали все выходные каналы

		// 0 - канал done
		// 1 - канал in
		// от 2 до n+1 - выходные каналы
		cases := make([]reflect.SelectCase, 2+n)
		cases[0] = doneCase

		for {
			if !hasValue {
				cases[1] = inCase

				// Отключаем выходные каналы
				for i := range n {
					cases[2+i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.Value{}}
				}
			} else {
				// Отключаем канал in
				cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.Value{}}
				for i := range n {
					if !sent[i] {
						// Заполняем выходной канал
						cases[2+i] = reflect.SelectCase{
							Dir:  reflect.SelectSend,
							Chan: outValues[i],
							Send: currentVal,
						}
					} else {
						// В этот канал уже отправили
						cases[2+i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.Value{}}
					}
				}
			}

			// Магия reflect.Select
			chosen, recv, ok := reflect.Select(cases)

			// Получили done
			if chosen == 0 {
				return
			}

			// Получили in
			if chosen == 1 {
				if !ok {
					return
				}
				currentVal = recv
				hasValue = true
				pendingCount = n
				for i := range sent {
					sent[i] = false
				}
			} else {
				// Получаем индекс выходного канала
				actualIdx := chosen - 2
				sent[actualIdx] = true
				pendingCount--

				if pendingCount == 0 {
					hasValue = false
				}
			}
		}
	}()

	return routs
}

func main() {
	done := make(chan struct{})
	defer close(done)

	source := make(chan int)
	go func() {
		defer close(source)
		for i := 1; i <= 5; i++ {
			source <- i
		}
	}()

	// a, b := Tee(done, source)
	// var wg sync.WaitGroup
	// wg.Add(2)

	// go func() {
	// 	defer wg.Done()
	// 	for v := range a {
	// 		fmt.Println("A:", v)
	// 	}
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	for v := range b {
	// 		fmt.Println("B:", v)
	// 	}
	// }()

	// wg.Wait()

	outputs := TeeN(done, source, 3)
	var wg sync.WaitGroup
	for i, ch := range outputs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for val := range ch {
				fmt.Printf("    [Читатель %d] Получил: %d\n", i, val)
			}
			fmt.Printf("    [Читатель %d] Канал закрыт\n", i)
		}()
	}

	// Ждем завершения работы всех читателей
	wg.Wait()

}
