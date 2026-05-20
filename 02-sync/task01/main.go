// ============================================================
// Задача: Потокобезопасный TTL-кеш на RWMutex  🟡 Middle
// ============================================================
//
// Частый вопрос на собесах уровня Middle.
//
// Реализуй TTLCache — кеш с временем жизни записей.
//
//   type TTLCache[K comparable, V any] struct { ... }
//
//   func NewTTLCache[K comparable, V any](ttl time.Duration) *TTLCache[K, V]
//   func (c *TTLCache[K, V]) Set(key K, value V)
//   func (c *TTLCache[K, V]) Get(key K) (V, bool)
//   func (c *TTLCache[K, V]) Delete(key K)
//   func (c *TTLCache[K, V]) Len() int
//
// Требования:
//   - Get использует RLock (параллельное чтение)
//   - Set и Delete используют Lock (эксклюзивная запись)
//   - Get возвращает (zero, false) для устаревших записей
//   - Устаревшие записи удаляются лениво (при следующем Get) ???
//   - Дополнительно: метод Cleanup() удаляет все устаревшие записи
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"time"
)

type entry[V any] struct {
	value     V
	expiry    time.Time
	isDeleted bool
}

type TTLCache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]entry[V]
	ttl   time.Duration
}

// TODO: реализуй NewTTLCache
func NewTTLCache[K comparable, V any](ttl time.Duration) *TTLCache[K, V] {
	return &TTLCache[K, V]{
		items: make(map[K]entry[V]),
		ttl:   ttl,
	}
}

// TODO: реализуй Set — записывает значение с временем жизни ttl
func (c *TTLCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO: сохрани entry с expiry = time.Now().Add(c.ttl)
	c.items[key] = entry[V]{
		value:     value,
		expiry:    time.Now().Add(c.ttl),
		isDeleted: false,
	}
}

// TODO: реализуй Get — возвращает значение если оно есть и не устарело
func (c *TTLCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock() // TODO: поменяй на RLock, но нужен апгрейд до Lock если запись устарела
	v, ok := c.items[key]
	c.mu.RUnlock()

	var zero V
	if !ok {
		return zero, false
	}

	if v.isDeleted {
		c.Delete(key)
		return zero, false
	} else {
		// TODO: проверь entry.expiry.After(time.Now())
		// Если устарело — удали из map и верни zero, false
		now := time.Now()
		if now.After(v.expiry) {
			c.items[key] = entry[V]{
				value:     v.value,
				expiry:    v.expiry,
				isDeleted: true,
			}
			return zero, false
		}

		return v.value, true
	}
}

// TODO: реализуй Delete
func (c *TTLCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if e, ok := c.items[key]; ok && now.After(e.expiry) {
		delete(c.items, key)
	}
}

// TODO: реализуй Len — количество ЖИВЫХ записей
func (c *TTLCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// TODO: считай только не устаревшие записи
	count := 0
	for _, e := range c.items {
		if e.expiry.After(time.Now()) {
			count++
		}
	}
	return count
}

// TODO: реализуй Cleanup — удаляет все устаревшие записи
func (c *TTLCache[K, V]) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.items {
		if now.After(e.expiry) {
			delete(c.items, k)
		}
	}
}

func main() {
	cache := NewTTLCache[string, int](100 * time.Millisecond)

	cache.Set("a", 1)
	cache.Set("b", 2)

	if v, ok := cache.Get("a"); ok {
		fmt.Printf("a = %d\n", v)
	}

	fmt.Printf("Len = %d\n", cache.Len()) // 2

	time.Sleep(150 * time.Millisecond)

	_, ok := cache.Get("a")
	fmt.Printf("a после TTL: ok=%v\n", ok) // false

	fmt.Printf("Len после TTL = %d\n", cache.Len()) // 0
}
