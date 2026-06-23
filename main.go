package main

import (
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// queue хранит готовые сообщения и FIFO-очередь ожидающих получателей.
type queue struct {
	messages []string
	waiters  []chan string
}

var (
	mu     sync.Mutex
	queues = map[string]*queue{}
)

func getQueue(name string) *queue {
	q := queues[name]
	if q == nil {
		q = &queue{}
		queues[name] = q
	}
	return q
}

// put отдаёт сообщение первому ожидающему получателю либо кладёт его в очередь.
func put(name, msg string) {
	mu.Lock()
	defer mu.Unlock()
	q := getQueue(name)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w <- msg
		return
	}
	q.messages = append(q.messages, msg)
}

// get забирает сообщение по FIFO; при пустой очереди ждёт до timeout.
func get(name string, timeout time.Duration) (string, bool) {
	mu.Lock()
	q := getQueue(name)
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		mu.Unlock()
		return msg, true
	}
	if timeout <= 0 {
		mu.Unlock()
		return "", false
	}
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	mu.Unlock()

	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		mu.Lock()
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				mu.Unlock()
				return "", false
			}
		}
		mu.Unlock()
		return <-ch, true // сообщение успели передать прямо во время таймаута
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]
	switch r.Method {
	case http.MethodPut:
		v, ok := r.URL.Query()["v"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		put(name, v[0])
	case http.MethodGet:
		var timeout time.Duration
		if t, err := strconv.Atoi(r.URL.Query().Get("timeout")); err == nil {
			timeout = time.Duration(t) * time.Second
		}
		msg, ok := get(name, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write([]byte(msg))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}
	http.HandleFunc("/", handler)
	if err := http.ListenAndServe(":"+os.Args[1], nil); err != nil {
		os.Exit(1)
	}
}
