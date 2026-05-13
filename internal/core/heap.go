package core

import "time"

type expirationItem struct {
	LeaseID   string
	ExpiresAt time.Time
}

type expirationHeap []expirationItem

func (h expirationHeap) Len() int {
	return len(h)
}

func (h expirationHeap) Less(i, j int) bool {
	return h[i].ExpiresAt.Before(h[j].ExpiresAt)
}

func (h expirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *expirationHeap) Push(x any) {
	*h = append(*h, x.(expirationItem))
}

func (h *expirationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h expirationHeap) peek() (expirationItem, bool) {
	if len(h) == 0 {
		return expirationItem{}, false
	}
	return h[0], true
}
