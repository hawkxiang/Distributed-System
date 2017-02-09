package raft

import (
	"math/rand"
	"time"
)

func random(min, max time.Duration) <-chan time.Time {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(r.Int63n(int64(delta)))
	}
	return time.After(d)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
