package main

import (
	"errors"
	"sync"
)

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

var ErrorNoSuchKey = errors.New("no such key")

func Delete(key string) error {
	store.Lock()
	defer store.Unlock()

	delete(store.m, key)

	return nil
}

func Get(key string) (string, error) {

	store.RLock()
	defer store.RUnlock()

	value, ok := store.m[key]

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Put(key string, value string) error {
	store.Lock()
	defer store.Unlock()

	store.m[key] = value

	return nil
}
