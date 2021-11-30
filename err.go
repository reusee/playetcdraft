package main

import "errors"

var (
	ErrNodeNotFound = errors.New("node not found")
	ErrKeyNotFound  = errors.New("key not found")
)
