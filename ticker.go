package main

import "time"

func (_ Global) Ticker() *time.Ticker {
	return time.NewTicker(time.Millisecond * 100)
}
