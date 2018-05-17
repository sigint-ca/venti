package venti

import "log"

var Debug = false

func dprintf(fmt string, args ...interface{}) {
	if Debug {
		log.Printf(fmt, args...)
	}
}
