package main

import "errors"

func isDaemonChild() bool { return false }

func startDaemon(string) error {
	return errors.New("Windows requires server.daemonize=false; manage the foreground process with a Windows service wrapper")
}

func finalizeDaemon(string) error {
	return startDaemon("")
}
