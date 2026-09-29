package main

import "testing"

func TestWindowsDaemonConfigurationRejected(t *testing.T) {
	t.Setenv("_AMQP_DAEMON", "2")
	if isDaemonChild() {
		t.Fatal("Unix daemon environment must not affect the Windows foreground process")
	}
	if err := startDaemon(""); err == nil {
		t.Fatal("Unix daemon mode must fail explicitly on Windows")
	}
}
