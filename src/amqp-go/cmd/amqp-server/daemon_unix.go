//go:build !windows

package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
)

// startDaemon turns the process into a proper Unix daemon using double-fork technique
func startDaemon(logFile string) error {
	// Check if we're already a daemon (child process)
	if os.Getenv("_AMQP_DAEMON") == "1" {
		// We're the final daemon process - just do final setup
		return setupDaemonEnvironment(logFile)
	}

	// First fork - create child process
	args := make([]string, len(os.Args))
	copy(args, os.Args)

	cmd := &exec.Cmd{
		Path: os.Args[0],
		Args: args,
		Env:  append(os.Environ(), "_AMQP_DAEMON=1"),
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon process: %v", err)
	}

	// Parent exits immediately
	fmt.Printf("AMQP server daemonized with PID %d\n", cmd.Process.Pid)
	os.Exit(0)
	return nil // Never reached
}

// setupDaemonEnvironment sets up the daemon environment for the child process
func setupDaemonEnvironment(logFile string) error {
	// Create new session
	if _, err := unix.Setsid(); err != nil {
		return fmt.Errorf("setsid failed: %v", err)
	}

	// Second fork - prevents daemon from ever acquiring controlling terminal
	cmd := &exec.Cmd{
		Path: os.Args[0],
		Args: os.Args,
		Env:  append(os.Environ(), "_AMQP_DAEMON=2"),
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("second fork failed: %v", err)
	}

	// First child exits
	os.Exit(0)
	return nil // Never reached
}

// isDaemonChild checks if this is the final daemon process
func isDaemonChild() bool {
	return os.Getenv("_AMQP_DAEMON") == "2"
}

// finalizeDaemon completes the daemonization process
func finalizeDaemon(logFile string) error {
	// Change working directory to root
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("failed to change directory to /: %v", err)
	}

	// Set file creation mask
	unix.Umask(0)

	// Redirect standard file descriptors
	if err := redirectStdFiles(logFile); err != nil {
		return fmt.Errorf("failed to redirect standard files: %v", err)
	}

	return nil
}

// redirectStdFiles redirects stdin to /dev/null and stdout/stderr to log file or /dev/null
func redirectStdFiles(logFile string) error {
	// Redirect stdin to /dev/null
	devNull, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %v", err)
	}

	// Duplicate stdin to /dev/null
	if err := unix.Dup2(int(devNull.Fd()), int(os.Stdin.Fd())); err != nil {
		devNull.Close()
		return fmt.Errorf("failed to redirect stdin: %v", err)
	}

	// Handle stdout and stderr
	var outputFile *os.File
	if logFile != "" {
		// Redirect to log file
		outputFile, err = os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			devNull.Close()
			return fmt.Errorf("failed to open log file %s: %v", logFile, err)
		}
	} else {
		// Redirect to /dev/null if no log file specified
		outputFile = devNull
	}

	// Redirect stdout and stderr
	if err := unix.Dup2(int(outputFile.Fd()), int(os.Stdout.Fd())); err != nil {
		outputFile.Close()
		devNull.Close()
		return fmt.Errorf("failed to redirect stdout: %v", err)
	}

	if err := unix.Dup2(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		outputFile.Close()
		devNull.Close()
		return fmt.Errorf("failed to redirect stderr: %v", err)
	}

	// Don't close the files as they're now being used by the standard descriptors
	return nil
}
