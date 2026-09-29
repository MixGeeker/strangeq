package main

import (
	"errors"
	"io"
	"os"

	"golang.org/x/crypto/bcrypt"
)

func hashPassword(input io.Reader, output io.Writer) error {
	password, err := io.ReadAll(io.LimitReader(input, 73))
	if err != nil || len(password) == 0 || len(password) > 72 {
		return errors.New("PASSWORD_INPUT_INVALID")
	}
	defer clear(password)
	hash, err := bcrypt.GenerateFromPassword(password, 12)
	if err != nil {
		return err
	}
	_, err = output.Write(append(hash, '\n'))
	return err
}

type supervisedServer interface {
	StartWithQuitChannel(<-chan struct{}) error
	Stop() error
}

func runServer(server supervisedServer, signals <-chan os.Signal, input io.Reader) error {
	quit, finished := make(chan struct{}), make(chan struct{})
	defer close(finished)
	var eof <-chan struct{}
	if input != nil {
		closed := make(chan struct{})
		eof = closed
		go func() {
			// A broken supervisor pipe must stop the child as well as normal EOF.
			_, _ = io.Copy(io.Discard, input)
			close(closed)
		}()
	}
	go func() {
		select {
		case <-signals:
		case <-eof:
		case <-finished:
			return
		}
		close(quit)
	}()
	err := server.StartWithQuitChannel(quit)
	// Start can return when Accept is interrupted, before another Stop caller
	// finishes draining. Stop is idempotent and waits for that cleanup.
	return errors.Join(err, server.Stop())
}
