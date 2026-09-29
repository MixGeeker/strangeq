package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestPasswordHashStdin(t *testing.T) {
	var out bytes.Buffer
	password := strings.Repeat("x", 64)
	require.NoError(t, hashPassword(strings.NewReader(password), &out))
	hash := bytes.TrimSpace(out.Bytes())
	require.NoError(t, bcrypt.CompareHashAndPassword(hash, []byte(password)))
	require.NotContains(t, out.String(), password)
	for _, bad := range []string{"", strings.Repeat("a", 73)} {
		out.Reset()
		require.Error(t, hashPassword(strings.NewReader(bad), &out))
		require.Empty(t, out.Bytes())
	}
}

type supervisedFixture struct {
	stopping chan struct{}
	release  chan struct{}
}

func (s *supervisedFixture) StartWithQuitChannel(quit <-chan struct{}) error {
	<-quit
	return nil
}
func (s *supervisedFixture) Stop() error {
	close(s.stopping)
	<-s.release
	return errors.New("CLEANUP_ERROR")
}

func TestSupervisorWaitsForCleanup(t *testing.T) {
	for _, trigger := range []string{"eof", "signal"} {
		t.Run(trigger, func(t *testing.T) {
			input, pipe := io.Pipe()
			defer input.Close()
			defer pipe.Close()
			signals := make(chan os.Signal, 1)
			server := &supervisedFixture{make(chan struct{}), make(chan struct{})}
			result := make(chan error, 1)
			go func() { result <- runServer(server, signals, input) }()
			if trigger == "eof" {
				require.NoError(t, pipe.Close())
			} else {
				signals <- os.Interrupt
			}
			select {
			case <-server.stopping:
			case <-time.After(time.Second):
				t.Fatal("shutdown was not requested")
			}
			select {
			case <-result:
				t.Fatal("returned before storage cleanup")
			default:
			}
			close(server.release)
			require.ErrorContains(t, <-result, "CLEANUP_ERROR")
		})
	}
}
