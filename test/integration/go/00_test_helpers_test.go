package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type broker struct {
	cmd     *exec.Cmd
	tempDir string
	amqpURL string
	mgmtURL string
}

func startBroker(t *testing.T) *broker {
	t.Helper()
	root := projectRoot(t)
	tempDir := t.TempDir()

	binary := filepath.Join(tempDir, "gobitmq")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	build := exec.Command("go", "build", "-o", binary, "./cmd")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	amqpPort := freePort(t)
	mgmtPort := freePort(t)

	cmd := exec.Command(binary)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOBITMQ_AMQP_ADDR=127.0.0.1:%d", amqpPort),
		fmt.Sprintf("GOBITMQ_MGMT_ADDR=127.0.0.1:%d", mgmtPort),
		fmt.Sprintf("GOBITMQ_DATA_DIR=%s", filepath.Join(tempDir, "data")),
		"GOBITMQ_MGMT_USERS=guest:guest:admin",
		"GOBITMQ_MGMT_ALLOW_REMOTE=true",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	b := &broker{
		cmd:     cmd,
		tempDir: tempDir,
		amqpURL: fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", amqpPort),
		mgmtURL: fmt.Sprintf("http://127.0.0.1:%d", mgmtPort),
	}

	waitForPort(t, amqpPort)
	waitForPort(t, mgmtPort)
	return b
}

func (b *broker) stop(t *testing.T) {
	t.Helper()
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_ = b.cmd.Wait()
	}
}

func (b *broker) restart(t *testing.T) {
	t.Helper()
	b.stop(t)

	binary := filepath.Join(b.tempDir, "gobitmq")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	cmd := exec.Command(binary)
	cmd.Dir = projectRoot(t)
	cmd.Env = b.cmd.Env
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("restart broker: %v", err)
	}
	b.cmd = cmd

	amqpPort := portFromURL(t, b.amqpURL)
	mgmtPort := portFromURL(t, b.mgmtURL)
	waitForPort(t, amqpPort)
	waitForPort(t, mgmtPort)
}

func projectRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine project root")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func portFromURL(t *testing.T, rawURL string) int {
	t.Helper()
	var port int
	for i := len(rawURL) - 1; i >= 0; i-- {
		if rawURL[i] == ':' {
			fmt.Sscanf(rawURL[i+1:], "%d", &port)
			break
		}
		if rawURL[i] == '/' {
			rawURL = rawURL[:i]
		}
	}
	if port == 0 {
		t.Fatalf("cannot parse port from %q", rawURL)
	}
	return port
}

func waitForPort(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for port %d", port)
}

func connect(t *testing.T, url string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial: %v", err)
	}
	return conn
}

func openChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	return ch
}

func unique(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UnixNano())
}

func assertBodyReceived(t *testing.T, ctx context.Context, msgs <-chan amqp.Delivery, want string, label string) {
	t.Helper()
	select {
	case msg := <-msgs:
		if string(msg.Body) != want {
			t.Fatalf("%s: unexpected body: got=%q want=%q", label, msg.Body, want)
		}
	case <-ctx.Done():
		t.Fatalf("%s: timeout waiting for message", label)
	}
}

func assertNoMessage(t *testing.T, msgs <-chan amqp.Delivery, message string) {
	t.Helper()
	select {
	case msg := <-msgs:
		t.Fatalf("%s: got unexpected body %q", message, msg.Body)
	case <-time.After(300 * time.Millisecond):
	}
}
