// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The init system is found as the bootstrap finds it: systemd when its
// directory exists, then OpenRC when rc-service is on the PATH.
func TestDetectInit(t *testing.T) {
	dir := t.TempDir()
	systemdDir := filepath.Join(dir, "systemd")
	found := func(string) (string, error) { return "/sbin/rc-service", nil }
	missing := func(string) (string, error) { return "", exec.ErrNotFound }

	if got := detectInit(systemdDir, missing); got != initUnknown {
		t.Fatalf("neither: %v", got)
	}
	if got := detectInit(systemdDir, found); got != initOpenRC {
		t.Fatalf("rc-service: %v", got)
	}
	// A file of that name is not the directory of a running systemd.
	if err := os.WriteFile(systemdDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := detectInit(systemdDir, found); got != initOpenRC {
		t.Fatalf("a file: %v", got)
	}
	if err := os.Remove(systemdDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(systemdDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, lookPath := range map[string]func(string) (string, error){"with rc-service": found, "without": missing} {
		if got := detectInit(systemdDir, lookPath); got != initSystemd {
			t.Fatalf("systemd %s: %v", name, got)
		}
	}
}

func TestInitSystemCommands(t *testing.T) {
	tests := []struct {
		init   initSystem
		start  [][]string
		reload []string
	}{
		{initSystemd, [][]string{{"systemctl", "enable", "--now", "norca"}},
			[]string{"systemctl", "kill", "--kill-whom=main", "--signal=HUP", "norca"}},
		{initOpenRC, [][]string{{"rc-update", "add", "norca", "default"}, {"rc-service", "norca", "start"}},
			[]string{"rc-service", "norca", "reload"}},
		{initUnknown, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.init.String(), func(t *testing.T) {
			if got := tt.init.startCommands("norca"); !slices.EqualFunc(got, tt.start, slices.Equal[[]string]) {
				t.Errorf("start: %q, want %q", got, tt.start)
			}
			if got := tt.init.reloadCommand("norca"); !slices.Equal(got, tt.reload) {
				t.Errorf("reload: %q, want %q", got, tt.reload)
			}
		})
	}
}

// runCommands runs the commands in order through the PATH and stops at the
// first that fails, with its output in the error.
func TestRunCommands(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stubs are sh scripts")
	}
	bin := t.TempDir()
	calls := filepath.Join(t.TempDir(), "calls")
	for name, code := range map[string]string{"rc-update": "0", "rc-service": "0", "systemctl": "1"} {
		script := "#!/bin/sh\necho \"" + name + " $*\" >>\"$CALLS\"\necho \"" + name + " says no\"\nexit " + code + "\n"
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin)
	t.Setenv("CALLS", calls)

	if err := runCommands(context.Background(), initOpenRC.startCommands("norca")...); err != nil {
		t.Fatal(err)
	}
	if err := runCommands(context.Background(), initOpenRC.reloadCommand("norca")); err != nil {
		t.Fatal(err)
	}
	err := runCommands(context.Background(), initSystemd.startCommands("norca")...)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(err.Error(), "systemctl says no") {
		t.Fatalf("a failed command: %v", err)
	}
	got, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	want := "rc-update add norca default\nrc-service norca start\nrc-service norca reload\nsystemctl enable --now norca\n"
	if string(got) != want {
		t.Fatalf("calls:\n%s\nwant:\n%s", got, want)
	}
}
