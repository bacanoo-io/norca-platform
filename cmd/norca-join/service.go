// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// initSystem is the init system that runs the service of the agent, which
// norca-join starts after the join and tells to reload the certificate
// after a renewal.
type initSystem int

const (
	initUnknown initSystem = iota
	initSystemd
	initOpenRC
)

// systemdRunDir exists while systemd runs as the init system (the test of
// sd_booted).
const systemdRunDir = "/run/systemd/system"

// detectInit finds the init system as the bootstrap does at boot: systemd
// when systemdDir exists, otherwise OpenRC when rc-service is on the PATH
// (lookPath), otherwise initUnknown.
func detectInit(systemdDir string, lookPath func(string) (string, error)) initSystem {
	if fi, err := os.Stat(systemdDir); err == nil && fi.IsDir() {
		return initSystemd
	}
	if _, err := lookPath("rc-service"); err == nil {
		return initOpenRC
	}
	return initUnknown
}

func (s initSystem) String() string {
	switch s {
	case initSystemd:
		return "systemd"
	case initOpenRC:
		return "OpenRC"
	default:
		return "unknown"
	}
}

// startCommands are the commands that enable the service and start it: it
// starts at the next boot too. Each is a no-op for a service that is
// enabled and running already, as after a restart of norca-join.
func (s initSystem) startCommands(service string) [][]string {
	switch s {
	case initSystemd:
		return [][]string{{"systemctl", "enable", "--now", service}}
	case initOpenRC:
		return [][]string{{"rc-update", "add", service, "default"}, {"rc-service", service, "start"}}
	default:
		return nil
	}
}

// reloadCommand is the command that sends the agent SIGHUP: with OpenRC
// the reload of its service, which the OpenRC service of the deployer has
// (supervise-daemon --signal HUP).
func (s initSystem) reloadCommand(service string) []string {
	switch s {
	case initSystemd:
		return []string{"systemctl", "kill", "--kill-whom=main", "--signal=HUP", service}
	case initOpenRC:
		return []string{"rc-service", service, "reload"}
	default:
		return nil
	}
}

// runCommands runs commands in order and stops at the first that fails.
func runCommands(ctx context.Context, commands ...[]string) error {
	for _, c := range commands {
		out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput() //nolint:gosec // G204: systemctl, rc-update or rc-service for the service of the agent named in the configuration
		if err != nil {
			return fmt.Errorf("%v: %w: %s", c, err, out)
		}
	}
	return nil
}
