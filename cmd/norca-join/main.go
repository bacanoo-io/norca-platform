// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-join gives a node of the node scaling operator its identity
// without a key in the user data: it makes the key on the machine, joins
// with the one-time token of its bootstrap, and renews the certificate
// before it expires (package join).
//
//	norca-join run -config /etc/norca/norca-join.json
//	    joins, starts the agent (systemctl enable --now <service>), renews,
//	    and sends the agent SIGHUP after each renewal (the systemd unit of
//	    the bootstrap runs this)
//	norca-join run -config /etc/norca/norca-join.json -- norca agent -config /etc/norca/norca.hcl
//	    the same, with the agent as its child (a container): norca-join
//	    ends when the agent does, and passes SIGTERM and SIGINT on
//	norca-join join -config ...    one join
//	norca-join renew -config ...   one renewal (no reload)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	code, err := run(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "norca-join:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

const usage = `usage: norca-join run|join|renew -config <file> [-- <agent command>...]
       norca-join -version`

func run(args []string) (int, error) {
	if len(args) == 1 && (args[0] == "-version" || args[0] == "--version") {
		fmt.Println("norca-join", version)
		return 0, nil
	}
	if len(args) == 0 {
		return 2, errors.New(usage)
	}
	mode := args[0]
	fs := flag.NewFlagSet("norca-join "+mode, flag.ContinueOnError)
	configPath := fs.String("config", "/etc/norca/"+join.ConfigFileName, "the configuration of norca-join")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	if err := fs.Parse(args[1:]); err != nil {
		return 2, err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return 2, fmt.Errorf("-log-level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	conf, err := join.LoadConfig(*configPath)
	if err != nil {
		return 1, err
	}
	client := &join.Client{Config: conf, Log: logger}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch mode {
	case "join":
		return 0, client.Join(ctx, *configPath)
	case "renew":
		return 0, client.Renew(ctx)
	case "run":
	default:
		return 2, fmt.Errorf("unknown mode %q\n%s", mode, usage)
	}

	r := &join.Runner{Client: client, ConfigPath: *configPath}
	command := fs.Args()
	if len(command) == 0 {
		if conf.Service == "" {
			return 2, errors.New("no service in the configuration and no agent command after --")
		}
		r.Start = func(ctx context.Context) error {
			return systemctl(ctx, "enable", "--now", conf.Service)
		}
		r.Reload = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			return systemctl(ctx, "kill", "--kill-whom=main", "--signal=HUP", conf.Service)
		}
		return 0, r.Run(ctx)
	}
	return supervise(stop, r, command, logger)
}

func systemctl(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput() //nolint:gosec // G204: systemctl for the unit of the agent named in the configuration
	if err != nil {
		return fmt.Errorf("systemctl %v: %w: %s", args, err, out)
	}
	return nil
}

// supervise runs the agent as the child of norca-join: started once the node
// has its certificate, sent SIGHUP after a renewal, sent SIGTERM and SIGINT
// that norca-join receives. norca-join ends with the agent, with its exit
// code.
func supervise(stop context.CancelFunc, r *join.Runner, command []string, logger *slog.Logger) (int, error) {
	stop() // the signals go to the agent, not to the context
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start and Reload run in the goroutine of the Runner; the process
	// reaches this one through started.
	var agent *os.Process
	started := make(chan *os.Process, 1)
	exited := make(chan error, 1)
	r.Start = func(ctx context.Context) error {
		// The agent is stopped with a signal it handles, never killed with
		// the context.
		cmd := exec.CommandContext(context.WithoutCancel(ctx), command[0], command[1:]...) //nolint:gosec // G204: running the agent command given after -- is the purpose of run
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("starting the agent: %w", err)
		}
		logger.Info("the agent started", "pid", cmd.Process.Pid)
		agent = cmd.Process
		started <- cmd.Process
		go func() { exited <- cmd.Wait() }()
		return nil
	}
	r.Reload = func() error { return agent.Signal(syscall.SIGHUP) }

	var proc *os.Process
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	for {
		select {
		case p := <-started:
			proc = p
		case sig := <-sigs:
			if proc == nil {
				return 0, nil // before the agent: nothing to stop
			}
			_ = proc.Signal(sig)
		case err := <-done:
			if err != nil {
				select {
				case p := <-started:
					proc = p
				default:
				}
				if proc != nil {
					_ = proc.Signal(syscall.SIGTERM)
					if werr := <-exited; werr != nil {
						logger.Debug("the agent ended", "error", werr)
					}
				}
				return 1, err
			}
		case err := <-exited:
			cancel()
			if exit, ok := errors.AsType[*exec.ExitError](err); ok {
				return exit.ExitCode(), nil
			}
			if err != nil {
				return 1, err
			}
			return 0, nil
		}
	}
}
