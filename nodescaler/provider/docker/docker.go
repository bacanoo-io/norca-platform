// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package docker is a node provider whose machines are containers on the
// host it runs on, for the lab and for trials: each instance is a container
// that runs the Norca agent with the files of its bootstrap. The bootstrap
// is read back from the cloud-init document (package bootstrap): the files
// are written under the state directory and mounted read-only at their
// paths, the agent binary of the host is mounted too, and the container
// runs "norca agent -config <the .hcl file>". The container has the name and
// the host name of the instance.
//
// The containers carry the labels of the request, so the provider knows its
// instances after a restart and answers a repeated request with them.
//
// The addresses of an instance are those of its container on its docker
// networks and the addresses of the host (Config.HostAddresses): traffic
// from a container to its own host keeps the container's address, traffic
// to other machines is masqueraded to the host's. For the address binding
// of the join endpoint the containers of one host are therefore one
// machine toward the rest of the cluster.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

// The labels of the containers of the provider.
const (
	LabelManaged = "norca.nodescaler.docker"
	LabelGroup   = "norca.provider_group"
	LabelName    = "norca.instance_name"
	LabelCreated = "norca.created"
)

// Runner runs the docker command line.
type Runner interface {
	Run(ctx context.Context, args ...string) (string, error)

	// Logs returns the last lines a container wrote, its output and its
	// errors together.
	Logs(ctx context.Context, name string, lines int) (string, error)
}

// CLI runs the docker binary.
type CLI struct{ Bin string }

// Run implements Runner.
func (c CLI) Run(ctx context.Context, args ...string) (string, error) {
	bin := c.Bin
	if bin == "" {
		bin = "docker"
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, args...) //nolint:gosec // G204: running docker for the containers is the purpose of the provider
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// Logs implements Runner.
func (c CLI) Logs(ctx context.Context, name string, lines int) (string, error) {
	bin := c.Bin
	if bin == "" {
		bin = "docker"
	}
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, "logs", "--tail", fmt.Sprint(lines), name) //nolint:gosec // G204: as Run
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// failureLogLines and failureLogBytes bound what the message of a failed
// instance carries of the output of its container.
const (
	failureLogLines = 8
	failureLogBytes = 800
)

// GroupConfig is a group of the provider.
type GroupConfig struct {
	// Capacity is reported as the size of a machine.
	Capacity provider.Capacity
	// Max is the most containers of the group; 0 is no limit.
	Max int
}

// Config are the settings of the provider.
type Config struct {
	Groups map[string]GroupConfig

	// Image is the image of the containers: it needs what the agent needs
	// (glibc); the agent binary comes from Binary (default
	// debian:bookworm-slim).
	Image string

	// Binary is the path of the norca binary on the host, mounted at
	// /usr/local/bin/norca (default /usr/local/bin/norca).
	Binary string

	// JoinBinary is the path of norca-join on the host, mounted at
	// /usr/local/bin/norca-join into the container of a node that joins with
	// a key of its own; norca-join then runs the agent (default
	// /usr/local/bin/norca-join).
	JoinBinary string

	// StateDir is where the files of the nodes are written, one directory
	// per instance.
	StateDir string

	// Network is the docker network of the containers (default bridge).
	Network string

	// HostAddresses are the addresses the traffic of the containers to
	// other machines comes from (the host's, after the masquerade of the
	// docker bridge); they are reported with the container's own addresses.
	HostAddresses []string

	Docker Runner
	Now    func() time.Time
}

// Provider is the docker node provider.
type Provider struct {
	cfg Config
	mu  sync.Mutex
}

// New returns the provider.
func New(cfg Config) (*Provider, error) {
	if cfg.StateDir == "" {
		return nil, errors.New("the state directory is required")
	}
	if cfg.Image == "" {
		cfg.Image = "debian:bookworm-slim"
	}
	if cfg.Binary == "" {
		cfg.Binary = "/usr/local/bin/norca"
	}
	if cfg.JoinBinary == "" {
		cfg.JoinBinary = "/usr/local/bin/norca-join"
	}
	if cfg.Network == "" {
		cfg.Network = "bridge"
	}
	if cfg.Docker == nil {
		cfg.Docker = CLI{}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Groups = maps.Clone(cfg.Groups)
	return &Provider{cfg: cfg}, nil
}

// container is what docker inspect says about a container.
type container struct {
	Name    string
	Created string
	State   struct {
		Status   string
		ExitCode int
		Error    string
	}
	Config struct {
		Labels map[string]string
	}
	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress         string
			GlobalIPv6Address string
		}
	}
}

// instances lists the containers of a group.
func (p *Provider) instances(ctx context.Context, group string) ([]provider.Instance, error) {
	ids, err := p.cfg.Docker.Run(ctx, "ps", "-a", "-q", "--no-trunc",
		"--filter", "label="+LabelManaged+"=1", "--filter", "label="+LabelGroup+"="+group)
	if err != nil {
		return nil, err
	}
	list := strings.Fields(ids)
	if len(list) == 0 {
		return []provider.Instance{}, nil
	}
	out, err := p.cfg.Docker.Run(ctx, append([]string{"inspect"}, list...)...)
	if err != nil {
		return nil, err
	}
	var cs []container
	if err := json.Unmarshal([]byte(out), &cs); err != nil {
		return nil, fmt.Errorf("reading docker inspect: %w", err)
	}
	insts := make([]provider.Instance, 0, len(cs))
	for _, c := range cs {
		inst := p.instanceOf(c)
		if inst.State == provider.StateFailed {
			// The operator deletes a failed instance, and the output of the
			// container with it: the reason goes into the message.
			if logs, err := p.cfg.Docker.Logs(ctx, inst.ID, failureLogLines); err == nil && strings.TrimSpace(logs) != "" {
				inst.Message += "; last output: " + lastOutput(logs)
			}
		}
		insts = append(insts, inst)
	}
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.ID, b.ID) })
	return insts, nil
}

// lastOutput joins the lines of the output of a container into one line,
// bounded, the last part kept.
func lastOutput(logs string) string {
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	out := strings.Join(lines, " | ")
	if len(out) > failureLogBytes {
		out = "..." + out[len(out)-failureLogBytes:]
	}
	return out
}

// addresses are the addresses of a container on its networks, then those of
// the host; none while the container has no address of its own (not
// started): a partial list would bind the join to the host alone.
func (p *Provider) addresses(c container) []string {
	var out []string
	for _, name := range slices.Sorted(maps.Keys(c.NetworkSettings.Networks)) {
		n := c.NetworkSettings.Networks[name]
		for _, a := range []string{n.IPAddress, n.GlobalIPv6Address} {
			if a != "" && !slices.Contains(out, a) {
				out = append(out, a)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	for _, a := range p.cfg.HostAddresses {
		if !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	return out
}

// instanceOf maps a container to an instance.
func (p *Provider) instanceOf(c container) provider.Instance {
	labels := map[string]string{}
	for k, v := range c.Config.Labels {
		if strings.HasPrefix(k, "norca.") && k != LabelManaged && k != LabelGroup && k != LabelName && k != LabelCreated {
			labels[k] = v
		}
	}
	inst := provider.Instance{
		ID:        strings.TrimPrefix(c.Name, "/"),
		Name:      c.Config.Labels[LabelName],
		RequestID: c.Config.Labels["norca.request"],
		Labels:    labels,
		Addresses: p.addresses(c),
	}
	inst.CreatedAt, _ = time.Parse(time.RFC3339, c.Config.Labels[LabelCreated])
	switch c.State.Status {
	case "running":
		inst.State = provider.StateRunning
	case "created", "restarting":
		inst.State = provider.StateCreating
	case "removing":
		inst.State = provider.StateDeleting
	default: // exited, dead, paused
		inst.State = provider.StateFailed
		inst.Reason = provider.ReasonError
		inst.Message = fmt.Sprintf("the container is %s (exit code %d) %s", c.State.Status, c.State.ExitCode, c.State.Error)
	}
	return inst
}

// Group implements provider.Provider.
func (p *Provider) Group(ctx context.Context, group string, _ map[string]any) (*provider.Group, error) {
	cfg, ok := p.cfg.Groups[group]
	if !ok {
		return nil, provider.ErrUnknownGroup
	}
	insts, err := p.instances(ctx, group)
	if err != nil {
		return nil, err
	}
	capacity := cfg.Capacity
	return &provider.Group{Name: group, Capacity: &capacity, Instances: insts}, nil
}

var validName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// Create implements provider.Provider.
func (p *Provider) Create(ctx context.Context, group string, req provider.CreateRequest) ([]provider.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, ok := p.cfg.Groups[group]
	if !ok {
		return nil, provider.ErrUnknownGroup
	}
	existing, err := p.instances(ctx, group)
	if err != nil {
		return nil, err
	}
	var same []provider.Instance
	for _, inst := range existing {
		if inst.RequestID == req.RequestID {
			same = append(same, inst)
		}
	}
	if len(same) > 0 {
		return same, nil
	}
	if cfg.Max > 0 && len(existing)+len(req.Instances) > cfg.Max {
		return nil, provider.Fail(provider.ReasonQuota, "group %s has %d of %d containers, %d more asked for",
			group, len(existing), cfg.Max, len(req.Instances))
	}
	for _, ir := range req.Instances {
		if !validName.MatchString(ir.Name) {
			return nil, provider.Fail(provider.ReasonInvalid, "%q is not a container name", ir.Name)
		}
		if ir.Bootstrap == nil {
			return nil, provider.Fail(provider.ReasonInvalid, "instance %s has no bootstrap: a container runs the agent of its bootstrap", ir.Name)
		}
	}

	out := make([]provider.Instance, 0, len(req.Instances))
	for _, ir := range req.Instances {
		inst, err := p.start(ctx, group, req, ir)
		if err != nil {
			return out, provider.Fail(provider.ReasonError, "starting %s: %v", ir.Name, err)
		}
		out = append(out, inst)
	}
	return out, nil
}

// start writes the files of an instance and starts its container.
func (p *Provider) start(ctx context.Context, group string, req provider.CreateRequest, ir provider.InstanceRequest) (provider.Instance, error) {
	doc, err := bootstrap.Decode(ir.Bootstrap.UserData)
	if err != nil {
		return provider.Instance{}, err
	}
	config, err := doc.ConfigFile()
	if err != nil {
		return provider.Instance{}, err
	}
	dir := filepath.Join(p.cfg.StateDir, ir.Name)
	if err := os.RemoveAll(dir); err != nil {
		return provider.Instance{}, err
	}
	args := []string{"run", "-d", "--name", ir.Name, "--hostname", ir.Name,
		"--privileged", "--cgroupns=host", "--network", p.cfg.Network,
		"--label", LabelManaged + "=1", "--label", LabelGroup + "=" + group, "--label", LabelName + "=" + ir.Name,
		"--label", LabelCreated + "=" + p.cfg.Now().UTC().Format(time.RFC3339),
		"--label", "norca.request=" + req.RequestID,
		"-v", p.cfg.Binary + ":/usr/local/bin/norca:ro"}
	for _, k := range slices.Sorted(maps.Keys(req.Labels)) {
		if k != "norca.request" {
			args = append(args, "--label", k+"="+req.Labels[k])
		}
	}
	for _, f := range doc.Files {
		if f.Path == bootstrap.InstallScriptPath {
			// The install script of a machine (install_script of the
			// bootstrap) is not run: a container does not run runcmd, and
			// the provider brings the binaries itself (the mounts of norca
			// and norca-join) with an image that needs no unit. The lab uses
			// images that have everything, so there is no script.
			continue
		}
		host := filepath.Join(dir, filepath.FromSlash(strings.TrimPrefix(f.Path, "/")))
		if err := os.MkdirAll(filepath.Dir(host), 0o700); err != nil {
			return provider.Instance{}, err
		}
		if err := os.WriteFile(host, f.Content, os.FileMode(f.Mode)); err != nil {
			return provider.Instance{}, err
		}
		args = append(args, "-v", host+":"+f.Path+":ro")
	}
	if joinConfig := doc.JoinConfig(); joinConfig != "" {
		// The node makes its key and gets its certificate itself:
		// norca-join runs the agent once it has one, and renews it.
		args = append(args, "-v", p.cfg.JoinBinary+":/usr/local/bin/norca-join:ro",
			"--entrypoint", "/usr/local/bin/norca-join", p.cfg.Image,
			"run", "-config", joinConfig, "--", "/usr/local/bin/norca", "agent", "-config", config)
	} else {
		args = append(args, "--entrypoint", "/usr/local/bin/norca", p.cfg.Image, "agent", "-config", config)
	}
	if _, err := p.cfg.Docker.Run(ctx, args...); err != nil {
		_ = os.RemoveAll(dir)
		return provider.Instance{}, err
	}
	inst := provider.Instance{
		ID: ir.Name, Name: ir.Name, State: provider.StateCreating, RequestID: req.RequestID,
		Labels: maps.Clone(req.Labels), CreatedAt: p.cfg.Now().UTC().Truncate(time.Second),
	}
	// The addresses at once: the node joins as soon as it starts, and the
	// operator records them from this answer. Without them (inspect failed)
	// the operator records them from a later list.
	if out, err := p.cfg.Docker.Run(ctx, "inspect", ir.Name); err == nil {
		var cs []container
		if json.Unmarshal([]byte(out), &cs) == nil && len(cs) == 1 {
			inst.Addresses = p.addresses(cs[0])
		}
	}
	return inst, nil
}

// Delete implements provider.Provider: the container and its files go.
func (p *Provider) Delete(ctx context.Context, group, id string) error {
	if _, ok := p.cfg.Groups[group]; !ok {
		return provider.ErrUnknownGroup
	}
	if !validName.MatchString(id) {
		return provider.Fail(provider.ReasonInvalid, "%q is not a container of the provider", id)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.cfg.Docker.Run(ctx, "rm", "-f", id); err != nil && !strings.Contains(err.Error(), "No such container") {
		return err
	}
	return os.RemoveAll(filepath.Join(p.cfg.StateDir, id))
}
