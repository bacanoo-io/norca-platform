// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/provider/node"
)

// fakeDocker keeps containers in memory and answers ps, inspect, run and rm
// as the docker command line does.
type fakeDocker struct {
	mu         sync.Mutex
	containers map[string]*container
	runs       [][]string
	failRun    error
	// failRunAfter makes every run after that many fail; 0 fails none.
	failRunAfter int
	logs         map[string]string
}

func (f *fakeDocker) Logs(_ context.Context, name string, _ int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs[name], nil
}

func (f *fakeDocker) Run(_ context.Context, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch args[0] {
	case "ps":
		var want []string
		for i, a := range args {
			if a == "--filter" {
				want = append(want, strings.TrimPrefix(args[i+1], "label="))
			}
		}
		var ids []string
		for name, c := range f.containers {
			ok := true
			for _, w := range want {
				k, v, _ := strings.Cut(w, "=")
				ok = ok && c.Config.Labels[k] == v
			}
			if ok {
				ids = append(ids, name)
			}
		}
		slices.Sort(ids)
		return strings.Join(ids, "\n"), nil
	case "inspect":
		var out []container
		for _, id := range args[1:] {
			if f.containers[id] == nil {
				// The text of the docker CLI of the lab (27.x): lower case.
				return "", errors.New("error: no such object: " + id)
			}
			out = append(out, *f.containers[id])
		}
		data, _ := json.Marshal(out)
		return string(data), nil
	case "run":
		if f.failRun != nil {
			return "", f.failRun
		}
		if f.failRunAfter > 0 && len(f.runs) >= f.failRunAfter {
			return "", errors.New("no space left on device")
		}
		if name := argAfter(args, "--name"); f.containers[name] != nil {
			return "", fmt.Errorf("Error response from daemon: Conflict. The container name %q is already in use", "/"+name)
		}
		f.runs = append(f.runs, args)
		c := &container{Name: "/" + argAfter(args, "--name")}
		c.State.Status = "running"
		// An address on the network of the run, as docker gives one.
		c.NetworkSettings.Networks = map[string]struct{ IPAddress, GlobalIPv6Address string }{
			argAfter(args, "--network"): {IPAddress: fmt.Sprintf("172.17.0.%d", len(f.runs)+1)}}
		c.Config.Labels = map[string]string{}
		for i, a := range args {
			if a == "--label" {
				k, v, _ := strings.Cut(args[i+1], "=")
				c.Config.Labels[k] = v
			}
		}
		f.containers[argAfter(args, "--name")] = c
		return "0123abcd\n", nil
	case "rm":
		if _, ok := f.containers[args[2]]; !ok {
			return "", errors.New("Error response from daemon: No such container: " + args[2])
		}
		delete(f.containers, args[2])
		return args[2] + "\n", nil
	}
	return "", fmt.Errorf("unexpected docker %v", args)
}

func argAfter(args []string, flag string) string {
	for i, a := range args {
		if a == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func newTestProvider(t *testing.T) (*Provider, *fakeDocker, string) {
	t.Helper()
	dir := t.TempDir()
	fd := &fakeDocker{containers: map[string]*container{}}
	p, err := New(Config{
		Groups:   map[string]GroupConfig{"dc1": {Capacity: node.Capacity{CPU: 2000, Memory: 2048, Disk: 10000}, Max: 3}},
		StateDir: dir, Docker: fd, HostAddresses: []string{"10.77.1.12"},
		CgroupRoot: filepath.Join(dir, ".cgroup"),
		Now:        func() time.Time { return time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, fd, dir
}

// request renders the bootstrap of the named instances, as the operator does.
func request(t *testing.T, id string, names ...string) node.CreateRequest {
	t.Helper()
	items, _ := bootstraptest.Items(t)
	tmpl, err := bootstrap.Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	req := node.CreateRequest{RequestID: id, Labels: map[string]string{"norca.node_group": "workers-dc1", "norca.region": "eu"}}
	for _, n := range names {
		r, err := tmpl.Render(bootstrap.Node{Name: n, Datacenter: "dc1", Pool: "workers"})
		if err != nil {
			t.Fatal(err)
		}
		req.Instances = append(req.Instances, node.InstanceRequest{Name: n, Bootstrap: &node.Bootstrap{UserData: r.UserData}})
	}
	return req
}

// A node that joins with a key of its own: norca-join is mounted and runs
// the agent; no key or certificate is written on the host.
func TestDocker_CreateRunsNorcaJoin(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	items, _ := bootstraptest.Items(t)
	tmpl, err := bootstrap.Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	r, err := tmpl.RenderJoin(bootstrap.Node{Name: "eu-workers-dc1-1-1", Datacenter: "dc1", Pool: "workers"},
		bootstrap.JoinTicket{Token: "t", URLs: []string{"https://10.0.0.7:8490"}})
	if err != nil {
		t.Fatal(err)
	}
	req := node.CreateRequest{RequestID: "eu/workers-dc1/1", Instances: []node.InstanceRequest{
		{Name: "eu-workers-dc1-1-1", Bootstrap: &node.Bootstrap{UserData: r.UserData}}}}
	if _, err := p.Create(context.Background(), "dc1", req); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(fd.runs[0], " ")
	for _, want := range []string{
		"/usr/local/bin/norca-join:/usr/local/bin/norca-join:ro", ":/etc/norca/norca-join.json:ro",
		"--entrypoint /usr/local/bin/norca-join debian:bookworm-slim run -config /etc/norca/norca-join.json -- " +
			"/usr/local/bin/norca agent -config /etc/norca/norca.hcl -config /etc/norca-provider/cgroup.hcl",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("docker run has no %q:\n%s", want, args)
		}
	}
	for _, f := range []string{"client-key.pem", "client.pem"} {
		if _, err := os.Stat(filepath.Join(dir, "eu-workers-dc1-1-1", "etc", "norca", "tls", f)); err == nil {
			t.Errorf("%s was written on the host", f)
		}
	}
}

// The install script of a machine is not mounted nor run: the provider
// brings the binaries itself.
func TestDocker_IgnoresTheInstallScript(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	items, _ := bootstraptest.Items(t)
	items[bootstrap.ItemInstallScript] = "#!/usr/bin/env bash\nexit 1\n"
	tmpl, err := bootstrap.Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	r, err := tmpl.Render(bootstrap.Node{Name: "eu-workers-dc1-1-1", Datacenter: "dc1", Pool: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	req := node.CreateRequest{RequestID: "eu/workers-dc1/1", Instances: []node.InstanceRequest{
		{Name: "eu-workers-dc1-1-1", Bootstrap: &node.Bootstrap{UserData: r.UserData}}}}
	if _, err := p.Create(context.Background(), "dc1", req); err != nil {
		t.Fatal(err)
	}
	if args := strings.Join(fd.runs[0], " "); strings.Contains(args, bootstrap.InstallScriptPath) ||
		!strings.Contains(args, "agent -config /etc/norca/norca.hcl") {
		t.Fatalf("docker run:\n%s", args)
	}
	if _, err := os.Stat(filepath.Join(dir, "eu-workers-dc1-1-1", "usr", "local", "sbin", "norca-node-install.sh")); err == nil {
		t.Fatal("the install script was written on the host")
	}
}

func TestDocker_CreateRunsTheAgentOfTheBootstrap(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	ctx := context.Background()
	insts, err := p.Create(ctx, "dc1", request(t, "eu/workers-dc1/1", "eu-workers-dc1-1-1", "eu-workers-dc1-1-2"))
	if err != nil {
		t.Fatal(err)
	}
	if len(insts) != 2 || insts[0].Name != "eu-workers-dc1-1-1" || insts[0].ID != "eu-workers-dc1-1-1" {
		t.Fatalf("instances: %+v", insts)
	}
	// The addresses are in the answer of the create: the container's on its
	// network, then the host's.
	if got := strings.Join(insts[1].Addresses, ","); got != "172.17.0.3,10.77.1.12" {
		t.Fatalf("the addresses of a new instance: %q", got)
	}
	args := strings.Join(fd.runs[0], " ")
	for _, want := range []string{
		"--name eu-workers-dc1-1-1", "--hostname eu-workers-dc1-1-1", "--label norca.request=eu/workers-dc1/1",
		"--label norca.node_group=workers-dc1", "/usr/local/bin/norca:/usr/local/bin/norca:ro",
		":/etc/norca/tls/client-key.pem:ro", ":/etc/norca/norca.hcl:ro",
		"--entrypoint /usr/local/bin/norca debian:bookworm-slim agent -config /etc/norca/norca.hcl -config /etc/norca-provider/cgroup.hcl",
		":/etc/norca-provider/cgroup.hcl:ro",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("docker run has no %q:\n%s", want, args)
		}
	}
	key := filepath.Join(dir, "eu-workers-dc1-1-1", "etc", "norca", "tls", "client-key.pem")
	info, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("the key has mode %o", info.Mode().Perm())
	}

	// The group lists them with the labels of the request.
	g, err := p.Group(ctx, "dc1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Instances) != 2 || g.Instances[0].State != node.StateRunning || g.Instances[0].RequestID != "eu/workers-dc1/1" ||
		g.Instances[0].Labels["norca.node_group"] != "workers-dc1" || !g.Instances[0].CreatedAt.Equal(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("group: %+v", g.Instances)
	}
	if got := strings.Join(g.Instances[0].Addresses, ","); got != "172.17.0.2,10.77.1.12" {
		t.Fatalf("the addresses of a listed instance: %q", got)
	}
	if g.Capacity.Memory != 2048 {
		t.Fatalf("capacity: %+v", g.Capacity)
	}

	// The same request again starts nothing.
	again, err := p.Create(ctx, "dc1", request(t, "eu/workers-dc1/1", "eu-workers-dc1-1-1", "eu-workers-dc1-1-2"))
	if err != nil || len(again) != 2 || len(fd.runs) != 2 {
		t.Fatalf("again: %+v %v, %d runs", again, err, len(fd.runs))
	}
}

// The agents of the containers see the cgroups of the host: each gets a
// parent of its own for the cgroups of its tasks, and the parent goes with
// the instance.
func TestDocker_CgroupParentOfAnInstance(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	ctx := context.Background()
	if _, err := p.Create(ctx, "dc1", request(t, "eu/workers-dc1/1", "eu-workers-dc1-1-1", "eu-workers-dc1-1-2")); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"eu-workers-dc1-1-1", "eu-workers-dc1-1-2"} {
		got, err := os.ReadFile(filepath.Join(dir, name, "etc", "norca-provider", "cgroup.hcl"))
		if err != nil {
			t.Fatal(err)
		}
		want := "client {\n  cgroup_parent = \"norca-node-" + name + ".slice\"\n}\n"
		if string(got) != want {
			t.Errorf("the cgroup settings of %s:\n%s\nwant:\n%s", name, got, want)
		}
		// The host namespace stays: in one of its own the agent could not
		// hand the controllers down to its tasks.
		if args := strings.Join(fd.runs[i], " "); !strings.Contains(args, "--cgroupns=host") {
			t.Errorf("docker run of %s has no --cgroupns=host:\n%s", name, args)
		}
	}

	// What the agent of the first instance left on the host, and the parent
	// of the second, which must stay.
	root := filepath.Join(dir, ".cgroup")
	own := filepath.Join(root, "norca-node-eu-workers-dc1-1-1.slice")
	other := filepath.Join(root, "norca-node-eu-workers-dc1-1-2.slice")
	for _, d := range []string{filepath.Join(own, "alloc-1.task.scope"), filepath.Join(own, "alloc-2.task.scope", "child"), other} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Delete(ctx, "dc1", "eu-workers-dc1-1-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(own); !os.IsNotExist(err) {
		t.Errorf("the cgroup parent of the deleted instance is left: %v", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Errorf("the cgroup parent of the other instance: %v", err)
	}
	// An instance whose agent made no cgroup.
	if err := p.Delete(ctx, "dc1", "eu-workers-dc1-1-2"); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(ctx, "dc1", "eu-workers-dc1-1-2"); err != nil {
		t.Fatalf("a second delete: %v", err)
	}
}

func TestDocker_LimitsAndFailures(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	ctx := context.Background()
	if _, err := p.Create(ctx, "dc1", request(t, "r1", "a-1", "a-2", "a-3", "a-4")); node.ReasonOf(err) != node.ReasonQuota {
		t.Fatalf("want quota, got %v", err)
	}
	req := request(t, "r2", "a-1")
	req.Instances[0].Bootstrap = nil
	if _, err := p.Create(ctx, "dc1", req); node.ReasonOf(err) != node.ReasonInvalid {
		t.Fatalf("want invalid without a bootstrap, got %v", err)
	}
	if _, err := p.Create(ctx, "dc1", request(t, "r3", "Bad_Name")); node.ReasonOf(err) != node.ReasonInvalid {
		t.Fatalf("want invalid for a bad name, got %v", err)
	}
	if _, err := p.Group(ctx, "dc9", nil); !errors.Is(err, node.ErrUnknownGroup) {
		t.Fatalf("unknown group: %v", err)
	}

	// docker run fails: an error, and no files are left.
	fd.failRun = errors.New("no such image")
	if _, err := p.Create(ctx, "dc1", request(t, "r4", "b-1")); node.ReasonOf(err) != node.ReasonError {
		t.Fatalf("want an error, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "b-1")); !os.IsNotExist(err) {
		t.Fatalf("the files of a failed start are left: %v", err)
	}
	fd.failRun = nil

	// A container that stopped is a failed instance.
	if _, err := p.Create(ctx, "dc1", request(t, "r5", "c-1")); err != nil {
		t.Fatal(err)
	}
	fd.containers["c-1"].State.Status = "exited"
	fd.containers["c-1"].State.ExitCode = 1
	// A stopped container has no address; neither then has the host.
	fd.containers["c-1"].NetworkSettings.Networks = nil
	fd.logs = map[string]string{"c-1": "==> Loaded configuration\nError starting agent: client setup failed\n"}
	g, _ := p.Group(ctx, "dc1", nil)
	if g.Instances[0].State != node.StateFailed || len(g.Instances[0].Addresses) != 0 || !strings.Contains(g.Instances[0].Message, "exit code 1") ||
		!strings.Contains(g.Instances[0].Message, "last output: ==> Loaded configuration | Error starting agent: client setup failed") {
		t.Fatalf("instance: %+v", g.Instances[0])
	}

	// Delete removes the container and its files; a second delete is fine.
	if err := p.Delete(ctx, "dc1", "c-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c-1")); !os.IsNotExist(err) {
		t.Fatalf("the files are left: %v", err)
	}
	if err := p.Delete(ctx, "dc1", "c-1"); err != nil {
		t.Fatalf("a second delete: %v", err)
	}
	if err := p.Delete(ctx, "dc1", "../etc"); node.ReasonOf(err) != node.ReasonInvalid {
		t.Fatalf("a bad id: %v", err)
	}
}

// A create that fails part of the way returns an error and no instance (the
// operator sees only the error over HTTP); the containers it made before the
// failure stay, carry the request, and are in the listing, and the request
// again is answered with them and makes nothing (review finding CON-6o: the
// provider returned the containers made so far together with the error).
func TestDocker_CreateThatFailsPartOfTheWay(t *testing.T) {
	p, fd, _ := newTestProvider(t)
	ctx := context.Background()
	fd.failRunAfter = 1
	req := request(t, "eu/workers-dc1/1", "eu-workers-dc1-1-1", "eu-workers-dc1-1-2")
	insts, err := p.Create(ctx, "dc1", req)
	if node.ReasonOf(err) != node.ReasonError || err == nil || !strings.Contains(err.Error(), "eu-workers-dc1-1-2") {
		t.Fatalf("a create whose second container does not start: %v", err)
	}
	if len(insts) != 0 {
		t.Fatalf("a create that failed returned instances with its error: %+v", insts)
	}
	g, err := p.Group(ctx, "dc1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Instances) != 1 || g.Instances[0].Name != "eu-workers-dc1-1-1" || g.Instances[0].RequestID != req.RequestID {
		t.Fatalf("the listing after a create that failed part of the way: %+v", g.Instances)
	}

	// The request again: the container it has, nothing new.
	fd.failRunAfter = 0
	again, err := p.Create(ctx, "dc1", req)
	if err != nil || len(again) != 1 || again[0].Name != "eu-workers-dc1-1-1" || len(fd.runs) != 1 {
		t.Fatalf("the request again: %+v, %v, %d runs", again, err, len(fd.runs))
	}
	// What is missing comes with a request of its own.
	rest, err := p.Create(ctx, "dc1", request(t, "eu/workers-dc1/2", "eu-workers-dc1-2-1"))
	if err != nil || len(rest) != 1 || rest[0].RequestID != "eu/workers-dc1/2" {
		t.Fatalf("a new request for the rest: %+v, %v", rest, err)
	}
}

// A request that names a container that exists (another request gave its
// instance the same name, as two regions on one docker did) fails and leaves
// the files of that container alone: they are mounted into it (review
// finding CON-6n: start removed the state directory of the name before the
// run, and again when the run failed).
func TestDocker_StartLeavesTheFilesOfAnExistingContainer(t *testing.T) {
	p, fd, dir := newTestProvider(t)
	ctx := context.Background()
	if _, err := p.Create(ctx, "dc1", request(t, "eu/workers/1", "workers-1-1")); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(dir, "workers-1-1", "etc", "norca", "tls", "client-key.pem")
	before, err := os.ReadFile(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.Create(ctx, "dc1", request(t, "us/workers/1", "workers-1-1"))
	if node.ReasonOf(err) != node.ReasonError || err == nil || !strings.Contains(err.Error(), "workers-1-1") {
		t.Fatalf("a request for the name of a container that exists: %v", err)
	}
	after, err := os.ReadFile(key)
	if err != nil {
		t.Fatalf("the files of the container that exists are gone: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("the key of the container that exists was written again")
	}
	if c := fd.containers["workers-1-1"]; c == nil || c.Config.Labels["norca.request"] != "eu/workers/1" || len(fd.runs) != 1 {
		t.Fatalf("the container that exists was touched: %+v, %d runs", c, len(fd.runs))
	}

	// A directory without a container (left by a crash) is not in the way.
	stale := filepath.Join(dir, "workers-1-2", "etc", "stale")
	if err := os.MkdirAll(filepath.Dir(stale), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Create(ctx, "dc1", request(t, "eu/workers/2", "workers-1-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("the stale files of a name without a container are left: %v", err)
	}
}

// The docker of the lab answers an inspect of a missing container with
// "error: no such object", an older one with "Error: No such object": a
// create worked with neither of them being an error.
func TestNoSuch(t *testing.T) {
	for text, want := range map[string]bool{
		"exit status 1: error: no such object: workers-1-1":                  true,
		"exit status 1: Error: No such object: workers-1-1":                  true,
		"Error response from daemon: No such container: workers-1-1":         true,
		"Cannot connect to the Docker daemon at unix:///var/run/docker.sock": false,
		"dial tcp: lookup docker.lab: no such host":                          false,
	} {
		if got := noSuch(errors.New(text)); got != want {
			t.Errorf("noSuch(%q) = %v, want %v", text, got, want)
		}
	}
	if noSuch(nil) {
		t.Error("no error is not a missing container")
	}
}
