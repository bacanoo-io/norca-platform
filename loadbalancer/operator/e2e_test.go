// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
	"github.com/bacanoo-io/norca/norca/controller/envtest"
	"github.com/shoenig/test/must"
	"github.com/shoenig/test/wait"
)

// TestE2E_OperatorPack installs the LoadBalancer operator pack of the Norca
// pack registry (packs/loadbalancer-operator of a norca-registry checkout:
// NORCA_REGISTRY_DIR, or ../norca-registry next to this repository) on a dev
// agent with the norca CLI, runs the controller
// against it (in process; the pack's controller job runs with the mock
// driver), and reconciles a load balancer for a service of a mock driver
// job: the proxy job is created and owned by the object, the status gets
// ready, and deleting the object removes the proxy job through the
// finalizer. Uninstalling the release removes the kind, the policy and the
// job. It needs the norca binary (NORCA_BINARY or norca on the PATH) and the
// registry checkout, and is skipped without them.
func TestE2E_OperatorPack(t *testing.T) {
	packDir := registryPack(t)
	agent := envtest.StartT(t, envtest.Options{ACL: true})
	client := agent.Client

	// Rendering may write a lock file: use a copy.
	pack := filepath.Join(t.TempDir(), "loadbalancer-operator")
	must.NoError(t, os.CopyFS(pack, os.DirFS(packDir)))

	home := t.TempDir()
	norca := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, agent.Binary(), args...)
		cmd.Env = append(os.Environ(), agent.Env()...)
		cmd.Env = append(cmd.Env, "HOME="+home, "USERPROFILE="+home, "XDG_CACHE_HOME="+home, "GIT_TERMINAL_PROMPT=0")
		out, err := cmd.CombinedOutput()
		must.NoError(t, err, must.Sprintf("norca %s:\n%s", strings.Join(args, " "), out))
		return string(out)
	}

	out := norca("pack", "run", pack, "--name=lb", "--detach",
		"--var=driver=mock_driver", "--var=proxy_driver=mock_driver")
	must.StrContains(t, out, `Release "lb" deployed at revision 1`)

	kind, _, err := client.ObjectKinds().Info(Kind.Group, Kind.Kind, nil)
	must.NoError(t, err)
	must.NotNil(t, kind.Limits)
	_, _, err = client.Jobs().Info("loadbalancer-controller", nil)
	must.NoError(t, err)

	// A service with an instance: a mock driver job with a static port.
	web := api.NewServiceJob("web", "web", "global", 50)
	web.Datacenters = []string{"*"}
	group := api.NewTaskGroup("web", 1)
	group.Networks = []*api.NetworkResource{{ReservedPorts: []api.Port{{Label: "http", Value: 28080}}}}
	group.Services = []*api.Service{{Name: "web", PortLabel: "http", Provider: "nomad"}}
	task := api.NewTask("web", "mock_driver")
	task.Config = map[string]any{"run_for": "1h"}
	group.AddTask(task)
	web.AddTaskGroup(group)
	_, _, err = client.Jobs().Register(web, nil)
	must.NoError(t, err)

	// The controller, as the pack's job would run it.
	backend := controller.NewBackend(client)
	cluster := &nomadCluster{client: client}
	prov, err := newNomadProvider(NomadProviderConfig{Driver: "mock_driver"}, backend, cluster)
	must.NoError(t, err)
	r := &reconciler{
		cluster:         cluster,
		providers:       map[string]Provider{prov.Class(): prov},
		logger:          testLogger(),
		now:             time.Now,
		requeueNotReady: 500 * time.Millisecond,
		requeueReady:    time.Minute,
	}
	ctrl, err := controller.New(backend, r, controller.Options{
		Name:     "loadbalancer",
		Kind:     Kind,
		Storage:  controller.StorageNative,
		OwnsJobs: true,
		Sources:  []controller.Source{serviceSource()},
		Logger:   testLogger(),
	})
	must.NoError(t, err)
	r.ctrl = ctrl
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = ctrl.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	_, _, err = client.Objects().Apply(&api.CustomObject{
		Group: Kind.Group, Kind: Kind.Kind, Namespace: "default", Name: "web",
		Spec: map[string]any{"service": "web", "port": 28081},
	}, nil, nil)
	must.NoError(t, err)

	get := func() *api.CustomObject {
		co, _, err := client.Objects().Get(Kind.Group, Kind.Kind, "web", nil)
		if err != nil {
			return nil
		}
		return co
	}
	// The last Ready condition seen, for the log of a failure.
	var last string
	defer func() {
		if t.Failed() {
			t.Logf("last Ready condition of the load balancer: %s", last)
		}
	}()
	must.Wait(t, wait.InitialSuccess(wait.BoolFunc(func() bool {
		co := get()
		if co == nil {
			return false
		}
		c := co.Condition(api.ObjectConditionReady)
		if c != nil {
			last = c.Reason + ": " + c.Message
		}
		return c != nil && c.Status == api.ObjectConditionTrue
	}), wait.Timeout(90*time.Second), wait.Gap(500*time.Millisecond)), must.Sprint("load balancer ready"))

	co := get()
	must.Eq(t, []string{Finalizer}, co.Finalizers)
	must.Eq[any](t, "nomad", co.Spec["class"]) // the schema's default
	proxy, _, err := client.Jobs().Info("lb-web", nil)
	must.NoError(t, err)
	must.Eq(t, "default/crd/norca-io/LoadBalancer/web", proxy.Meta[controller.OwnerMetaKey])
	events, _, err := client.Objects().Events(Kind.Group, Kind.Kind, "web", nil)
	must.NoError(t, err)
	must.SliceNotEmpty(t, events)

	// Deleting the object removes the load balancer through the finalizer.
	_, _, err = client.Objects().Delete(Kind.Group, Kind.Kind, "web", nil, nil)
	must.NoError(t, err)
	must.Wait(t, wait.InitialSuccess(wait.BoolFunc(func() bool {
		if get() != nil {
			return false
		}
		_, _, err := client.Jobs().Info("lb-web", nil)
		return err != nil && strings.Contains(err.Error(), "404")
	}), wait.Timeout(60*time.Second), wait.Gap(500*time.Millisecond)), must.Sprint("load balancer deleted"))

	cancel()
	wg.Wait()
	out = norca("pack", "uninstall", "lb")
	must.StrContains(t, out, `Release "lb" uninstalled`)
	_, _, err = client.ObjectKinds().Info(Kind.Group, Kind.Kind, nil)
	must.ErrorContains(t, err, "404")
	_, _, err = client.ACLPolicies().Info("loadbalancer-controller", nil)
	must.Error(t, err)
}

// registryPack returns the directory of the loadbalancer-operator pack of
// the registry checkout, or skips the test.
func registryPack(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("NORCA_REGISTRY_DIR")
	if dir == "" {
		_, file, _, ok := runtime.Caller(0)
		must.True(t, ok)
		// loadbalancer/operator of norca-platform -> ../norca-registry
		dir = filepath.Join(filepath.Dir(file), "..", "..", "..", "norca-registry")
	}
	pack := filepath.Join(dir, "packs", "loadbalancer-operator")
	if _, err := os.Stat(filepath.Join(pack, "metadata.hcl")); err != nil {
		t.Skipf("no norca-registry checkout with the pack (NORCA_REGISTRY_DIR): %v", err)
	}
	return pack
}
