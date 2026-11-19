// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
	"github.com/bacanoo-io/norca/norca/controller/fake"
	"github.com/shoenig/test/must"
)

// fakeJobRegistrar registers jobs on a fake backend with check-and-set on
// their job modify index, as the servers do for EnforceRegister.
type fakeJobRegistrar struct {
	mu sync.Mutex
	b  *fake.Backend
}

func (f *fakeJobRegistrar) EnforceRegisterJob(ctx context.Context, job *api.Job, modifyIndex uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := "default"
	if job.Namespace != nil && *job.Namespace != "" {
		ns = *job.Namespace
	}
	var current uint64
	if j := f.b.GetJob(ns, *job.ID); j != nil {
		current = *j.JobModifyIndex
	}
	if current != modifyIndex {
		return fmt.Errorf("%w: job %q has the job modify index %d, not %d", controller.ErrConflict, *job.ID, current, modifyIndex)
	}
	return f.b.RegisterJob(ctx, job)
}

// racingJobs is a fake backend on which somebody else registers a job right
// after the next read of its ID: between the provider's read of the proxy
// job and its registration.
type racingJobs struct {
	*fake.Backend

	mu  sync.Mutex
	job *api.Job // registered after the next read of its ID; nil: none
}

func (r *racingJobs) ReadJob(ctx context.Context, namespace, id string) (*api.Job, error) {
	j, err := r.Backend.ReadJob(ctx, namespace, id)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.job != nil && *r.job.ID == id {
		r.Backend.PutJob(r.job)
		r.job = nil
	}
	return j, err
}

func (r *racingJobs) registerAfterRead(job *api.Job) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.job = job
}

// A job with the proxy's ID that somebody else registers between the
// provider's read and its registration is not overwritten, whether the proxy
// job was absent or owned by the load balancer; the next reconcile reports
// it as not owned (review finding ATM-5).
func TestNomadProvider_EnsureJobRegisteredMeanwhile(t *testing.T) {
	tests := []struct {
		name     string
		existing bool // the load balancer's proxy job exists before
	}{
		{name: "absent job", existing: false},
		{name: "owned job", existing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			b := &racingJobs{Backend: fake.New()}
			p, err := newNomadProvider(NomadProviderConfig{Command: "/usr/local/bin/loadbalancer"}, b, &fakeJobRegistrar{b: b.Backend}, newFakeCluster())
			must.NoError(t, err)
			obj := &controller.Object{Kind: Kind, Key: controller.ObjectKey{Namespace: "default", Name: "web"}}
			spec := Spec{Service: "web", Port: 80}
			spec.Defaults()
			if tt.existing {
				_, err := p.Ensure(ctx, obj, spec, nil)
				must.NoError(t, err)
				must.True(t, controller.IsOwnedBy(b.GetJob("default", "lb-web").Meta, obj))
				spec.Port = 81 // a changed spec registers the job again
			}

			foreign := api.NewServiceJob("lb-web", "lb-web", "global", 50)
			foreign.AddTaskGroup(api.NewTaskGroup("g", 1))
			b.registerAfterRead(foreign)
			_, err = p.Ensure(ctx, obj, spec, nil)
			must.ErrorIs(t, err, controller.ErrConflict)
			must.MapNotContainsKey(t, b.GetJob("default", "lb-web").Meta, controller.OwnerMetaKey)

			// The next reconcile finds the job of somebody else.
			res, err := p.Ensure(ctx, obj, spec, nil)
			must.NoError(t, err)
			must.StrContains(t, res.Message, "is not owned by this load balancer")
			must.MapNotContainsKey(t, b.GetJob("default", "lb-web").Meta, controller.OwnerMetaKey)
		})
	}
}

// The API client registers a job with the job modify index enforced; the
// servers' answer to a failed check is a conflict, other errors are not.
func TestAPIJobRegistrar_EnforceRegisterJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req api.JobRegisterRequest
		if r.Method != http.MethodPut || r.URL.Path != "/v1/jobs" || json.NewDecoder(r.Body).Decode(&req) != nil ||
			!req.EnforceIndex || req.Job == nil || r.URL.Query().Get("namespace") != "apps" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		switch req.JobModifyIndex {
		case 5:
			_, _ = w.Write([]byte(`{"EvalID":"e"}`))
		case 0:
			http.Error(w, "rpc error: Enforcing job modify index 0: job already exists", http.StatusInternalServerError)
		default:
			http.Error(w, "rpc error: permission denied", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	client, err := api.NewClient(&api.Config{Address: srv.URL})
	must.NoError(t, err)
	r := &apiJobRegistrar{client: client}
	job := api.NewServiceJob("lb-web", "lb-web", "global", 50)
	job.Namespace = pointerOf("apps")

	must.NoError(t, r.EnforceRegisterJob(t.Context(), job, 5))
	must.ErrorIs(t, r.EnforceRegisterJob(t.Context(), job, 0), controller.ErrConflict)
	err = r.EnforceRegisterJob(t.Context(), job, 9)
	must.Error(t, err)
	must.False(t, errors.Is(err, controller.ErrConflict))
}
