// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package provider is the contract between the node scaling operator
// (norca-nodescaler) and a node provider: an application outside the core
// that creates and deletes the machines of one platform. The operator calls
// the provider over HTTP:
//
//	GET    {endpoint}/v1/groups/{group}                 the instances and the capacity of one node
//	POST   {endpoint}/v1/groups/{group}/instances       create instances (idempotent by request_id)
//	DELETE {endpoint}/v1/groups/{group}/instances/{id}  delete one instance
//
// A provider implements Provider and serves it with NewHandler; the operator
// calls it with Client. Credentials of the platform stay with the provider;
// the operator sends a bearer token.
package provider

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The states of an instance.
const (
	StateCreating = "creating"
	StateRunning  = "running"
	StateDeleting = "deleting"
	StateFailed   = "failed"
)

// The reasons a provider cannot create an instance. A provider says so with
// a reason and does not retry for ever.
const (
	// ReasonQuota: a limit of the platform or of the group is reached.
	ReasonQuota = "quota"
	// ReasonNoCapacity: the platform has no machine of the kind now.
	ReasonNoCapacity = "no_capacity"
	// ReasonInvalid: the request or its parameters are wrong.
	ReasonInvalid = "invalid"
	// ReasonError: any other failure.
	ReasonError = "error"
)

// Capacity is the size of one machine of a kind: CPU in MHz, memory and disk
// in MB.
type Capacity struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	Disk   int64 `json:"disk"`
}

// Instance is a machine of a group.
type Instance struct {
	ID    string `json:"id"`
	State string `json:"state"`

	// Name is the name the operator gave the instance in the request; the
	// node it runs registers with it (meta norca.instance), which is how a
	// node is matched with its instance.
	Name string `json:"name,omitempty"`

	// Reason and Message say why an instance failed.
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`

	// RequestID is the request that created the instance.
	RequestID string `json:"request_id,omitempty"`

	Addresses []string          `json:"addresses,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// Group is a group (a zone) of the provider: its instances and the capacity
// of one machine of the kind the parameters name. The capacity is known also
// when the group has no instance: it is a property of the kind, not of a
// running machine.
type Group struct {
	Name      string     `json:"name"`
	Capacity  *Capacity  `json:"capacity,omitempty"`
	Instances []Instance `json:"instances"`
}

// CreateRequest asks for instances, one per entry of Instances. It is
// idempotent by RequestID: the same request again returns the instances of
// the first one and creates nothing, so a request repeated after a failure or
// a restart of the operator does not create more machines.
type CreateRequest struct {
	RequestID  string            `json:"request_id"`
	Instances  []InstanceRequest `json:"instances"`
	Parameters map[string]any    `json:"parameters,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// InstanceRequest is one instance of a request: its name (the machine name
// on the platform, and the name of the node) and what it runs first. Each
// instance has a bootstrap of its own: it holds the key and the certificate
// of its node.
type InstanceRequest struct {
	Name      string     `json:"name"`
	Bootstrap *Bootstrap `json:"bootstrap,omitempty"`
}

// Bootstrap is what a new machine runs first: the agent, its configuration
// and its certificate, as cloud-init or the user data of the platform. The
// operator renders it and hands it to the provider only.
type Bootstrap struct {
	UserData string `json:"user_data"`
}

// CreateResponse lists the instances of a request.
type CreateResponse struct {
	Instances []Instance `json:"instances"`
}

// Provider is a node provider.
type Provider interface {
	// Group returns the instances of a group and the capacity of a machine
	// of the kind the parameters name. ErrUnknownGroup if there is no such
	// group.
	Group(ctx context.Context, group string, params map[string]any) (*Group, error)

	// Create creates the instances of a request, or returns those of an
	// earlier request with the same ID. An error with a reason (see Fail)
	// says why nothing was created.
	Create(ctx context.Context, group string, req CreateRequest) ([]Instance, error)

	// Delete deletes an instance. Deleting one that is gone is not an error.
	Delete(ctx context.Context, group, id string) error
}

// ErrUnknownGroup is the error of a group the provider does not have.
var ErrUnknownGroup = errors.New("unknown group")

// Error is a failure with a reason (ReasonQuota, ReasonNoCapacity,
// ReasonInvalid, ReasonError).
type Error struct {
	Reason  string `json:"reason"`
	Message string `json:"error"`
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

// Fail returns an Error with the reason.
func Fail(reason, format string, args ...any) error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf returns the reason of an error: the reason of an Error, else
// ReasonError.
func ReasonOf(err error) string {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Reason
	}
	return ReasonError
}
