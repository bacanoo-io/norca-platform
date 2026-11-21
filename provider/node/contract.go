// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package node is the contract between the node scaling operator
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
// the operator sends a bearer token. norca-provider serves the contract of a
// provider at {advertise_url}/providers/{name}, the endpoint of its
// NodeProvider object.
package node

import (
	"context"
	"errors"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/wire"
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
	ReasonInvalid = wire.ReasonInvalid
	// ReasonError: any other failure.
	ReasonError = wire.ReasonError
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

	// Addresses are the IP addresses the traffic of the instance's node to
	// the operator can come from: its own, and the address of a NAT in
	// front of it. Optional: the join endpoint of the operator binds the
	// join and the renewals of a node to them, and without addresses there
	// is no binding. A provider that knows them when the instance is created
	// returns them from Create; the first join of a node waits until the
	// operator has the addresses, or until the instance runs without any.
	// A list, once reported, replaces the one before; an empty list does not
	// clear it.
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
// idempotent by RequestID, and the rule is about the instances the provider
// has, not about the entries of the request:
//
//   - While the provider has an instance with the RequestID (in any state),
//     a request with that ID is answered with the instances of the ID it has,
//     and nothing is created from the entries of the request. This holds also
//     when the provider has fewer of them than the request names: because a
//     create failed part of the way, or because instances of the request were
//     deleted since. The request is not brought up to the number it names.
//     The operator makes a request again after a lost answer, a restart, or
//     when the listing did not show its instances in time. The request made
//     again names only instances of the request that no listing has shown
//     the operator, each with its bootstrap when the operator has a bootstrap
//     source, and no more of them than the group is short of: so its entries
//     can be fewer than, and other than, the instances the provider has of
//     the ID, and the answer is still the instances it has. What a group is
//     short of beyond that, the operator asks for in a request with a new
//     ID.
//   - A provider that has no instance with the RequestID treats the request
//     as a new one and creates its instances. A provider need not remember
//     the IDs of requests whose instances are all gone: it knows a request by
//     the instances of its platform (a label or a tag with the ID).
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

	// Create creates the instances of a request. When the provider has an
	// instance of a request with the same ID, it returns the instances of
	// that ID it has and creates nothing, whatever the request names; when
	// it has none, the request is a new one (see CreateRequest).
	//
	// A create that fails returns an error with a reason (see Fail) and no
	// instance: the answer of a failed create is the error alone (review
	// finding CON-6o). A request refused before anything was made (an
	// invalid one, a full group) leaves nothing. A create that fails part
	// of the way may leave the instances it made before the failure, or
	// remove them: those it leaves carry the request ID and are in the
	// listing of the group, where the operator finds them, and by the rule
	// of CreateRequest the request again makes nothing more; the operator
	// asks for the rest with a new ID.
	Create(ctx context.Context, group string, req CreateRequest) ([]Instance, error)

	// Delete deletes an instance. Deleting one that is gone is not an error.
	Delete(ctx context.Context, group, id string) error
}

// ErrUnknownGroup is the error of a group the provider does not have.
var ErrUnknownGroup = errors.New("unknown group")

// Error is a failure with a reason (ReasonQuota, ReasonNoCapacity,
// ReasonInvalid, ReasonError); its shape is the one of both provider
// contracts (package wire).
type Error = wire.Error

// Fail returns an Error with the reason.
func Fail(reason, format string, args ...any) error { return wire.Fail(reason, format, args...) }

// ReasonOf returns the reason of an error: the reason of an Error, else
// ReasonError.
func ReasonOf(err error) string { return wire.ReasonOf(err) }
