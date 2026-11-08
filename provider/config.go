// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
)

// File is the configuration file of norca-provider (JSON):
//
//	{
//	  "listen": ":8443",
//	  "tls": {"cert": "...", "key": "..."},
//	  "advertise_url": "https://provider.service.norca:8443",
//	  "heartbeat_interval": "30s",
//	  "providers": [{
//	    "name": "hetzner-eu", "type": "hcloud",
//	    "params": {"token": "${HCLOUD_TOKEN}"},
//	    "token_file": "/secrets/token", "token_variable": "providers/hetzner-eu",
//	    "node": {"groups": [{"name": "fsn1", "max": 10, "params": {}}]},
//	    "loadbalancer": {"classes": [{"class": "hcloud", "params": {"location": "fsn1"}}]}
//	  }]
//	}
//
// Every string may refer to the environment as ${NAME} or ${NAME:-default}
// (see ExpandEnv). Only the sections a provider has are started, registered
// and served.
type File struct {
	// Listen is the address the server listens on (default ":8443").
	Listen string `json:"listen"`

	// TLS makes the server https. Without it the server is http, which the
	// LoadBalancer operator only accepts with -class-allow-http.
	TLS *TLS `json:"tls,omitempty"`

	// AdvertiseURL is the base URL the operators reach this process on, for
	// example "https://provider.service.norca:8443". The endpoint of a node
	// provider is AdvertiseURL + "/providers/<name>", that of a class
	// AdvertiseURL + "/classes/<class>".
	AdvertiseURL string `json:"advertise_url"`

	// HeartbeatInterval is how often the status of the registrations is
	// updated (default 30s).
	HeartbeatInterval string `json:"heartbeat_interval"`

	// Providers are the providers this process serves.
	Providers []ProviderConfig `json:"providers"`
}

// TLS is the certificate of the https server.
type TLS struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

// ProviderConfig is one provider: an instance of a type with its shared
// parameters, its token, and the sections it serves.
type ProviderConfig struct {
	// Name is the name of the provider: the name of its NodeProvider object
	// and the path of its node contract (/providers/<name>).
	Name string `json:"name"`

	// Type is the ID of the platform.
	Type string `json:"type"`

	// Params are the parameters shared by the sections (credentials, the
	// API of the platform).
	Params map[string]any `json:"params,omitempty"`

	// Token is the bearer token the operators must send, for the node
	// contract and for every class of the provider; "env:NAME" and
	// "file:/path" are resolved. TokenFile names a file with it instead.
	// One of them is required unless NoAuth is set.
	Token     string `json:"token,omitempty"`
	TokenFile string `json:"token_file,omitempty"`

	// NoAuth accepts calls without a token. Only for a trusted network.
	NoAuth bool `json:"no_auth,omitempty"`

	// TokenVariable is the path of the Variable that holds the same token
	// on the Norca side (item "token"), where the operators read it. It is
	// written to the registrations.
	TokenVariable string `json:"token_variable,omitempty"`

	// Node is the node provider section; nil serves no node provider.
	Node *NodeSection `json:"node,omitempty"`

	// LoadBalancer is the load balancer section; nil serves no class.
	LoadBalancer *LBSection `json:"loadbalancer,omitempty"`
}

// NodeSection configures the node provider of a provider.
type NodeSection struct {
	// Endpoint is the full URL the node scaling operator calls. Without it
	// the endpoint is advertise_url + "/providers/<name>".
	Endpoint string `json:"endpoint,omitempty"`

	// Params are the parameters of the node section of the type.
	Params map[string]any `json:"params,omitempty"`

	// Groups are the groups (zones) the provider offers.
	Groups []GroupConfig `json:"groups"`
}

// GroupConfig is a group of a node provider.
type GroupConfig struct {
	Name string `json:"name"`

	// Max is the most instances of the group; 0 is no limit.
	Max int `json:"max,omitempty"`

	// Params are the group parameters of the type.
	Params map[string]any `json:"params,omitempty"`
}

// LBSection configures the load balancer classes of a provider.
type LBSection struct {
	Classes []ClassConfig `json:"classes"`
}

// ClassConfig is a load balancer class.
type ClassConfig struct {
	// Class is the class name, which a service names in
	// expose { load_balancer { class = "..." } }. Unique in the file.
	Class string `json:"class"`

	// Params are the class parameters of the type.
	Params map[string]any `json:"params,omitempty"`

	// Internal limits the class to public load balancers when false, and
	// requires the capability of the type when true; unset offers what the
	// type can.
	Internal *bool `json:"internal,omitempty"`

	// Ports limits the frontend ports of the class ("80,443,8000-8100");
	// empty is any.
	Ports string `json:"ports,omitempty"`

	// Endpoint is the full URL the operator calls for this class. Without it
	// the endpoint is advertise_url + "/classes/<class>". Set it when the
	// class is reached on another address or port, through a proxy or a
	// port mapping.
	Endpoint string `json:"endpoint,omitempty"`
}

// ExpandEnv replaces ${NAME} and ${NAME:-default} in every string of the
// configuration with the environment of the process, so a file can be written
// for a job whose ports and addresses are only known at run time, for example
// "listen": ":${NORCA_PORT_api}". A variable that is not set and has no
// default is an error; $NAME without braces is left as it is.
func ExpandEnv(raw []byte, lookup func(string) (string, bool)) ([]byte, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var firstErr error
	var walk func(v any) any
	walk = func(v any) any {
		switch t := v.(type) {
		case string:
			s, err := expandString(t, lookup)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			return s
		case []any:
			for i := range t {
				t[i] = walk(t[i])
			}
		case map[string]any:
			for k := range t {
				t[k] = walk(t[k])
			}
		}
		return v
	}
	doc = walk(doc)
	if firstErr != nil {
		return nil, firstErr
	}
	return json.Marshal(doc)
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

func expandString(s string, lookup func(string) (string, bool)) (string, error) {
	var err error
	out := envRef.ReplaceAllStringFunc(s, func(m string) string {
		sub := envRef.FindStringSubmatch(m)
		if v, ok := lookup(sub[1]); ok && v != "" {
			return v
		}
		if strings.Contains(m, ":-") {
			return sub[2]
		}
		if err == nil {
			err = fmt.Errorf("environment variable %s is not set (used as %s)", sub[1], m)
		}
		return m
	})
	return out, err
}

// Load reads and checks a configuration file.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the configuration file the operator names
	if err != nil {
		return nil, err
	}
	f, err := Parse(raw, os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// Parse reads a configuration with the environment of lookup and checks
// what does not need the types.
func Parse(raw []byte, lookup func(string) (string, bool)) (*File, error) {
	raw, err := ExpandEnv(raw, lookup)
	if err != nil {
		return nil, err
	}
	var f File
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, err
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

// Validate checks what does not need the types, and fills in the defaults.
func (f *File) Validate() error {
	if f.Listen == "" {
		f.Listen = ":8443"
	}
	if f.AdvertiseURL != "" {
		if err := checkURL(f.AdvertiseURL); err != nil {
			return fmt.Errorf("advertise_url: %w", err)
		}
	}
	if f.TLS != nil && (f.TLS.Cert == "" || f.TLS.Key == "") {
		return fmt.Errorf("tls needs cert and key")
	}
	if f.HeartbeatInterval != "" {
		if d, err := time.ParseDuration(f.HeartbeatInterval); err != nil || d <= 0 {
			return fmt.Errorf("heartbeat_interval: %q is not a positive duration", f.HeartbeatInterval)
		}
	}
	if len(f.Providers) == 0 {
		return fmt.Errorf("at least one provider is required")
	}
	names := map[string]bool{}
	classes := map[string]string{}
	for i, p := range f.Providers {
		if !ValidName(p.Name) {
			return fmt.Errorf("providers[%d]: invalid name %q", i, p.Name)
		}
		if names[p.Name] {
			return fmt.Errorf("provider %q is configured twice", p.Name)
		}
		names[p.Name] = true
		if p.Type == "" {
			return fmt.Errorf("provider %q: type is required", p.Name)
		}
		switch {
		case p.Token != "" && p.TokenFile != "":
			return fmt.Errorf("provider %q: token and token_file exclude each other", p.Name)
		case p.Token == "" && p.TokenFile == "" && !p.NoAuth:
			return fmt.Errorf("provider %q: token or token_file is required (or no_auth for a trusted network)", p.Name)
		}
		if p.Node == nil && p.LoadBalancer == nil {
			return fmt.Errorf("provider %q: has neither a node nor a loadbalancer section", p.Name)
		}
		if n := p.Node; n != nil {
			if err := checkEndpoint(n.Endpoint, f.AdvertiseURL); err != nil {
				return fmt.Errorf("provider %q: node: %w", p.Name, err)
			}
			if len(n.Groups) == 0 {
				return fmt.Errorf("provider %q: node: at least one group is required", p.Name)
			}
			groups := map[string]bool{}
			for j, g := range n.Groups {
				if !ValidName(g.Name) {
					return fmt.Errorf("provider %q: node.groups[%d]: invalid name %q", p.Name, j, g.Name)
				}
				if groups[g.Name] {
					return fmt.Errorf("provider %q: group %q is configured twice", p.Name, g.Name)
				}
				groups[g.Name] = true
				if g.Max < 0 {
					return fmt.Errorf("provider %q: group %q: max must not be negative", p.Name, g.Name)
				}
			}
		}
		if l := p.LoadBalancer; l != nil {
			if len(l.Classes) == 0 {
				return fmt.Errorf("provider %q: loadbalancer: at least one class is required", p.Name)
			}
			for j, c := range l.Classes {
				if !lb.ValidClass(c.Class) {
					return fmt.Errorf("provider %q: loadbalancer.classes[%d]: invalid class name %q", p.Name, j, c.Class)
				}
				if other, dup := classes[c.Class]; dup {
					return fmt.Errorf("class %q is configured twice (providers %q and %q)", c.Class, other, p.Name)
				}
				classes[c.Class] = p.Name
				if _, err := lb.ParsePorts(c.Ports); err != nil {
					return fmt.Errorf("class %q: ports: %w", c.Class, err)
				}
				if err := checkEndpoint(c.Endpoint, f.AdvertiseURL); err != nil {
					return fmt.Errorf("class %q: %w", c.Class, err)
				}
			}
		}
	}
	return nil
}

func checkEndpoint(endpoint, advertise string) error {
	switch {
	case endpoint != "":
		if err := checkURL(endpoint); err != nil {
			return fmt.Errorf("endpoint: %w", err)
		}
	case advertise == "":
		return fmt.Errorf("needs an endpoint, or advertise_url for all")
	}
	return nil
}

func checkURL(s string) error {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return fmt.Errorf("%q must be an http or https URL", s)
	}
	return nil
}

// Interval returns the heartbeat interval.
func (f *File) Interval() time.Duration {
	d, err := time.ParseDuration(f.HeartbeatInterval)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}
