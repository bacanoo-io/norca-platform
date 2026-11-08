// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package params declares the settings of a provider type and resolves the
// configured values against the declarations: types, defaults, required
// values, secrets ("env:NAME", "file:/path"), and the dynamic parameters a
// LoadBalancerClass object can change while the provider runs. A provider
// type never parses its own settings.
package params

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ParamType is the type of a parameter.
type ParamType string

const (
	ParamString   ParamType = "string"
	ParamInt      ParamType = "int"
	ParamBool     ParamType = "bool"
	ParamDuration ParamType = "duration"
	ParamList     ParamType = "list" // list of strings
)

// Param declares one setting of a provider type. The declarations drive the
// validation of the configuration, the defaults, the introspection endpoint
// and what is published in the LoadBalancerClass object, so a provider type
// never parses its own settings.
type Param struct {
	Name        string
	Type        ParamType
	Description string

	// Required parameters have no default and must be configured.
	Required bool

	// Default is used when the parameter is not configured. It must be
	// valid for Type.
	Default any

	// Secret parameters are never published. A string value may refer to
	// the environment ("env:NAME") or to a file ("file:/path"), so that the
	// secret itself is not in the configuration file.
	Secret bool

	// Dynamic parameters can be changed while the provider runs, by editing
	// "parameters" of the LoadBalancerClass object: the operator sends the
	// current values with every call and they override the configured ones.
	// All other parameters are fixed when the provider starts.
	Dynamic bool

	// Enum lists the allowed values of a string parameter.
	Enum []string
}

// Values are the resolved parameters of an instance (or of one call):
// every declared parameter is present, with its Go type (string, int, bool,
// time.Duration or []string).
type Values map[string]any

// String returns a string parameter.
func (v Values) String(name string) string { s, _ := v[name].(string); return s }

// Int returns an int parameter.
func (v Values) Int(name string) int { n, _ := v[name].(int); return n }

// Bool returns a bool parameter.
func (v Values) Bool(name string) bool { b, _ := v[name].(bool); return b }

// Duration returns a duration parameter.
func (v Values) Duration(name string) time.Duration { d, _ := v[name].(time.Duration); return d }

// List returns a list parameter.
func (v Values) List(name string) []string { l, _ := v[name].([]string); return l }

// Clone returns a copy of the values.
func (v Values) Clone() Values {
	out := make(Values, len(v))
	for k, val := range v {
		if l, ok := val.([]string); ok {
			val = slices.Clone(l)
		}
		out[k] = val
	}
	return out
}

// Validate checks the declarations of the parameters of a type.
func Validate(params []Param) error {
	seen := map[string]bool{}
	for _, p := range params {
		if p.Name == "" || strings.ContainsAny(p.Name, " .:/") {
			return fmt.Errorf("invalid parameter name %q", p.Name)
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate parameter %q", p.Name)
		}
		seen[p.Name] = true
		switch p.Type {
		case ParamString, ParamInt, ParamBool, ParamDuration, ParamList:
		default:
			return fmt.Errorf("parameter %q: invalid type %q", p.Name, p.Type)
		}
		if p.Required && p.Default != nil {
			return fmt.Errorf("parameter %q: a required parameter has no default", p.Name)
		}
		if p.Dynamic && p.Secret {
			return fmt.Errorf("parameter %q: a secret cannot be dynamic (it is never published)", p.Name)
		}
		if len(p.Enum) > 0 && p.Type != ParamString {
			return fmt.Errorf("parameter %q: enum is for strings", p.Name)
		}
		if p.Default != nil {
			if _, err := coerce(p, p.Default); err != nil {
				return fmt.Errorf("parameter %q: invalid default: %w", p.Name, err)
			}
		}
	}
	return nil
}

// Resolve validates the configured values against the declarations and
// returns all parameters: unknown names and wrong types are errors, missing
// ones get their default.
func Resolve(params []Param, raw map[string]any) (Values, error) {
	known := make(map[string]Param, len(params))
	for _, p := range params {
		known[p.Name] = p
	}
	var unknown []string
	for name := range raw {
		if _, ok := known[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown parameters: %s", strings.Join(unknown, ", "))
	}

	out := make(Values, len(params))
	var errs []string
	for _, p := range params {
		val, set := raw[p.Name]
		switch {
		case set:
		case p.Default != nil:
			val = p.Default
		case p.Required:
			errs = append(errs, fmt.Sprintf("parameter %q is required", p.Name))
			continue
		default:
			out[p.Name] = zero(p.Type)
			continue
		}
		v, err := coerce(p, val)
		if err != nil {
			errs = append(errs, fmt.Sprintf("parameter %q: %v", p.Name, err))
			continue
		}
		out[p.Name] = v
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, nil
}

// Overlay returns base with the dynamic parameters of raw applied: the values
// the operator sends with a call. Parameters that are not dynamic, and
// unknown names, are ignored, so the configuration of the instance stays in
// charge of them; an invalid value of a dynamic parameter is an error.
func Overlay(params []Param, base Values, raw map[string]any) (Values, error) {
	out := base.Clone()
	for _, p := range params {
		val, ok := raw[p.Name]
		if !p.Dynamic || !ok {
			continue
		}
		v, err := coerce(p, val)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", p.Name, err)
		}
		out[p.Name] = v
	}
	return out, nil
}

// Published returns the parameters that are written to the LoadBalancerClass
// object: every parameter that is not secret, as JSON-friendly values.
func Published(params []Param, values Values) map[string]any {
	out := map[string]any{}
	for _, p := range params {
		if p.Secret {
			continue
		}
		v, ok := values[p.Name]
		if !ok {
			continue
		}
		if d, ok := v.(time.Duration); ok {
			v = d.String()
		}
		out[p.Name] = v
	}
	return out
}

func zero(t ParamType) any {
	switch t {
	case ParamInt:
		return 0
	case ParamBool:
		return false
	case ParamDuration:
		return time.Duration(0)
	case ParamList:
		return []string(nil)
	default:
		return ""
	}
}

// coerce converts a configured value (as it comes from JSON or from the
// LoadBalancerClass object) to the Go type of the parameter.
func coerce(p Param, val any) (any, error) {
	switch p.Type {
	case ParamString:
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("must be a string")
		}
		if p.Secret {
			var err error
			if s, err = resolveSecret(s); err != nil {
				return nil, err
			}
		}
		if len(p.Enum) > 0 && !slices.Contains(p.Enum, s) {
			return nil, fmt.Errorf("must be one of %s", strings.Join(p.Enum, ", "))
		}
		return s, nil

	case ParamInt:
		switch n := val.(type) {
		case int:
			return n, nil
		case int64:
			return int(n), nil
		case float64:
			if n != math.Trunc(n) || math.Abs(n) > 1<<53 {
				return nil, fmt.Errorf("must be an integer")
			}
			return int(n), nil
		case json.Number:
			i, err := n.Int64()
			if err != nil {
				return nil, fmt.Errorf("must be an integer")
			}
			return int(i), nil
		case string:
			i, err := strconv.Atoi(strings.TrimSpace(n))
			if err != nil {
				return nil, fmt.Errorf("must be an integer")
			}
			return i, nil
		}
		return nil, fmt.Errorf("must be an integer")

	case ParamBool:
		switch b := val.(type) {
		case bool:
			return b, nil
		case string:
			parsed, err := strconv.ParseBool(strings.TrimSpace(b))
			if err != nil {
				return nil, fmt.Errorf("must be true or false")
			}
			return parsed, nil
		}
		return nil, fmt.Errorf("must be true or false")

	case ParamDuration:
		switch d := val.(type) {
		case time.Duration:
			return d, nil
		case string:
			parsed, err := time.ParseDuration(strings.TrimSpace(d))
			if err != nil {
				return nil, fmt.Errorf("must be a duration such as 30s")
			}
			return parsed, nil
		case float64: // seconds
			return time.Duration(d * float64(time.Second)), nil
		case int:
			return time.Duration(d) * time.Second, nil
		}
		return nil, fmt.Errorf("must be a duration such as 30s")

	case ParamList:
		switch l := val.(type) {
		case []string:
			return slices.Clone(l), nil
		case []any:
			out := make([]string, len(l))
			for i, e := range l {
				s, ok := e.(string)
				if !ok {
					return nil, fmt.Errorf("must be a list of strings")
				}
				out[i] = s
			}
			return out, nil
		case string:
			if strings.TrimSpace(l) == "" {
				return []string(nil), nil
			}
			parts := strings.Split(l, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			return parts, nil
		}
		return nil, fmt.Errorf("must be a list of strings")
	}
	return nil, fmt.Errorf("unsupported type %q", p.Type)
}

// resolveSecret resolves "env:NAME" and "file:/path"; other values are
// returned as they are.
func resolveSecret(s string) (string, error) {
	switch {
	case strings.HasPrefix(s, "env:"):
		name := strings.TrimPrefix(s, "env:")
		v := os.Getenv(name)
		if v == "" {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
		return v, nil
	case strings.HasPrefix(s, "file:"):
		b, err := os.ReadFile(strings.TrimPrefix(s, "file:"))
		if err != nil {
			return "", fmt.Errorf("reading the secret file: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return s, nil
}

// ResolveSecret resolves a configured secret (a token, for example) that may
// be an "env:NAME" or "file:/path" reference.
func ResolveSecret(s string) (string, error) { return resolveSecret(s) }

// Merge returns the values of all the maps in one; a later map wins. The
// shared parameters of a type and those of a section (a group, a class) are
// declared apart and handed to the constructors together.
func Merge(all ...Values) Values {
	out := Values{}
	for _, v := range all {
		maps.Copy(out, v.Clone())
	}
	return out
}

// Info describes a parameter for the listing of the types (GET /v1/types
// and "norca-provider types").
type Info struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Default     any      `json:"default,omitempty"`
	Secret      bool     `json:"secret,omitempty"`
	Dynamic     bool     `json:"dynamic,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// Infos describes the declarations.
func Infos(params []Param) []Info {
	out := make([]Info, 0, len(params))
	for _, p := range params {
		def := p.Default
		if d, ok := def.(time.Duration); ok {
			def = d.String()
		}
		out = append(out, Info{
			Name: p.Name, Type: string(p.Type), Description: p.Description, Required: p.Required,
			Default: def, Secret: p.Secret, Dynamic: p.Dynamic, Enum: p.Enum,
		})
	}
	return out
}
