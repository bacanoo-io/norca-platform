// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package params

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testParams = []Param{
	{Name: "endpoint", Type: ParamString, Required: true},
	{Name: "token", Type: ParamString, Secret: true, Default: ""},
	{Name: "size", Type: ParamString, Default: "small", Enum: []string{"small", "large"}, Dynamic: true},
	{Name: "retries", Type: ParamInt, Default: 3, Dynamic: true},
	{Name: "timeout", Type: ParamDuration, Default: 10 * time.Second, Dynamic: true},
	{Name: "debug", Type: ParamBool, Default: false},
	{Name: "zones", Type: ParamList, Default: []string{"a"}},
}

func TestResolve(t *testing.T) {
	got, err := Resolve(testParams, map[string]any{
		"endpoint": "https://x",
		"retries":  float64(5), // as JSON delivers numbers
		"timeout":  "45s",
		"debug":    "true",
		"zones":    []any{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Values{
		"endpoint": "https://x", "token": "", "size": "small", "retries": 5,
		"timeout": 45 * time.Second, "debug": true, "zones": []string{"a", "b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
	if got.String("size") != "small" || got.Int("retries") != 5 || !got.Bool("debug") ||
		got.Duration("timeout") != 45*time.Second || len(got.List("zones")) != 2 {
		t.Fatal("typed getters")
	}
}

func TestResolveErrors(t *testing.T) {
	cases := map[string]struct {
		raw  map[string]any
		want string
	}{
		"required":  {map[string]any{}, `parameter "endpoint" is required`},
		"unknown":   {map[string]any{"endpoint": "x", "nope": 1, "also": 2}, "unknown parameters: also, nope"},
		"enum":      {map[string]any{"endpoint": "x", "size": "huge"}, "must be one of small, large"},
		"int":       {map[string]any{"endpoint": "x", "retries": 1.5}, "must be an integer"},
		"duration":  {map[string]any{"endpoint": "x", "timeout": "soon"}, "must be a duration"},
		"bool":      {map[string]any{"endpoint": "x", "debug": "maybe"}, "must be true or false"},
		"list type": {map[string]any{"endpoint": "x", "zones": []any{1}}, "must be a list of strings"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Resolve(testParams, tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSecrets(t *testing.T) {
	t.Setenv("LB_TEST_TOKEN", "from-env")
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{"env:LB_TEST_TOKEN": "from-env", "file:" + file: "from-file", "literal": "literal"} {
		got, err := Resolve(testParams, map[string]any{"endpoint": "x", "token": in})
		if err != nil || got.String("token") != want {
			t.Fatalf("%s: got %q, %v", in, got.String("token"), err)
		}
	}
	if _, err := Resolve(testParams, map[string]any{"endpoint": "x", "token": "env:LB_TEST_UNSET"}); err == nil {
		t.Fatal("an unset variable must be an error")
	}
}

func TestOverlayAndPublished(t *testing.T) {
	base, err := Resolve(testParams, map[string]any{"endpoint": "https://x", "token": "s3cret"})
	if err != nil {
		t.Fatal(err)
	}

	// Dynamic parameters are overridden, the others and unknown names are not.
	got, err := Overlay(testParams, base, map[string]any{
		"size": "large", "retries": float64(9), "endpoint": "https://evil", "unknown": 1, "debug": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.String("size") != "large" || got.Int("retries") != 9 {
		t.Fatalf("dynamic values not applied: %#v", got)
	}
	if got.String("endpoint") != "https://x" || got.Bool("debug") {
		t.Fatalf("a fixed parameter was overridden: %#v", got)
	}
	if base.String("size") != "small" {
		t.Fatal("Overlay changed its base")
	}
	if _, err := Overlay(testParams, base, map[string]any{"size": "huge"}); err == nil {
		t.Fatal("an invalid dynamic value must be an error")
	}

	// Secrets are not published; durations are strings.
	pub := Published(testParams, base)
	if _, ok := pub["token"]; ok {
		t.Fatal("a secret was published")
	}
	if pub["timeout"] != "10s" || pub["endpoint"] != "https://x" {
		t.Fatalf("published: %#v", pub)
	}
}

func TestValidate(t *testing.T) {
	if err := Validate(testParams); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]Param{
		"bad default":      {{Name: "n", Type: ParamInt, Default: "many"}},
		"required+default": {{Name: "n", Type: ParamString, Required: true, Default: "a"}},
		"secret dynamic":   {{Name: "n", Type: ParamString, Secret: true, Dynamic: true}},
		"duplicate param":  {{Name: "n", Type: ParamString}, {Name: "n", Type: ParamString}},
		"bad name":         {{Name: "a.b", Type: ParamString}},
		"bad type":         {{Name: "n", Type: "float"}},
		"enum not string":  {{Name: "n", Type: ParamInt, Enum: []string{"1"}}},
	}
	for name, ps := range cases {
		if err := Validate(ps); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestMergeAndInfos(t *testing.T) {
	a := Values{"x": 1, "l": []string{"a"}}
	got := Merge(a, Values{"x": 2, "y": "b"})
	if got.Int("x") != 2 || got.String("y") != "b" || len(got.List("l")) != 1 {
		t.Fatalf("merge: %#v", got)
	}
	got.List("l")[0] = "changed"
	if a.List("l")[0] != "a" {
		t.Fatal("Merge shares a list with its input")
	}
	infos := Infos(testParams)
	if len(infos) != len(testParams) || infos[4].Default != "10s" || !infos[1].Secret || !infos[2].Dynamic {
		t.Fatalf("infos: %+v", infos)
	}
}
