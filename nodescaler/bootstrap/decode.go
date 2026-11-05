// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Document is a cloud-init document as Render writes it, read back by a
// provider that runs the node itself instead of handing the document to a
// platform (a container of the lab, for example).
type Document struct {
	Hostname string
	Files    []File
	RunCmd   []string
}

// File is an entry of write_files.
type File struct {
	Path    string
	Mode    uint32
	Content []byte
}

// ConfigFile returns the agent configuration among the files: the one with
// the suffix .hcl.
func (d *Document) ConfigFile() (string, error) {
	var found string
	for _, f := range d.Files {
		if strings.HasSuffix(f.Path, ".hcl") {
			if found != "" {
				return "", fmt.Errorf("two agent configurations: %s and %s", found, f.Path)
			}
			found = f.Path
		}
	}
	if found == "" {
		return "", errors.New("no agent configuration (.hcl) in the document")
	}
	return found, nil
}

// Decode reads a document written by Render: the hostname, write_files with
// base64 content, and runcmd. It is not a YAML parser; any other document is
// an error.
func Decode(doc string) (*Document, error) {
	lines := strings.Split(doc, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "#cloud-config" {
		return nil, errors.New("not a #cloud-config document")
	}
	d := &Document{}
	section := ""
	var cur *File
	finish := func() error {
		if cur == nil {
			return nil
		}
		if cur.Path == "" || !strings.HasPrefix(cur.Path, "/") {
			return fmt.Errorf("a file without an absolute path: %q", cur.Path)
		}
		d.Files = append(d.Files, *cur)
		cur = nil
		return nil
	}
	for i, raw := range lines[1:] {
		line := strings.TrimRight(raw, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			if err := finish(); err != nil {
				return nil, err
			}
			key, value, _ := strings.Cut(line, ":")
			switch key {
			case "hostname":
				d.Hostname = strings.TrimSpace(value)
			case "write_files", "runcmd":
				section = key
			default:
				return nil, fmt.Errorf("line %d: unknown key %q", i+2, key)
			}
			continue
		}
		item := strings.TrimSpace(line)
		switch section {
		case "runcmd":
			d.RunCmd = append(d.RunCmd, strings.TrimPrefix(item, "- "))
		case "write_files":
			if rest, ok := strings.CutPrefix(item, "- "); ok {
				if err := finish(); err != nil {
					return nil, err
				}
				cur = &File{Mode: 0o644}
				item = rest
			}
			if cur == nil {
				return nil, fmt.Errorf("line %d: a field outside a file", i+2)
			}
			key, value, _ := strings.Cut(item, ":")
			value = strings.TrimSpace(value)
			switch key {
			case "path":
				cur.Path = value
			case "permissions":
				mode, err := strconv.ParseUint(strings.Trim(value, "'\""), 8, 32)
				if err != nil {
					return nil, fmt.Errorf("line %d: permissions %q", i+2, value)
				}
				cur.Mode = uint32(mode)
			case "encoding":
				if value != "b64" {
					return nil, fmt.Errorf("line %d: encoding %q (only b64)", i+2, value)
				}
			case "content":
				data, err := base64.StdEncoding.DecodeString(value)
				if err != nil {
					return nil, fmt.Errorf("line %d: content: %w", i+2, err)
				}
				cur.Content = data
			default:
				return nil, fmt.Errorf("line %d: unknown file field %q", i+2, key)
			}
		default:
			return nil, fmt.Errorf("line %d: an item outside a section", i+2)
		}
	}
	if err := finish(); err != nil {
		return nil, err
	}
	return d, nil
}
