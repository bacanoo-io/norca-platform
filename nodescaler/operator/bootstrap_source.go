// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca/norca/controller"
)

// BootstrapSource gives the template of the bootstrap of the nodes.
type BootstrapSource interface {
	Template(ctx context.Context) (*bootstrap.Template, error)
}

// VariableBootstrap reads the template from a Variable, by default
// nodescaler/bootstrap/<region>, written by the deployer. The Variable holds
// the key of the node CA: only the operator may read it.
type VariableBootstrap struct {
	Vars      controller.VariablesAPI
	Namespace string
	Path      string
	Region    string
}

// Template implements BootstrapSource. The Variable is read for every
// request, so a new node CA is used at once.
func (v *VariableBootstrap) Template(ctx context.Context) (*bootstrap.Template, error) {
	vr, err := v.Vars.ReadVariable(ctx, v.Namespace, v.Path)
	if errors.Is(err, controller.ErrNotFound) {
		return nil, fmt.Errorf("the Variable %q does not exist", v.Path)
	}
	if err != nil {
		return nil, fmt.Errorf("reading the Variable %q: %w", v.Path, err)
	}
	t, err := bootstrap.Parse(v.Region, vr.Items)
	if err != nil {
		return nil, fmt.Errorf("the Variable %q: %w", v.Path, err)
	}
	return t, nil
}
