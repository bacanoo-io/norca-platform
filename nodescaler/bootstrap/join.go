// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"strings"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"
)

// JoinTicket is what a node that joins with a key of its own receives
// instead of a certificate: a one-time token and the join endpoints.
type JoinTicket struct {
	Token string
	URLs  []string
}

// JoinUnit is the systemd unit of norca-join that RenderJoin writes.
const JoinUnit = "norca-join"

// RenderJoin renders the bootstrap of a node that joins with a key of its
// own (package join): the cluster CA, the agent configuration, the
// configuration of norca-join with the one-time token, and its systemd
// unit. No key and no certificate travel: norca-join makes the key on the
// machine, gets the certificate from the join endpoint, starts the agent
// (Service) and renews the certificate. The node carries the meta
// norca.identity = "join" and no expiry of a certificate: it is not
// replaced for its age.
func (t *Template) RenderJoin(n Node, ticket JoinTicket) (*Rendered, error) {
	if !join.ValidInstance(n.Name) {
		return nil, fmt.Errorf("the node name %q is not a host name", n.Name)
	}
	if ticket.Token == "" || len(ticket.URLs) == 0 {
		return nil, errors.New("a join needs a token and an endpoint")
	}
	if !strings.HasPrefix(t.JoinBinary, "/") || strings.ContainsAny(t.JoinBinary, " \t\n\"'") {
		return nil, fmt.Errorf("the join binary %q is not an absolute path without spaces", t.JoinBinary)
	}
	n.Region = t.Region
	n.CAFile = t.TLSDir + "/ca.pem"
	n.CertFile = t.TLSDir + "/client.pem"
	n.KeyFile = t.TLSDir + "/client-key.pem"
	meta := make(map[string]string, len(n.Meta)+1)
	maps.Copy(meta, n.Meta)
	meta[join.MetaIdentity] = join.IdentityJoin
	n.Meta = meta

	var agent bytes.Buffer
	if err := t.agent.Execute(&agent, n); err != nil {
		return nil, fmt.Errorf("rendering the agent configuration of node %s: %w", n.Name, err)
	}

	joinFile := path.Join(path.Dir(t.ConfigFile), join.ConfigFileName)
	conf := join.Config{
		Instance: n.Name, Region: t.Region,
		URLs: ticket.URLs, Endpoint: join.EndpointURI(t.TrustDomain, t.Region).String(), Token: ticket.Token,
		CAFile: n.CAFile, CertFile: n.CertFile, KeyFile: n.KeyFile,
		StateFile: t.TLSDir + "/norca-join.state", Service: t.Service,
	}
	if err := conf.Check(); err != nil {
		return nil, fmt.Errorf("the join of node %s: %w", n.Name, err)
	}
	confJSON, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		return nil, err
	}
	unit := fmt.Sprintf(`[Unit]
Description=Norca node identity (norca-join): the key and the certificate of the node
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=%s run -config %s
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
`, t.JoinBinary, joinFile)

	var doc strings.Builder
	doc.WriteString("#cloud-config\n")
	doc.WriteString("hostname: " + n.Name + "\n")
	doc.WriteString("write_files:\n")
	writeFile(&doc, n.CAFile, "0644", t.CAPEM)
	writeFile(&doc, t.ConfigFile, "0640", agent.Bytes())
	writeFile(&doc, joinFile, "0600", confJSON)
	writeFile(&doc, "/etc/systemd/system/"+JoinUnit+".service", "0644", []byte(unit))
	t.writeInstallScript(&doc)
	doc.WriteString("runcmd:\n")
	t.runInstallScript(&doc)
	doc.WriteString("  - [systemctl, daemon-reload]\n")
	doc.WriteString("  - [systemctl, enable, --now, " + JoinUnit + "]\n")
	return &Rendered{UserData: doc.String()}, nil
}
