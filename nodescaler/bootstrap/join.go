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
	"regexp"
	"strings"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"
)

// JoinTicket is what a node that joins with a key of its own receives
// instead of a certificate: a one-time token and the join endpoints, and
// where it keeps its key (join.KeyStoreFile, KeyStoreTPM or KeyStoreAuto;
// empty is a file).
type JoinTicket struct {
	Token    string
	URLs     []string
	KeyStore string
}

// JoinUnit is the service of norca-join that RenderJoin writes: the
// systemd unit norca-join.service and the OpenRC service
// /etc/init.d/norca-join.
const JoinUnit = "norca-join"

// The files of the service of norca-join: the bootstrap writes both, and
// the line of runcmd enables the one of the init system that runs. A
// systemd machine removes the OpenRC service (see enableLine).
const (
	JoinUnitFile   = "/etc/systemd/system/" + JoinUnit + ".service"
	JoinOpenRCFile = "/etc/init.d/" + JoinUnit
)

// validPath is an absolute path that goes into the unit and the OpenRC
// service (a shell script) of norca-join without quoting: no spaces, no
// quotes, nothing the shell expands.
var validPath = regexp.MustCompile(`^/[A-Za-z0-9_./+@-]+$`)

// RenderJoin renders the bootstrap of a node that joins with a key of its
// own (package join): the cluster CA, the agent configuration, the
// configuration of norca-join with the one-time token, and its service,
// as a systemd unit and as an OpenRC service. No key and no certificate
// travel: norca-join makes the key on the machine, gets the certificate
// from the join endpoint, starts the agent (Service) and renews the
// certificate. The node carries the meta norca.identity = "join" and no
// expiry of a certificate: it is not replaced for its age.
func (t *Template) RenderJoin(n Node, ticket JoinTicket) (*Rendered, error) {
	if !join.ValidInstance(n.Name) {
		return nil, fmt.Errorf("the node name %q is not a host name", n.Name)
	}
	if ticket.Token == "" || len(ticket.URLs) == 0 {
		return nil, errors.New("a join needs a token and an endpoint")
	}
	if !validPath.MatchString(t.JoinBinary) {
		return nil, fmt.Errorf("the join binary %q is not an absolute path of letters, digits and ./_+@-", t.JoinBinary)
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
	if !validPath.MatchString(joinFile) {
		return nil, fmt.Errorf("the configuration of norca-join %q is not an absolute path of letters, digits and ./_+@-", joinFile)
	}
	conf := join.Config{
		Instance: n.Name, Region: t.Region,
		URLs: ticket.URLs, Endpoint: join.EndpointURI(t.TrustDomain, t.Region).String(), Token: ticket.Token,
		CAFile: n.CAFile, CertFile: n.CertFile, KeyFile: n.KeyFile,
		StateFile: t.TLSDir + "/norca-join.state", Service: t.Service, KeyStore: ticket.KeyStore,
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
	writeFile(&doc, JoinUnitFile, "0644", []byte(unit))
	writeFile(&doc, JoinOpenRCFile, "0755", []byte(joinOpenRC(t.JoinBinary, joinFile)))
	t.writeInstallScript(&doc)
	doc.WriteString("runcmd:\n")
	t.runInstallScript(&doc)
	writeRun(&doc, enableLine(JoinUnit, JoinOpenRCFile))
	return &Rendered{UserData: doc.String()}, nil
}

// joinOpenRC is the OpenRC service of norca-join, the twin of its systemd
// unit: norca-join run keeps running (it renews the certificate), so
// supervise-daemon runs it and starts it again 10 seconds after it ended
// (Restart=always, RestartSec=10), its output goes to syslog (the journal
// of the unit), and it starts once the network is up. The agent is a
// service of its own, which norca-join starts: stopping norca-join leaves
// it running.
func joinOpenRC(binary, config string) string {
	return fmt.Sprintf(`#!/sbin/openrc-run
# Written by the bootstrap of the node scaling operator.
name="norca-join"
description="Norca node identity (norca-join): the key and the certificate of the node"

supervisor=supervise-daemon
command="%s"
command_args="run -config %s"
output_logger="logger -t norca-join -p daemon.info"
error_logger="logger -t norca-join -p daemon.err"
respawn_delay=10
respawn_max=0

depend() {
	need net
	use dns logger
}
`, binary, config)
}
