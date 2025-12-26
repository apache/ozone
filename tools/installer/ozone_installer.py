#!/usr/bin/env python3

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ANSIBLE_ROOT = Path(__file__).resolve().parent
ANSIBLE_CFG = ANSIBLE_ROOT / "ansible.cfg"
PLAYBOOKS_DIR = ANSIBLE_ROOT / "playbooks"

DEFAULTS = {
    "install_base": "/opt/ozone",
    "data_base": "/data/ozone",
    "ozone_version": "2.0.0",
    "jdk_major": 17,
    "service_user": "ozone",
    "service_group": "ozone",
    "dl_url": "https://dlcdn.apache.org/ozone",
    "JAVA_MARKER": "Apache Ozone Installer Java Home",
    "ENV_MARKER": "Apache Ozone Installer Env",
    "start_after_install": True,
    "use_sudo": True,
}

def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Ozone Ansible Installer (Python trigger) - mirrors bash installer flags"
    )
    p.add_argument("-H", "--host", help="Target host(s). Non-HA: host. HA: comma-separated or brace expansion host{1..n}")
    p.add_argument("-m", "--auth-method", choices=["password", "key"], default=None)
    p.add_argument("-p", "--password", help="SSH password (for --auth-method=password)")
    p.add_argument("-k", "--keyfile", help="SSH private key file (for --auth-method=key)")
    p.add_argument("-v", "--version", help="Ozone version (e.g., 2.0.0) or 'local'")
    p.add_argument("-i", "--install-dir", help=f"Install root (default: {DEFAULTS['install_base']})")
    p.add_argument("-d", "--data-dir", help=f"Data root (default: {DEFAULTS['data_base']})")
    p.add_argument("-s", "--start", action="store_true", help="Initialize and start after install")
    p.add_argument("-M", "--cluster-mode", choices=["non-ha", "ha"], help="Force cluster mode (default: auto by host count)")
    p.add_argument("-r", "--role-file", help="Role file (YAML) for HA mapping (optional)")
    p.add_argument("-j", "--jdk-version", type=int, choices=[17, 21], help="JDK major version (default: 17)")
    p.add_argument("-c", "--config-dir", help="Config dir (optional, templates are used by default)")
    p.add_argument("-x", "--clean", action="store_true", help="(Reserved) Cleanup before install [not yet implemented]")
    p.add_argument("-l", "--ssh-user", help="SSH username (default: root)")
    p.add_argument("-S", "--use-sudo", action="store_true", help="Run remote commands via sudo (default)")
    p.add_argument("-u", "--service-user", help="Service user (default: ozone)")
    p.add_argument("-g", "--service-group", help="Service group (default: ozone)")
    p.add_argument("--deploy", action="store_true", help="Provision hosts via cdep before install")
    # Local extras
    p.add_argument("--local-shared-path", help="Local shared path on target for snapshot")
    p.add_argument("--local-ozone-dirname", help="Local ozone dir name (ozone-*-SNAPSHOT)")
    p.add_argument("--dl-url", help="Upstream download base URL")
    p.add_argument("--yes", action="store_true", help="Non-interactive; accept defaults for missing values")
    return p.parse_args(argv)

def prompt(prompt_text, default=None, secret=False, yes_mode=False):
    if yes_mode:
        return default
    try:
        if default:
            text = f"{prompt_text} [{default}]: "
        else:
            text = f"{prompt_text}: "
        if secret:
            import getpass
            val = getpass.getpass(text)
        else:
            val = input(text)
        if not val and default is not None:
            return default
        return val
    except EOFError:
        return default

def expand_braces(expr):
    # Supports simple pattern like prefix{1..N}suffix
    if not expr or "{" not in expr or ".." not in expr or "}" not in expr:
        return [expr]
    m = re.search(r"(.*)\{(\d+)\.\.(\d+)\}(.*)", expr)
    if not m:
        return [expr]
    pre, a, b, post = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    return [f"{pre}{i}{post}" for i in range(a, b + 1)]

def parse_hosts(hosts_raw):
    """
    Accepts comma-separated hosts; each may contain brace expansion.
    Returns list of dicts: {host, user, port}
    """
    if not hosts_raw:
        return []
    out = []
    for token in hosts_raw.split(","):
        token = token.strip()
        expanded = expand_braces(token)
        for item in expanded:
            user = None
            hostport = item
            if "@" in item:
                user, hostport = item.split("@", 1)
            host = hostport
            port = None
            if ":" in hostport:
                host, port = hostport.split(":", 1)
            out.append({"host": host, "user": user, "port": port})
    return out

def auto_cluster_mode(hosts, forced=None):
    if forced in ("non-ha", "ha"):
        return forced
    return "ha" if len(hosts) >= 3 else "non-ha"

def build_inventory(hosts, ssh_user=None, keyfile=None, password=None, cluster_mode="single"):
    """
    Returns INI inventory text for our groups: [om], [scm], [datanodes], [recon]
    """
    if not hosts:
        return ""
    # Non-HA mapping: single OM/SCM on first host; all hosts as datanodes; recon on first
    if cluster_mode == "single":
        h = hosts[0]
        return _render_inv_groups(
            om=[h], scm=[h], dn=hosts, recon=[h],
            ssh_user=ssh_user, keyfile=keyfile, password=password
        )
    # HA: first 3 go to OM and SCM; all to datanodes; recon is first if present
    om = hosts[:3] if len(hosts) >= 3 else hosts
    scm = hosts[:3] if len(hosts) >= 3 else hosts
    dn = hosts
    recon = [hosts[0]]
    return _render_inv_groups(om=om, scm=scm, dn=dn, recon=recon,
                              ssh_user=ssh_user, keyfile=keyfile, password=password)

def _render_inv_groups(om, scm, dn, recon, ssh_user=None, keyfile=None, password=None):
    def hostline(hd):
        parts = [hd["host"]]
        if ssh_user or hd.get("user"):
            parts.append(f"ansible_user={(ssh_user or hd.get('user'))}")
        if hd.get("port"):
            parts.append(f"ansible_port={hd['port']}")
        if keyfile:
            parts.append(f"ansible_ssh_private_key_file={shlex.quote(str(keyfile))}")
        if password:
            parts.append(f"ansible_password={shlex.quote(password)}")
        return " ".join(parts)

    sections = []
    sections.append("[om]")
    sections += [hostline(h) for h in om]
    sections.append("\n[scm]")
    sections += [hostline(h) for h in scm]
    sections.append("\n[datanodes]")
    sections += [hostline(h) for h in dn]
    sections.append("\n[recon]")
    sections += [hostline(h) for h in recon]
    sections.append("\n")
    return "\n".join(sections)

def run_playbook(playbook, inventory_path, extra_vars_path, ask_pass=False, become=True):
    cmd = [
        "ansible-playbook",
        "-i", str(inventory_path),
        str(playbook),
        "-e", f"@{extra_vars_path}",
    ]
    if ask_pass:
        cmd.append("-k")
    if become:
        cmd.append("--become")
    env = os.environ.copy()
    env["ANSIBLE_CONFIG"] = str(ANSIBLE_CFG)
    print(f"Running: {' '.join(shlex.quote(c) for c in cmd)}")
    return subprocess.call(cmd, env=env)

def main(argv):
    args = parse_args(argv)
    yes = bool(args.yes)

    # Gather inputs interactively where missing
    hosts_raw = args.host or prompt("Target host(s) [non-ha: host | HA: h1,h2,h3 or brace expansion]", default="", yes_mode=yes)
    hosts = parse_hosts(hosts_raw) if hosts_raw else []
    if not hosts:
        print("Error: No hosts provided (-H/--host).", file=sys.stderr)
        return 2
    # Decide HA vs Non-HA with user input; default depends on host count
    if args.cluster_mode:
        cluster_mode = args.cluster_mode
    else:
        default_mode = "ha" if len(hosts) >= 3 else "non-ha"
        selected = prompt("Deployment type (ha|non-ha)", default=default_mode, yes_mode=yes)
        cluster_mode = (selected or default_mode).strip().lower()
        if cluster_mode not in ("ha", "non-ha"):
            cluster_mode = default_mode
    if cluster_mode == "ha" and len(hosts) < 3:
        print("Error: HA requires at least 3 hosts (to map 3 OMs and 3 SCMs).", file=sys.stderr)
        return 2

    ozone_version = args.version or prompt("Ozone version (e.g., 2.0.0 | local)", default=DEFAULTS["ozone_version"], yes_mode=yes)
    jdk_major = args.jdk_version or int(prompt("JDK major (17|21)", default=str(DEFAULTS["jdk_major"]), yes_mode=yes))
    install_base = args.install_dir or prompt("Install base directory", default=DEFAULTS["install_base"], yes_mode=yes)
    data_base = args.data_dir or prompt("Data base directory", default=DEFAULTS["data_base"], yes_mode=yes)

    # Auth (before service user/group)
    auth_method = args.auth_method or prompt("Auth method (key|password)", default="password", yes_mode=yes)
    if auth_method not in ("key", "password"):
        auth_method = "password"
    ssh_user = args.ssh_user or prompt("SSH username", default="root", yes_mode=yes)
    password = args.password
    keyfile = args.keyfile
    if auth_method == "password" and not password:
        password = prompt("SSH password", default="", secret=True, yes_mode=yes)
    if auth_method == "key" and not keyfile:
        keyfile = prompt("Path to SSH private key", default=str(Path.home() / ".ssh" / "id_ed25519"), yes_mode=yes)
    # Ensure we don't mix methods
    if auth_method == "password":
        keyfile = None
    elif auth_method == "key":
        password = None
    service_user = args.service_user or prompt("Service user", default=DEFAULTS["service_user"], yes_mode=yes)
    service_group = args.service_group or prompt("Service group", default=DEFAULTS["service_group"], yes_mode=yes)
    dl_url = args.dl_url or DEFAULTS["dl_url"]
    start_after_install = args.start or DEFAULTS["start_after_install"]
    use_sudo = args.use_sudo or DEFAULTS["use_sudo"]

    # Local specifics
    local_shared_path = args.local_shared_path
    local_oz_dir = args.local_ozone_dirname
    if ozone_version and ozone_version.lower() == "local":
        local_shared_path = local_shared_path or prompt("Local shared path on controller (contains ozone-*/)", default="", yes_mode=yes)
        local_oz_dir = local_oz_dir or prompt("Local ozone dir (e.g., ozone-2.1.0-SNAPSHOT)", default="", yes_mode=yes)

    # Optional provisioning
    do_deploy = bool(args.deploy)
    hosts_base_for_deploy = hosts_raw or ""

    # Prepare dynamic inventory and extra-vars
    inventory_text = build_inventory(hosts, ssh_user=ssh_user, keyfile=keyfile, password=password,
                                     cluster_mode=cluster_mode)
    extra_vars = {
        "cluster_mode": cluster_mode,
        "install_base": install_base,
        "data_base": data_base,
        "jdk_major": jdk_major,
        "service_user": service_user,
        "service_group": service_group,
        "dl_url": dl_url,
        "ozone_version": ozone_version,
        "start_after_install": bool(start_after_install),
        "use_sudo": bool(use_sudo),
        "JAVA_MARKER": DEFAULTS["JAVA_MARKER"],
        "ENV_MARKER": DEFAULTS["ENV_MARKER"],
    }
    if ozone_version and ozone_version.lower() == "local":
        extra_vars.update({
            "local_shared_path": local_shared_path or "",
            "local_ozone_dirname": local_oz_dir or "",
        })

    ask_pass = (auth_method == "password" and not password)  # whether to forward -k; we embed password if provided

    with tempfile.TemporaryDirectory() as tdir:
        inv_path = Path(tdir) / "hosts.ini"
        ev_path = Path(tdir) / "vars.json"
        inv_path.write_text(inventory_text or "", encoding="utf-8")
        ev_path.write_text(json.dumps(extra_vars, indent=2), encoding="utf-8")
        # Roles order removed (no resume via tags)

        # Optional provisioning run
        if do_deploy:
            prov_vars = {
                "hosts_base": hosts_base_for_deploy,
            }
            prov_path = Path(tdir) / "prov.json"
            prov_path.write_text(json.dumps(prov_vars, indent=2), encoding="utf-8")
            rc = run_playbook(PLAYBOOKS_DIR / "provision.yml", inv_path, prov_path, ask_pass=False, become=False)
            if rc != 0:
                print("Provisioning failed.", file=sys.stderr)
                return rc
        # Cleanup before install:
        # - If --clean is provided, always run cleanup
        # - Otherwise, ask for confirmation (defaults to no) and run if accepted
        do_cleanup = False
        if args.clean:
            do_cleanup = True
        else:
            answer = prompt(f"Cleanup existing install at {install_base} (if present)? (y/N)", default="n", yes_mode=yes)
            if str(answer).strip().lower().startswith("y"):
                do_cleanup = True
        if do_cleanup:
            rc = run_playbook(PLAYBOOKS_DIR / "cleanup.yml", inv_path, ev_path, ask_pass=ask_pass, become=True)
            if rc != 0:
                print("Cleanup failed.", file=sys.stderr)
                return rc

        # Install + (optional) start
        playbook = PLAYBOOKS_DIR / ("ha-cluster.yml" if cluster_mode == "ha" else "non-ha-cluster.yml")
        rc = run_playbook(playbook, inv_path, ev_path, ask_pass=ask_pass, become=True)
        if rc != 0:
            return rc

        # Smoke test (only if started)
        if extra_vars.get("start_after_install"):
            rc = run_playbook(PLAYBOOKS_DIR / "smoke.yml", inv_path, ev_path, ask_pass=ask_pass, become=True)
            if rc != 0:
                return rc

    print("All done.")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


