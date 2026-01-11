#!/usr/bin/env python3

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

# Optional nicer interactive prompts (fallback to built-in prompts if unavailable)
try:
    import click  # type: ignore
except Exception:
    click = None  # type: ignore

ANSIBLE_ROOT = Path(__file__).resolve().parent
ANSIBLE_CFG = ANSIBLE_ROOT / "ansible.cfg"
PLAYBOOKS_DIR = ANSIBLE_ROOT / "playbooks"
LOGS_DIR = ANSIBLE_ROOT / "logs"
LAST_FAILED_FILE = LOGS_DIR / "last_failed_task.txt"
LAST_RUN_FILE = LOGS_DIR / "last_run.json"

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

def get_logger(log_path: Optional[Path] = None) -> logging.Logger:
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    logger = logging.getLogger("ozone_installer")
    logger.setLevel(logging.INFO)
    # Avoid duplicate handlers if re-invoked
    if not logger.handlers:
        dest = log_path or (LOGS_DIR / "ansible.log")
        fh = logging.FileHandler(dest)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        logger.addHandler(sh)
    return logger

def parse_args(argv: List[str]) -> argparse.Namespace:
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
    # Local extras
    p.add_argument("--local-path", help="Path to local Ozone build (contains bin/ozone)")
    p.add_argument("--dl-url", help="Upstream download base URL")
    p.add_argument("--yes", action="store_true", help="Non-interactive; accept defaults for missing values")
    p.add_argument("-R", "--resume", action="store_true", help="Resume play at last failed task (if available)")
    return p.parse_args(argv)

def _validate_local_ozone_dir(path: Path) -> bool:
    """
    Returns True if 'path/bin/ozone' exists and is executable.
    """
    ozone_bin = path / "bin" / "ozone"
    try:
        return ozone_bin.exists() and os.access(str(ozone_bin), os.X_OK)
    except OSError:
        return False

def prompt(prompt_text: str, default: Optional[str] = None, secret: bool = False, yes_mode: bool = False) -> Optional[str]:
    if yes_mode:
        return default
    if click is not None and sys.stdout.isatty():
        try:
            display = prompt_text
            # logger.info(f"prompt_text: {prompt_text} , default: {default}")
            if default:
                display = f"{prompt_text} [default={default}]"
            if secret:
                return click.prompt(display, default=default, hide_input=True, show_default=False)
            return click.prompt(display, default=default, show_default=False)
        except (EOFError, KeyboardInterrupt):
            return default
    # Fallback to built-in input/getpass
    try:
        text = f"{prompt_text}: "
        if default:
            text = f"{prompt_text} [default={default}]: "
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

def _semver_key(v: str) -> Tuple[int, int, int, str]:
    """
    Convert version like '2.0.0' or '2.1.0-RC0' to a sortable key.
    Pre-release suffix sorts before final.
    """
    try:
        core, *rest = v.split("-", 1)
        major, minor, patch = core.split(".")
        suffix = rest[0] if rest else ""
        return (int(major), int(minor), int(patch), suffix)
    except Exception:
        return (0, 0, 0, v)

def _render_table(rows: List[Tuple[str, str]]) -> str:
    """
    Returns a simple two-column table string without extra dependencies.
    """
    if not rows:
        return ""
    col1_width = max(len(k) for k, _ in rows)
    col2_width = max(len(str(v)) for _, v in rows)
    sep = f"+-{'-' * col1_width}-+-{'-' * col2_width}-+"
    out = [sep, f"| {'Field'.ljust(col1_width)} | {'Value'.ljust(col2_width)} |", sep]
    for k, v in rows:
        out.append(f"| {k.ljust(col1_width)} | {str(v).ljust(col2_width)} |")
    out.append(sep)
    return "\n".join(out)

def _confirm_summary(rows: List[Tuple[str, str]], yes_mode: bool) -> bool:
    """
    Print the input summary table and ask user to continue. Returns True if confirmed.
    """
    logger = get_logger()
    table = _render_table(rows)
    if click is not None:
        logger.info(table)
        if yes_mode:
            return True
        return click.confirm("Proceed with these settings?", default=True)
    else:
        logger.info(table)
        if yes_mode:
            return True
        answer = prompt("Proceed with these settings? (Y/n)", default="Y", yes_mode=False)
        return str(answer or "Y").strip().lower() in ("y", "yes")

def fetch_available_versions(dl_url: str, limit: int = 30) -> List[str]:
    """
    Fetch available Ozone versions from the download base. Returns newest-first.
    """
    try:
        import urllib.request
        with urllib.request.urlopen(dl_url, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        # Apache directory listing usually has anchors like href="2.0.0/"
        candidates = set(m.group(1) for m in re.finditer(r'href="([0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9]+)?)\/"', html))
        versions = sorted(candidates, key=_semver_key, reverse=True)
        if limit and len(versions) > limit:
            versions = versions[:limit]
        return versions
    except Exception:
        return []

def choose_version_interactive(versions: List[str], default_version: str, yes_mode: bool) -> Optional[str]:
    """
    Present a numbered list and prompt user to choose a version.
    Returns selected version string or None if not chosen.
    """
    if not versions:
        return None
    if yes_mode:
        return versions[0]
    # Use click when available and interactive; otherwise fallback to basic prompt
    if click is not None and sys.stdout.isatty():
        click.echo("Available Ozone versions (newest first):")
        for idx, ver in enumerate(versions, start=1):
            click.echo(f"  {idx}) {ver}")
        while True:
            choice = prompt(
                "Select number, type a version (e.g., 2.1.0) or 'local'",
                default="1",
                yes_mode=False,
            )
            if choice is None:
                return versions[0]
            choice = str(choice).strip()
            if choice == "":
                return versions[0]
            if choice.lower() == "local":
                return "local"
            if choice.isdigit():
                i = int(choice)
                if 1 <= i <= len(versions):
                    return versions[i - 1]
            if re.match(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9]+)?$", choice):
                return choice
            click.echo("Invalid selection. Enter a number, a valid version, or 'local'.")
    else:
        logger = get_logger()
        logger.info("Available Ozone versions:")
        for idx, ver in enumerate(versions, start=1):
            logger.info(f"  {idx}) {ver}")
        while True:
            choice = prompt("Select number, type a version (e.g., 2.1.0) or 'local'", default="1", yes_mode=False)
            if choice is None or str(choice).strip() == "":
                return versions[0]
            choice = str(choice).strip()
            if choice.lower() == "local":
                return "local"
            if choice.isdigit():
                i = int(choice)
                if 1 <= i <= len(versions):
                    return versions[i - 1]
            # allow typing a specific version not listed
            if re.match(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9]+)?$", choice):
                return choice
            logger.info("Invalid selection. Please enter a number from the list, a valid version (e.g., 2.1.0) or 'local'.")

def expand_braces(expr: str) -> List[str]:
    # Supports simple pattern like prefix{1..N}suffix
    if not expr or "{" not in expr or ".." not in expr or "}" not in expr:
        return [expr]
    m = re.search(r"(.*)\{(\d+)\.\.(\d+)\}(.*)", expr)
    if not m:
        return [expr]
    pre, a, b, post = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    return [f"{pre}{i}{post}" for i in range(a, b + 1)]

def parse_hosts(hosts_raw: Optional[str]) -> List[dict]:
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

def auto_cluster_mode(hosts: List[dict], forced: Optional[str] = None) -> str:
    if forced in ("non-ha", "ha"):
        return forced
    return "ha" if len(hosts) >= 3 else "non-ha"

def build_inventory(hosts: List[dict], ssh_user: Optional[str] = None, keyfile: Optional[str] = None, password: Optional[str] = None, cluster_mode: str = "non-ha") -> str:
    """
    Returns INI inventory text for our groups: [om], [scm], [datanodes], [recon]
    """
    if not hosts:
        return ""
    # Non-HA mapping: OM/SCM on first host; all hosts as datanodes; recon on first
    if cluster_mode == "non-ha":
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

def _render_inv_groups(om: List[dict], scm: List[dict], dn: List[dict], recon: List[dict], ssh_user: Optional[str] = None, keyfile: Optional[str] = None, password: Optional[str] = None) -> str:
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

def run_playbook(playbook: Path, inventory_path: Path, extra_vars_path: Path, ask_pass: bool = False, become: bool = True, start_at_task: Optional[str] = None, tags: Optional[List[str]] = None) -> int:
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
    if start_at_task:
        cmd += ["--start-at-task", str(start_at_task)]
    if tags:
        cmd += ["--tags", ",".join(tags)]
    env = os.environ.copy()
    env["ANSIBLE_CONFIG"] = str(ANSIBLE_CFG)
    # Route Ansible logs to the same file as the Python logger
    log_path = LOGS_DIR / "ansible.log"
    try:
        logger = get_logger()
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                # type: ignore[attr-defined]
                log_path = Path(getattr(h, "baseFilename"))  # type: ignore
                break
    except Exception:
        pass
    env["ANSIBLE_LOG_PATH"] = str(log_path)
    logger = get_logger()
    if start_at_task:
        logger.info(f"Resuming from task: {start_at_task}")
    if tags:
        logger.info(f"Using tags: {','.join(tags)}")
    logger.info(f"Running: {' '.join(shlex.quote(c) for c in cmd)}")
    return subprocess.call(cmd, env=env)

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    # Resume mode: reuse last provided configs and suppress prompts when possible
    resuming = bool(getattr(args, "resume", False))
    yes = True if resuming else bool(args.yes)
    last_cfg = None
    if resuming and LAST_RUN_FILE.exists():
        try:
            last_cfg = json.loads(LAST_RUN_FILE.read_text(encoding="utf-8"))
        except Exception:
            last_cfg = None

    # Gather inputs interactively where missing
    hosts_raw_default = (last_cfg.get("hosts_raw") if last_cfg else None)
    hosts_raw = args.host or hosts_raw_default or prompt("Target host(s) [non-ha: host | HA: h1,h2,h3 or brace expansion]", default="", yes_mode=yes)
    hosts = parse_hosts(hosts_raw) if hosts_raw else []
    # Initialize per-run logger as soon as we have hosts_raw
    try:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        raw_hosts_for_name = (hosts_raw or "").strip()
        safe_hosts = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw_hosts_for_name)[:80] or "hosts"
        run_log_path = LOGS_DIR / f"ansible-{ts}-{safe_hosts}.log"
        logger = get_logger(run_log_path)
        logger.info(f"Logging to: {run_log_path}")
    except Exception:
        run_log_path = LOGS_DIR / "ansible.log"
        logger = get_logger(run_log_path)
        logger.info(f"Logging to: {run_log_path} (fallback)")

    if not hosts:
        logger.error("Error: No hosts provided (-H/--host).")
        return 2
    # Decide HA vs Non-HA with user input; default depends on host count
    resume_cluster_mode = (last_cfg.get("cluster_mode") if last_cfg else None)
    if args.cluster_mode:
        cluster_mode = args.cluster_mode
    elif resume_cluster_mode:
        cluster_mode = resume_cluster_mode
    else:
        default_mode = "ha" if len(hosts) >= 3 else "non-ha"
        selected = prompt("Deployment type (ha|non-ha)", default=default_mode, yes_mode=yes)
        cluster_mode = (selected or default_mode).strip().lower()
        if cluster_mode not in ("ha", "non-ha"):
            cluster_mode = default_mode
    if cluster_mode == "ha" and len(hosts) < 3:
        logger.error("Error: HA requires at least 3 hosts (to map 3 OMs and 3 SCMs).")
        return 2

    # Resolve download base early for version selection
    dl_url = args.dl_url or (last_cfg.get("dl_url") if last_cfg else None) or DEFAULTS["dl_url"]
    ozone_version = args.version or (last_cfg.get("ozone_version") if last_cfg else None)
    if not ozone_version:
        # Try to fetch available versions from dl_url and offer selection
        versions = fetch_available_versions(dl_url or DEFAULTS["dl_url"])
        selected = choose_version_interactive(versions, DEFAULTS["ozone_version"], yes_mode=yes)
        if selected:
            ozone_version = selected
        else:
            # Fallback prompt if fetch failed
            ozone_version = prompt("Ozone version (e.g., 2.1.0 | local)", default=DEFAULTS["ozone_version"], yes_mode=yes)
    jdk_major = args.jdk_version if args.jdk_version is not None else ((last_cfg.get("jdk_major") if last_cfg else None))
    if jdk_major is None:
        _jdk_val = prompt("JDK major (17|21)", default=str(DEFAULTS["jdk_major"]), yes_mode=yes)
        try:
            jdk_major = int(str(_jdk_val)) if _jdk_val is not None else DEFAULTS["jdk_major"]
        except Exception:
            jdk_major = DEFAULTS["jdk_major"]
    install_base = args.install_dir or (last_cfg.get("install_base") if last_cfg else None) \
        or prompt("Install base directory", default=DEFAULTS["install_base"], yes_mode=yes)
    data_base = args.data_dir or (last_cfg.get("data_base") if last_cfg else None) \
        or prompt("Data base directory", default=DEFAULTS["data_base"], yes_mode=yes)

    # Auth (before service user/group)
    auth_method = args.auth_method or (last_cfg.get("auth_method") if last_cfg else None) \
        or prompt("Auth method (key|password)", default="password", yes_mode=yes)
    if auth_method not in ("key", "password"):
        auth_method = "password"
    ssh_user = args.ssh_user or (last_cfg.get("ssh_user") if last_cfg else None) \
        or prompt("SSH username", default="root", yes_mode=yes)
    password = args.password or ((last_cfg.get("password") if last_cfg else None))  # persisted for resume on request
    keyfile = args.keyfile or (last_cfg.get("keyfile") if last_cfg else None)
    if auth_method == "password" and not password:
        password = prompt("SSH password", default="", secret=True, yes_mode=yes)
    if auth_method == "key" and not keyfile:
        keyfile = prompt("Path to SSH private key", default=str(Path.home() / ".ssh" / "id_ed25519"), yes_mode=yes)
    # Ensure we don't mix methods
    if auth_method == "password":
        keyfile = None
    elif auth_method == "key":
        password = None
    service_user = args.service_user or (last_cfg.get("service_user") if last_cfg else None) \
        or prompt("Service user", default=DEFAULTS["service_user"], yes_mode=yes)
    service_group = args.service_group or (last_cfg.get("service_group") if last_cfg else None) \
        or prompt("Service group", default=DEFAULTS["service_group"], yes_mode=yes)
    dl_url = args.dl_url or (last_cfg.get("dl_url") if last_cfg else None) or DEFAULTS["dl_url"]
    start_after_install = (args.start or (last_cfg.get("start_after_install") if last_cfg else None)
                           or DEFAULTS["start_after_install"])
    use_sudo = (args.use_sudo or (last_cfg.get("use_sudo") if last_cfg else None)
                or DEFAULTS["use_sudo"])

    # Local specifics (single path to local build)
    local_path = (getattr(args, "local_path", None) or (last_cfg.get("local_path") if last_cfg else None))
    local_shared_path = None
    local_oz_dir = None
    if ozone_version and ozone_version.lower() == "local":
        # Accept a direct path to the ozone build dir (relative or absolute) and validate it.
        # Backward-compat: if only legacy split values were saved previously, resolve them.
        candidate = None
        if local_path:
            candidate = Path(local_path).expanduser().resolve()
        else:
            legacy_shared = (last_cfg.get("local_shared_path") if last_cfg else None)
            legacy_dir = (last_cfg.get("local_ozone_dirname") if last_cfg else None)
            if legacy_shared and legacy_dir:
                candidate = Path(legacy_shared).expanduser().resolve() / legacy_dir

        def ask_for_path():
            val = prompt("Path to local Ozone build", default="", yes_mode=yes)
            return Path(val).expanduser().resolve() if val else None

        if candidate is None or not _validate_local_ozone_dir(candidate):
            if yes:
                logger.error("Error: For -v local, a valid Ozone build path containing bin/ozone is required.")
                return 2
            while True:
                maybe = ask_for_path()
                if maybe and _validate_local_ozone_dir(maybe):
                    candidate = maybe
                    break
                logger.warning("Invalid path. Expected an Ozone build directory with bin/ozone. Please try again.")

        # Normalize back to shared path + dirname for Ansible vars and persistable single path
        local_shared_path = str(candidate.parent)
        local_oz_dir = candidate.name
        local_path = str(candidate)

    # Build a human-friendly summary table of inputs before continuing
    host_list_display = str(hosts_raw or "")
    summary_rows: List[Tuple[str, str]] = [
        ("Hosts", host_list_display),
        ("Cluster mode", cluster_mode),
        ("Ozone version", str(ozone_version)),
        ("JDK major", str(jdk_major)),
        ("Install base", str(install_base)),
        ("Data base", str(data_base)),
        ("SSH user", str(ssh_user)),
        ("Auth method", str(auth_method))
    ]
    if keyfile:
        summary_rows.append(("Key file", str(keyfile)))
    summary_rows.extend([("Use sudo", str(bool(use_sudo))),
                        ("Service user", str(service_user)),
                        ("Service group", str(service_group)),
                        ("Start after install", str(bool(start_after_install)))])
    if ozone_version and str(ozone_version).lower() == "local":
        summary_rows.append(("Local Ozone path", str(local_path or "")))
    if not _confirm_summary(summary_rows, yes_mode=yes):
        logger.info("Aborted by user.")
        return 1

    # Prepare dynamic inventory and extra-vars
    inventory_text = build_inventory(hosts, ssh_user=ssh_user, keyfile=keyfile, password=password,
                                     cluster_mode=cluster_mode)
    # Decide cleanup behavior up-front (so we can pass it into the unified play)
    do_cleanup = False
    if args.clean:
        do_cleanup = True
    else:
        answer = prompt(f"Cleanup existing install at {install_base} (if present)? (y/N)", default="n", yes_mode=yes)
        if str(answer).strip().lower().startswith("y"):
            do_cleanup = True

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
        "do_cleanup": bool(do_cleanup),
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
        # Persist last run configs (and use them for execution)
        try:
            os.makedirs(LOGS_DIR, exist_ok=True)
            # Save inventory/vars for direct reuse
            persisted_inv = LOGS_DIR / "last_inventory.ini"
            persisted_ev = LOGS_DIR / "last_vars.json"
            persisted_inv.write_text(inventory_text or "", encoding="utf-8")
            persisted_ev.write_text(json.dumps(extra_vars, indent=2), encoding="utf-8")
            # Point playbook execution to persisted files (consistent first run and reruns)
            inv_path = persisted_inv
            ev_path = persisted_ev
            # Save effective simple config for future resume
            LAST_RUN_FILE.write_text(json.dumps({
                "hosts_raw": hosts_raw,
                "cluster_mode": cluster_mode,
                "ozone_version": ozone_version,
                "jdk_major": jdk_major,
                "install_base": install_base,
                "data_base": data_base,
                "auth_method": auth_method,
                "ssh_user": ssh_user,
                "password": password if auth_method == "password" else None,
                "keyfile": str(keyfile) if keyfile else None,
                "service_user": service_user,
                "service_group": service_group,
                "dl_url": dl_url,
                "start_after_install": bool(start_after_install),
                "use_sudo": bool(use_sudo),
                "local_shared_path": local_shared_path or "",
                "local_ozone_dirname": local_oz_dir or "",
            }, indent=2), encoding="utf-8")
        except Exception:
            # Fall back to temp files if persisting fails
            pass
        # Roles order removed (no resume via tags)

        # Install + (optional) start (single merged playbook)
        playbook = PLAYBOOKS_DIR / "cluster.yml"
        start_at = None
        use_tags = None
        if args.resume:
            if LAST_FAILED_FILE.exists():
                try:
                    # use first line (task name)
                    contents = LAST_FAILED_FILE.read_text(encoding="utf-8").splitlines()
                    start_at = contents[0].strip() if contents else None
                    # derive role tag if present
                    role_line = next((l for l in contents if l.startswith("# role:")), None)
                    if role_line:
                        role_name = role_line.split(":", 1)[1].strip()
                        if role_name:
                            use_tags = [role_name]
                except Exception:
                    start_at = None
        rc = run_playbook(playbook, inv_path, ev_path, ask_pass=ask_pass, become=True, start_at_task=start_at, tags=use_tags)
        if rc != 0:
            return rc

        # Successful completion: remove last_* persisted files so a fresh run starts clean
        try:
            for f in LOGS_DIR.glob("last_*"):
                try:
                    f.unlink()
                except FileNotFoundError:
                    pass
                except Exception:
                    # Best-effort cleanup; ignore failures
                    pass
        except Exception:
            pass

        try:
            example_host = hosts[0]["host"] if hosts else "HOSTNAME"
            logger.info(f"To view process logs: ssh to the node and read {install_base}/current/logs/ozone-{service_user}-<process>-<host>.log "
                        f"(e.g., {install_base}/current/logs/ozone-{service_user}-recon-{example_host}.log)")
        except Exception:
            pass
    logger.info("All done.")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


