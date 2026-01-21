# Ozone Installer (Ansible)

## On‑prem quickstart (with ozone-installer)

This installer automates the on‑prem steps described in the official Ozone docs, including SCM/OM initialization and starting services (SCM, OM, Datanodes, Recon). See the Ozone on‑prem guide for the conceptual background and properties such as `ozone.metadata.dirs`, `ozone.scm.names`, and `ozone.om.address` [Ozone On Premise Installation](https://ozone.apache.org/docs/edge/start/onprem.html).


What the installer does (mapped to the on‑prem doc):
- Initializes SCM and OM once, in the correct order, then starts them
- Starts Datanodes on all DN hosts
- Starts Recon on the first Recon host
- Renders `ozone-site.xml` with addresses derived from inventory (SCM names, OM address/service IDs, replication factor based on DN count)

Ports and service behavior follow Ozone defaults; consult the official documentation for details [Ozone On Premise Installation](https://ozone.apache.org/docs/edge/start/onprem.html).

## Software Requirements

- Controller: Python 3.10–3.12 (prefer 3.11) and pip
- Ansible Community 10.x (ansible-core 2.17.x)
- Python packages (installed via `requirements.txt`):
  - `ansible-core==2.17.*`
  - `click==8.*` (for nicer interactive prompts; optional but recommended)
- SSH prerequisites on controller:
  - `sshpass` (only if using password auth with `-m password`)
    - Debian/Ubuntu: `sudo apt-get install -y sshpass`
    - RHEL/CentOS/Rocky: `sudo yum install -y sshpass` or `sudo dnf install -y sshpass`
    - SUSE: `sudo zypper in -y sshpass`

### Controller node requirements
- Can be local or remote.
- Must be on the same network as the target hosts.
- Requires SSH access (key or password).

### Run on the controller node
```bash
pip install -r requirements.txt
```

## File structure

- `ansible.cfg` (defaults and logging)
- `playbooks/` (`cluster.yml`)
- `roles/` (ssh_bootstrap, ozone_user, java, ozone_layout, ozone_fetch, ozone_config, ozone_service, ozone_smoke, cleanup, ozone_ui)

## Usage (two options)

1) Python wrapper (orchestrates Ansible for you)

```bash
# Non-HA upstream
python3 ozone_installer.py -H host1.domain -v 2.0.0

# HA upstream (3+ hosts) - mode auto-detected
python3 ozone_installer.py -H "host{1..3}.domain" -v 2.0.0

# Local snapshot build
python3 ozone_installer.py -H host1 -v local --local-path /path/to/share/ozone-2.1.0-SNAPSHOT

# Cleanup and reinstall
python3 ozone_installer.py --clean -H "host{1..3}.domain" -v 2.0.0

# Notes on cleanup
# - During a normal install, you'll be asked whether to cleanup an existing install (if present). Default is No.
# - Use --clean to cleanup without prompting before reinstall.
```

### Interactive prompts and version selection
- The installer uses `click` for interactive prompts when available (TTY).
- Version selection shows a numbered list; you can select by number, type a specific version, or `local`.
- A summary table of inputs is displayed and logged before execution; confirm to proceed.
- Use `--yes` to auto-accept defaults (used implicitly during `--resume`).

### Resume last failed task

```bash
# Python wrapper (picks task name from logs/last_failed_task.txt)
python3 ozone_installer.py -H host1.domain -v 2.0.0 --resume
```

```bash
# Direct Ansible
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml \
  --start-at-task "$(head -n1 logs/last_failed_task.txt)"
```

2) Direct Ansible (run playbooks yourself)

```bash
# Non-HA upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml -e "ozone_version=2.0.0 cluster_mode=non-ha"

# HA upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml -e "ozone_version=2.0.0 cluster_mode=ha"

# Cleanup only (run just the cleanup role)
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml \
  --tags cleanup -e "do_cleanup=true"
```

## Inventory

When using the Python wrapper, inventory is built dynamically from `-H/--host` and persisted for reuse at:
- `logs/last_inventory.ini` (groups: `[om]`, `[scm]`, `[datanodes]`, `[recon]` and optional `[s3g]`)
- `logs/last_vars.json` (effective variables passed to the play)

For direct Ansible runs, you may edit `inventories/dev/hosts.ini` and `inventories/dev/group_vars/all.yml`, or point to `logs/last_inventory.ini` and `logs/last_vars.json` that the wrapper generated.

## Non-HA

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml -e "cluster_mode=non-ha"
```

## HA cluster

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cluster.yml -e "cluster_mode=ha"
```

## Notes

- Idempotent where possible; runtime `ozone` init/start guarded with `creates:`.
- JAVA_HOME and PATH are persisted for resume; runtime settings are exported via `ozone-env.sh`.
- Local snapshot mode archives from the controller and uploads/extracts on targets using `unarchive`.
- Logs are written to a per-run file under `logs/` named:
  - `ansible-<timestamp>-<hosts_raw_sanitized>.log`
  - Ansible and the Python wrapper share the same logfile.
- After a successful run, the wrapper prints where to find process logs on target hosts, e.g. `<install base>/current/logs/ozone-<service-user>-<process>-<host>.log`.

## Components and config mapping

- Components (per the Ozone docs): Ozone Manager (OM), Storage Container Manager (SCM), Datanodes (DN), and Recon. The installer maps:
  - Non‑HA: first host runs OM+SCM+Recon; all hosts are DNs.
  - HA: first three hosts serve as OM and SCM sets; all hosts are DNs; first host is Recon.
- `ozone-site.xml` is rendered from templates based on inventory groups:
  - `ozone.scm.names`, `ozone.scm.client.address`, `ozone.om.address` or HA service IDs
  - `ozone.metadata.dirs`, `hdds.datanode.dir`, and related paths map to `data_base`
  - Replication is set to ONE if DN count < 3, otherwise THREE

## Optional: S3 Gateway (S3G) and smoke

- Define a `[s3g]` group in inventory (commonly the first OM host) to enable S3G properties in `ozone-site.xml` (default HTTP port 9878).
- The smoke role can optionally install `awscli` on the first S3G host, configure dummy credentials, and create/list a test bucket against `http://localhost:9878` (for simple functional verification).


