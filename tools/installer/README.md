# Ozone Installer (Ansible)

## Software Requirements

- Controller: Python 3.10–3.12 (prefer 3.11)
- Ansible Community 9.x (ansible-core 2.16.x)
- rsync installed on controller and all target hosts (required for local snapshot tarball copy)
  - Debian/Ubuntu: `sudo apt-get install -y rsync`
  - RHEL/CentOS/Rocky: `sudo yum install -y rsync` or `sudo dnf install -y rsync`
  - SUSE: `sudo zypper in -y rsync`

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
- `inventories/dev/hosts.ini` + `inventories/dev/group_vars/all.yml`
- `playbooks/` (`cluster.yml`)
- `roles/` (ssh_bootstrap, ozone_user, java, ozone_layout, ozone_fetch, ozone_config, ozone_service_non_ha, ozone_service_ha)

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

Edit `inventories/dev/hosts.ini` and group vars in `inventories/dev/group_vars/all.yml`:

- Groups: `[om]`, `[scm]`, `[datanodes]`, `[recon]`
- Key vars: `ozone_version`, `install_base`, `data_base`, `jdk_major`, `service_user`, `start_after_install`

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
- JAVA_HOME and OZONE_HOME are exported in `/etc/profile.d/ozone.sh`.
- Local snapshot mode archives from controller and uploads to targets.

