# Ozone Installer (Ansible)

Ansible-based replacement for the bash installers:

- `utilities/ozone/installer/apache-ozone-installer.sh`
- `utilities/ozone/installer/common_lib.sh`
- `utilities/ozone/installer/deploy_hosts.sh` (optional provisioning bridge)

## Requirements

- Controller: Python 3.10–3.12 (prefer 3.11)
- Ansible Community 9.x (ansible-core 2.16.x)

Install on controller:

```bash
pip install -r requirements.txt
```

## Layout

- `ansible.cfg` (defaults and logging)
- `inventories/dev/hosts.ini` + `inventories/dev/group_vars/all.yml`
- `playbooks/` (`single-node.yml`, `ha-cluster.yml`, `provision.yml`, `smoke.yml`)
- `roles/` (ssh_bootstrap, ozone_user, java, ozone_layout, ozone_fetch, ozone_config, ozone_service_single, ozone_service_ha)

## Usage (two options)

1) Python wrapper (orchestrates Ansible for you)

```bash
# Note: You'll be prompted to choose auth method (default 'key') and SSH username (default 'root').
# Single-node upstream
python3 ozone_installer.py -H host1 -v 2.0.0

# HA upstream (3+ hosts) - mode auto-detected or force with -M ha
python3 ozone_installer.py -H "host1,host2,host3" -v 2.0.0

# Local snapshot
python3 ozone_installer.py -H host1 -v local \
  --local-shared-path /path/to/share \
  --local-ozone-dirname ozone-2.1.0-SNAPSHOT

# Optional provisioning bridge before install
python3 ozone_installer.py --deploy -H "myenv-{1..5}.domain"

# Cleanup and reinstall
python3 ozone_installer.py --clean -H "host1,host2,host3" -v 2.0.0

# Notes on cleanup
# - During a normal install, you'll be asked whether to cleanup an existing install (if present). Default is No.
# - Use --clean to cleanup without prompting before reinstall.
```

2) Direct Ansible (run playbooks yourself)

```bash
# Single-node upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/single-node.yml -e "ozone_version=2.0.0"

# HA upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/ha-cluster.yml -e "ozone_version=2.0.0"

# Local snapshot
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/single-node.yml \
  -e "ozone_version=local local_shared_path=/path/to/share local_ozone_dirname=ozone-2.1.0-SNAPSHOT"

# Cleanup only
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cleanup.yml
```

## Inventory

Edit `inventories/dev/hosts.ini` and group vars in `inventories/dev/group_vars/all.yml`:

- Groups: `[om]`, `[scm]`, `[datanodes]`, `[recon]`
- Key vars: `ozone_version`, `install_base`, `data_base`, `jdk_major`, `service_user`, `start_after_install`

## Single-node

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/single-node.yml
```

## HA cluster

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/ha-cluster.yml
```

## Smoke test

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/smoke.yml
```

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook playbooks/provision.yml -e "hosts_base=myenv-{1..5}.domain"
```

## Notes

- Idempotent where possible; runtime `ozone` init/start guarded with `creates:`.
- JAVA_HOME and OZONE_HOME are exported in `/etc/profile.d/ozone.sh`.
- Local snapshot mode archives from controller and uploads to targets.


