# Ozone Installer (Ansible)

## Software Requirements

- Controller: Python 3.10–3.12 (prefer 3.11)
- Ansible Community 9.x (ansible-core 2.16.x)

### Requirements for your controller node
- Could be local or remote.
- Needs to be on the same network as the target hosts.
- Must be able to SSH, either key or password.

### Run on the controller node
```bash
pip install -r requirements.txt
```

## File structure

- `ansible.cfg` (defaults and logging)
- `inventories/dev/hosts.ini` + `inventories/dev/group_vars/all.yml`
- `playbooks/` (`non-ha-cluster.yml`, `ha-cluster.yml`, `provision.yml`, `smoke.yml`)
- `roles/` (ssh_bootstrap, ozone_user, java, ozone_layout, ozone_fetch, ozone_config, ozone_service_non_ha, ozone_service_ha)

## Usage (two options)

1) Python wrapper (orchestrates Ansible for you)

```bash
# Non-HA upstream
python3 ozone_installer.py -H host1 -v 2.0.0

# HA upstream (3+ hosts) - mode auto-detected or force with -M ha
python3 ozone_installer.py -H "host1,host2,host3" -v 2.0.0

# Local snapshot
python3 ozone_installer.py -H host1 -v local \
  --local-shared-path /path/to/share \
  --local-ozone-dirname ozone-2.1.0-SNAPSHOT

# Cleanup and reinstall
python3 ozone_installer.py --clean -H "host1,host2,host3" -v 2.0.0

# Notes on cleanup
# - During a normal install, you'll be asked whether to cleanup an existing install (if present). Default is No.
# - Use --clean to cleanup without prompting before reinstall.
```

2) Direct Ansible (run playbooks yourself)

```bash
# Non-HA upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/non-ha-cluster.yml -e "ozone_version=2.0.0"

# HA upstream
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/ha-cluster.yml -e "ozone_version=2.0.0"

# Local snapshot
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/non-ha-cluster.yml \
  -e "ozone_version=local local_shared_path=/path/to/share local_ozone_dirname=ozone-2.1.0-SNAPSHOT"

# Cleanup only
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/cleanup.yml
```

## Inventory

Edit `inventories/dev/hosts.ini` and group vars in `inventories/dev/group_vars/all.yml`:

- Groups: `[om]`, `[scm]`, `[datanodes]`, `[recon]`
- Key vars: `ozone_version`, `install_base`, `data_base`, `jdk_major`, `service_user`, `start_after_install`

## Non-HA

```bash
ANSIBLE_CONFIG=ansible.cfg ansible-playbook -i inventories/dev/hosts.ini playbooks/non-ha-cluster.yml
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


