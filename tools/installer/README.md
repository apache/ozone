# Apache Ozone Installer

A comprehensive automation script for deploying Apache Ozone clusters on Linux hosts. Supports both single-node and high-availability (HA) configurations with minimal manual intervention.

## Overview

The Apache Ozone Installer automates the complete deployment process:

- **Passwordless SSH setup** from installer machine to target hosts
- **Automatic JDK installation** and JAVA_HOME configuration
- **Ozone binary download** and extraction
- **Configuration generation** (ozone-site.xml, core-site.xml, ozone-env.sh, ozone-hosts.yaml)
- **Service user creation** (optional)
- **Cluster initialization** and service startup
- **Cross-node passwordless SSH** for HA clusters
- **Smoke testing** to verify installation

## Features

- **Single-Node and HA Mode**: Automatically detects mode based on number of hosts
- **Brace Expansion Support**: Use `host-{1..10}.domain` to specify multiple hosts
- **Service User Support**: Run Ozone services as a dedicated user (default: `ozone`)
- **Sudo Support**: Option to run remote commands via sudo
- **Parallel Execution**: Installations and configurations run in parallel for multiple hosts
- **Interactive Mode**: Prompts for missing parameters with sensible defaults
- **Config Templates**: Uses pre-configured templates or custom config directories
- **Key Deduplication**: Prevents duplicate SSH keys in authorized_keys
- **System-Wide Environment**: Sets up `/etc/profile.d/ozone.sh` for all users

## Prerequisites

### Default Ports to be opened

- StorageContainerManagerStarter
`9860, 9861, 9863, 9894, 9895, 9876 (UI port)`

- OzoneManagerStarter
`8981, 9862, 9872, 9874 (UI port)`

- HddsDatanodeService
`19864, 9856, 9857, 9858, 9859, 9886, 9882 (UI port)`

- ReconServer
`9891, 9888 (UI port)`

### Local Machine (Installer)
- Bash 4.0+
- SSH client (`ssh`, `scp`)
- `ssh-keygen` (for key generation)
- `sshpass` (auto-installed if using password auth)
- `curl` (for downloading Ozone releases)
- `awk`, `sed`, `grep` (standard Unix tools)

### Remote Hosts (Target Nodes)
- Linux (Ubuntu/Debian/CentOS/RHEL)
- SSH server running
- `curl` or `wget` (for downloads)
- `useradd` or `adduser` (if using service user)
- `java` package manager: `apt-get`, `dnf`, or `yum`


## Installation Options

### Basic Options

| Option | Description | Default |
|--------|-------------|---------|
| `-H, --host` | Target host(s) in format `[user@]host[:port]` | Required |
| `-m, --auth-method` | SSH auth method: `password` or `key` | `password` |
| `-p, --password` | SSH password (required if `--auth-method=password`) | - |
| `-k, --keyfile` | SSH private key file path | `~/.ssh/id_ed25519` |
| `-v, --version` | Ozone version to install (e.g., `2.0.0`) | Interactive |
| `-i, --install-dir` | Installation directory on target | `/opt/ozone` |
| `-d, --data-dir` | Data directory on target | `/data/ozone` |
| `-j, --jdk-version` | JDK major version (`17` or `21`) | `17` |
| `-l, --ssh-user` | SSH username (overrides user@ in host) | `root` |
| `-s, --start` | Initialize and start services after install | No |

### Advanced Options

| Option | Description | Default |
|--------|-------------|---------|
| `-c, --config-dir` | Local config directory with templates | `configs/unsecure/{single\|ha}` |
| `-r, --role-file` | Role file for HA mode (YAML) | `host-map.yml` |
| `-x, --clean` | Clean install/data directories before install | `yes` |
| `-S, --use-sudo` | Run remote commands via sudo | No |
| `-u, --service-user` | Service user to run Ozone commands | `ozone` |
| `-g, --service-group` | Service group for service user | `ozone` |
| `-h, --help` | Show help message | - |

## Usage Examples

### Example 1: Single-Node with Service User

```bash
./apache-ozone-installer.sh -H myhost.example.com -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -u ozone-om -g ozone -S -l ubuntu -s
```

This installs Ozone as user `ozone-om` with group `ozone`, using sudo for privilege escalation.

### Example 2: HA Cluster with Custom Config

```bash
./apache-ozone-installer.sh -H "node1.example.com,node2.example.com,node3.example.com" -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -c ./my-custom-configs/ha -r my-cluster.yaml -u ozone -S -s
```

### Example 3: HA with Brace Expansion

```bash
./apache-ozone-installer.sh -H "cluster-{1..10}.example.com" -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -s
```

Brace expansion automatically expands to `cluster-1.example.com` through `cluster-10.example.com`.


### Example 4: Single-Node with JDK 21

```bash
./apache-ozone-installer.sh -H myhost.example.com -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -j 21 -u ozone -S -s
```

This installs Ozone with JDK 21 instead of the default JDK 17.

### Example 5: Installation Only (No Start)

```bash
./apache-ozone-installer.sh -H myhost.example.com -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -u ozone -S
```

Installs Ozone but doesn't start services. Manual start required:

```bash
ozone scm --init
ozone --daemon start scm
ozone om --init
ozone --daemon start om
ozone --daemon start datanode
```

## Host Format

The `-H, --host` option accepts multiple formats:

- **Single host**: `myhost.example.com`
- **User and host**: `ubuntu@myhost.example.com`
- **Host and port**: `myhost.example.com:2222`
- **Full format**: `ubuntu@myhost.example.com:2222`
- **Multiple hosts**: `host1,host2,host3` or `host-{1..3}.example.com`
- **Brace expansion**: `node-{1..10}.domain` expands to `node-1.domain` through `node-10.domain`

## Role File Format (HA Mode)

For HA deployments, create a role file (YAML) that maps Ozone services to hosts:

```yaml
om:
  - node1.example.com
  - node2.example.com
  - node3.example.com
scm:
  - node1.example.com
  - node2.example.com
  - node3.example.com
datanodes:
  - node1.example.com
  - node2.example.com
  - node3.example.com
  - node4.example.com
recon:
  # Optional
  - node5.example.com
```

You can also use placeholders that map to CLI hosts:

```yaml
om:
  - host1
  - host2
  - host3
scm:
  - host1
  - host2
  - host3
datanodes:
  - host1
  - host2
  - host3
```

When combined with `-H "node1,node2,node3"`, `host1` maps to `node1`, `host2` to `node2`, etc.

## Configuration Directory Structure

The installer expects a config directory with the following files:

```
configs/
├── unsecure/
│   ├── single/
│   │   ├── ozone-site.xml
│   │   ├── core-site.xml
│   │   └── ozone-env.sh
│   └── ha/
│       ├── ozone-site.xml
│       ├── core-site.xml
│       ├── ozone-env.sh
│       └── host-map.yml
```

The installer will:
1. Upload these templates to the target host
2. Replace placeholders (`host1`, `host2`, `DATA_BASE`, etc.) with actual values
3. Place them in `$INSTALL_BASE/current/etc/hadoop/`

## Service User and Sudo

### Service User (`-u, --service-user`)

When specified, the installer:
- Creates the service user (if it doesn't exist)
- Sets up environment variables for that user
- Runs all Ozone commands as that user
- Sets ownership of install/data directories to the service user

Default: `ozone`

### Sudo (`-S, --use-sudo`)

When enabled, the installer:
- Runs remote commands via `sudo`
- Useful when SSH user doesn't have root privileges
- Automatically handles shell syntax in commands

Example:

```bash
./apache-ozone-installer.sh -H myhost -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -u ozone -S -l ubuntu
```

## Environment Variables

The installer sets up environment variables system-wide via `/etc/profile.d/ozone.sh`:

- `JAVA_HOME`: JDK installation path
- `OZONE_HOME`: Ozone installation directory (typically `/opt/ozone/current`)
- `PATH`: Updated to include `$JAVA_HOME/bin` and `$OZONE_HOME/bin`

This ensures `ozone` and `java` commands are available for all users and in all shell contexts.

## What Gets Installed

### Single-Node Mode

- **Services**: SCM, OM, and DataNode on the same host
- **Configs**: Single-node optimized configuration
- **Data**: Single data directory

### HA Mode

- **Services**: Distributed across multiple hosts
  - SCM: 3 nodes (for HA)
  - OM: 3 nodes (for HA)
  - DataNode: All nodes
- **Configs**: HA-optimized with Raft consensus
- **Data**: Distributed across nodes

## Installation Process

1. **Passwordless SSH Setup**: Configures SSH from installer to target hosts
2. **Service User Creation**: Creates service user if specified
3. **JDK Installation**: Installs required JDK version if not present
4. **Directory Preparation**: Creates install and data directories
5. **Ozone Download**: Downloads and extracts Ozone tarball
6. **Environment Setup**: Configures JAVA_HOME and OZONE_HOME
7. **Config Generation**: Generates and uploads configuration files
8. **Cluster SSH Keys** (HA only): Generates and deploys shared SSH keys for cross-node communication
9. **Initialization** (if `-s`): Initializes and starts Ozone services
10. **Smoke Test**: Runs basic functional tests

## Troubleshooting

### SSH Connection Issues

**Problem**: Cannot connect to target host

**Solutions**:
- Verify SSH connectivity: `ssh user@host`
- Check SSH port if non-standard: Use `host:port` format
- Ensure SSH key has correct permissions: `chmod 600 ~/.ssh/id_ed25519`
- Verify password is correct (for password auth)

### Permission Denied Errors

**Problem**: Permission denied when creating directories or running commands

**Solutions**:
- Use `-S, --use-sudo` if SSH user doesn't have root
- Ensure service user has write access to install/data directories
- Check that directories exist and have correct permissions

### Service User Issues

**Problem**: Ozone commands fail with "command not found"

**Solutions**:
- Verify `/etc/profile.d/ozone.sh` exists and is sourced
- Check that `OZONE_HOME/bin` is in PATH
- Ensure service user's shell is `/bin/bash`
- Try: `source /etc/profile.d/ozone.sh && ozone version`

### Cross-Node SSH Not Working (HA)

**Problem**: Nodes cannot SSH to each other

**Solutions**:
- Verify shared SSH keys are installed: Check `~/.ssh/id_ed25519` exists
- Check `~/.ssh/config` has `StrictHostKeyChecking no`
- Verify `authorized_keys` contains the cluster public key
- Check file permissions: `.ssh` should be `700`, keys should be `600`


## Advanced Features

### Environment File

Create a `.env` file in the installer directory to set defaults:

```bash
# .env
DEFAULT_VERSION=2.0.0
JDK_MAJOR=17
DEFAULT_INSTALL_BASE=/opt/ozone
DEFAULT_DATA_BASE=/data/ozone
SERVICE_USER=ozone
SERVICE_GROUP=ozone
USE_SUDO=yes
```

### Custom Configurations

You can provide custom configuration templates:

```bash
./apache-ozone-installer.sh -H myhost -m key -k ~/.ssh/id_ed25519 -v 2.0.0 -c ./my-custom-configs/single -s
```

The config directory should contain:
- `ozone-site.xml`
- `core-site.xml`
- `ozone-env.sh`
- `host-map.yml` (for HA mode)

## File Locations

### Installed Files

- **Binaries**: `$INSTALL_BASE/current/` (symlink to versioned directory)
- **Configs**: `$INSTALL_BASE/current/etc/hadoop/`
- **Data**: `$DATA_BASE/`
- **Environment**: `/etc/profile.d/ozone.sh`


## License

This installer is part of the Apache Ozone project and follows the Apache License 2.0.

