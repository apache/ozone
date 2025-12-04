#!/usr/bin/env bash

set -euo pipefail

# Ozone environment file - system-wide file automatically sourced by all shells
OZONE_ENV_FILE="/etc/profile.d/ozone.sh"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found on local machine." >&2
    exit 1
  fi
}

parse_host() {
  local raw="$1"
  local user_part host_part port_part
  user_part="${raw%@*}"
  if [[ "$raw" == *"@"* ]]; then
    host_part="${raw#*@}"
    TARGET_USER="$user_part"
  else
    host_part="$raw"
    if [[ -n "${SSH_USER:-}" ]]; then
      TARGET_USER="$SSH_USER"
    else
      [[ -z "${TARGET_USER:-}" ]] && TARGET_USER="$(whoami)"
    fi
  fi
  if [[ "$host_part" == *":"* ]]; then
    TARGET_HOST="${host_part%:*}"
    port_part="${host_part##*:}"
    SSH_PORT="$port_part"
  else
    TARGET_HOST="$host_part"
  fi
  # Respect explicit user@host unless --ssh-user was provided explicitly
  if [[ "$raw" != *"@"* ]] && [[ -n "${SSH_USER:-}" ]]; then
    TARGET_USER="$SSH_USER"
  elif [[ "${SSH_USER_SET:-no}" == "yes" ]]; then
    TARGET_USER="$SSH_USER"
  fi
}


expand_brace_patterns() {
  # Expand bash brace patterns like {1..10} in host arguments
  local input="$1"
  local result=""
  
  # Split by comma first to handle comma-separated lists
  IFS=',' read -r -a parts <<<"$input"
  local expanded_parts=()
  
  for part in "${parts[@]}"; do
    # Trim whitespace
    part=$(echo "$part" | xargs)
    
    # Check if part contains brace expansion pattern
    if [[ "$part" =~ \{.*\.\..*\} ]]; then
      local expanded
      expanded=$(eval "echo $part" 2>/dev/null || echo "$part")
      for host in $expanded; do
        expanded_parts+=("$host")
      done
    else
      expanded_parts+=("$part")
    fi
  done
  
  # Join expanded parts back with commas
  local old_ifs="$IFS"
  IFS=','
  echo "${expanded_parts[*]}"
  IFS="$old_ifs"
}

find_config_dirs() {
  local search_base="$1"
  local config_dirs=()
  
  if [[ ! -d "$search_base" ]]; then
    echo ""
    return
  fi
  
  # Normalize search_base (remove trailing slash)
  search_base="${search_base%/}"
  
  # Find all directories containing ozone-site.xml
  while IFS= read -r dir; do
    [[ -z "$dir" ]] && continue
    # Get relative path from search_base
    local rel_path="${dir#$search_base/}"
    # Only add if it's actually a subdirectory (not the search_base itself)
    if [[ "$rel_path" != "$dir" ]]; then
      config_dirs+=("$rel_path")
    fi
  done < <(find "$search_base" -type f -name "ozone-site.xml" -exec dirname {} \; | sort -u)
  
  # Output as newline-separated list
  printf '%s\n' "${config_dirs[@]}"
}

confirm_or_read() {
  local prompt="$1"; shift
  local var_name="$1"; shift
  local default_value="${1:-}"
  local current_value="${!var_name:-}"
  if [[ "${YES_MODE:-no}" == "yes" ]]; then
    if [[ -z "$current_value" && -n "$default_value" ]]; then
      printf -v "$var_name" '%s' "$default_value"
    fi
    return
  fi
  local input
  if [[ -n "$current_value" ]]; then
    read -r -p "$prompt [$current_value]: " input || true
    if [[ -n "$input" ]]; then
      printf -v "$var_name" '%s' "$input"
    fi
  else
    if [[ -n "$default_value" ]]; then
      read -r -p "$prompt [$default_value]: " input || true
      printf -v "$var_name" '%s' "${input:-$default_value}"
    else
      read -r -p "$prompt: " input || true
      printf -v "$var_name" '%s' "$input"
    fi
  fi
}

ensure_local_prereqs() {
  require_cmd ssh
  require_cmd scp
  require_cmd ssh-keygen
  require_cmd ssh-copy-id
  require_cmd awk
  require_cmd sed
  require_cmd grep
  # Need sshpass for password mode and for key mode when a password is supplied for bootstrap
  if [[ "${AUTH_METHOD:-password}" == "password" || -n "${AUTH_PASSWORD:-}" ]]; then
    if ! command -v sshpass >/dev/null 2>&1; then
      install_sshpass_if_needed
    fi
    require_cmd sshpass
  fi
}

install_sshpass_if_needed() {
  echo "sshpass not found; attempting installation..."
  local SUDO=""
  if [[ ${EUID:-1} -ne 0 ]]; then SUDO="sudo"; fi
  if command -v apt-get >/dev/null 2>&1; then
    $SUDO apt-get update -y && $SUDO apt-get install -y sshpass
  elif command -v dnf >/dev/null 2>&1; then
    $SUDO dnf install -y sshpass
  elif command -v yum >/dev/null 2>&1; then
    $SUDO yum install -y sshpass || { $SUDO yum install -y epel-release && $SUDO yum install -y sshpass; }
  else
    echo "Unsupported package manager. Please install 'sshpass' manually. Supported: apt-get, dnf, yum." >&2
    exit 1
  fi
}

fetch_versions_list() {
  if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl is required but not found. Please install curl." >&2
    return 1
  fi
  local html=""
  html=$(curl -fsSL "$DL_URL" 2>/dev/null || true)
  if [[ -n "$html" ]]; then
    {
      printf '%s' "$html" | grep -Eo 'href=\"([0-9]+\.[0-9]+\.[0-9]+)\/\"' | sed -E 's/.*href=\"([0-9]+\.[0-9]+\.[0-9]+)\/\"/\1/'
      printf '%s' "$html" | grep -Eo '>[0-9]+\.[0-9]+\.[0-9]+/' | tr -d '>/ '
    } | sed 's:/$::' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -Vr | uniq
  fi
}

select_version() {
  # Returns a suggested default version for prompting.
  # Behavior:
  # - Try to fetch available versions; print the list to stderr for user reference
  # - Suggest DEFAULT_VERSION if set; otherwise suggest the latest from the list
  # - If fetching fails, suggest DEFAULT_VERSION (may be empty)
  local versions suggestion
  versions="$(fetch_versions_list || true)"
  if [[ -n "$versions" ]]; then
    echo "Available Ozone Upstream Released Versions:" >&2
    nl -ba <<<"$versions" | sed 's/^/  /' >&2
    if [[ -n "${DEFAULT_VERSION:-}" ]]; then
      suggestion="$DEFAULT_VERSION"
    else
      suggestion="$(awk 'NR==1{print;exit}' <<<"$versions")"
    fi
    echo "$suggestion"
  else
    echo "${DEFAULT_VERSION:-}"
  fi
}

choose_upstream_version() {
  # Interactive version chooser that accepts either a version string or a numeric index
  # Echoes the chosen version on stdout.
  local versions suggestion count
  versions="$(fetch_versions_list || true)"
  if [[ -n "$versions" ]]; then
    echo "Available Ozone Upstream Released Versions:" >&2
    nl -ba <<<"$versions" | sed 's/^/  /' >&2
    count="$(echo "$versions" | wc -l | tr -d ' ')"
    if [[ -n "${DEFAULT_VERSION:-}" ]]; then
      suggestion="$DEFAULT_VERSION"
    else
      suggestion="$(awk 'NR==1{print;exit}' <<<"$versions")"
    fi
    local input choice
    read -r -p "Ozone version to install [${suggestion} or 1-${count}]: " input || true
    if [[ -z "$input" ]]; then
      echo "$suggestion"
      return
    fi
    if [[ "$input" =~ ^[0-9]+$ ]] && [[ "$input" -ge 1 ]] && [[ "$input" -le "$count" ]]; then
      choice="$(awk -v n="$input" 'NR==n{print;exit}' <<<"$versions")"
      echo "$choice"
    else
      echo "$input"
    fi
  else
    # Fallback to DEFAULT_VERSION or prompt raw
    local fallback="${DEFAULT_VERSION:-}"
    local input=""
    if [[ -n "$fallback" ]]; then
      read -r -p "Ozone version to install [${fallback}]: " input || true
      echo "${input:-$fallback}"
    else
      read -r -p "Ozone version to install: " input || true
      echo "$input"
    fi
  fi
}

ensure_ozone_env_sourced() {
  # Ensure /etc/bash.bashrc or /etc/bashrc sources /etc/profile.d/ozone.sh
  # This covers cases where bash is invoked without -l flag (non-login shells)
  # Use conditional check: only source if OZONE_HOME is not already set
  ssh_run "set -euo pipefail; \
    OZONE_ENV_FILE=\"$OZONE_ENV_FILE\"; \
    if [ -f /etc/bash.bashrc ]; then \
      if ! grep -qsF \"\$OZONE_ENV_FILE\" /etc/bash.bashrc 2>/dev/null; then \
        printf 'if [ -z \"\$OZONE_HOME\" ] && [ -f %s ]; then\\n  . %s\\nfi\\n' \"\$OZONE_ENV_FILE\" \"\$OZONE_ENV_FILE\" >> /etc/bash.bashrc; \
      fi; \
    elif [ -f /etc/bashrc ]; then \
      if ! grep -qsF \"\$OZONE_ENV_FILE\" /etc/bashrc 2>/dev/null; then \
        printf 'if [ -z \"\$OZONE_HOME\" ] && [ -f %s ]; then\\n  . %s\\nfi\\n' \"\$OZONE_ENV_FILE\" \"\$OZONE_ENV_FILE\" >> /etc/bashrc; \
      fi; \
    fi"
}

remote_get_recon_http_port() {
  # Echoes the Recon HTTP port from ozone-site.xml on the current TARGET_HOST.
  # Args: $1 = install_base
  local install_base="$1"
  ssh_run "set -euo pipefail; \
    f=\"$install_base/current/etc/hadoop/ozone-site.xml\"; \
    if [ -f \"\$f\" ]; then \
      awk 'BEGIN{found=0} \
        /<name>[[:space:]]*ozone.recon.http-address[[:space:]]*<\\/name>/{found=1; next} \
        found && /<value>/{ \
          gsub(/.*>/, \"\"); gsub(/<.*/, \"\"); \
          n=split(\$0,a,\":\"); if (n>1) { print a[n]; } else { print \$0; } \
          exit \
        }' \"\$f\"; \
    fi"
}

ssh_run() {
  local cmd="$*"
  # echo "ssh_run: $cmd"
  # Only prepend sudo if USE_SUDO is set and command doesn't already start with sudo
  if [[ "${USE_SUDO:-no}" == "yes" ]] && [[ "$cmd" != sudo* ]]; then
    # Check if command contains shell syntax that needs to be wrapped in bash -c
    # Shell built-ins like 'set', 'if', 'for', 'while', ';', '&&', '||' indicate shell syntax
    if [[ "$cmd" =~ ^[[:space:]]*(set|if|for|while|case|function|export|declare|local|return|break|continue) ]] || \
       [[ "$cmd" =~ (;|&&|\|\||\||\{|\}|\(|\)) ]]; then
      # Wrap in sudo bash -c with proper quoting
      local cmd_quoted
      cmd_quoted=$(printf '%q' "$cmd")
      cmd="sudo -n bash -c $cmd_quoted"
    else
      cmd="sudo -n $cmd"
    fi
  fi
  local ssh_identity_args=""
  local ssh_batch_args=""
  if [[ "${AUTH_METHOD:-}" == "key" && -n "${AUTH_KEYFILE:-}" ]]; then
    ssh_identity_args="-i ${AUTH_KEYFILE}"
    ssh_batch_args="-o BatchMode=yes"
  fi
  # Use -n so ssh doesn't read from stdin; helps local Ctrl-C work reliably
  ssh -n -p "$SSH_PORT" ${ssh_identity_args} ${ssh_batch_args} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "${TARGET_USER}@${TARGET_HOST}" "$cmd"
}

ssh_run_as_user() {
  local target_user="${SERVICE_USER:-}"
  if [[ -n "$target_user" ]]; then
    local cmd_quoted
    cmd_quoted=$(printf '%q' "$*")
    
    if [[ "${USE_SUDO:-no}" == "yes" ]]; then
      # Run as login shell for target user, via sudo, to ensure environment (PATH, OZONE_HOME) is correct
      ssh_run "sudo -n su - $target_user -c $cmd_quoted"
    else
      # Fallback when sudo is not requested: try sudo if available, else su (may require password)
      ssh_run "if command -v sudo >/dev/null 2>&1; then sudo -n -u $target_user bash -c $cmd_quoted; else su - $target_user -c $cmd_quoted; fi"
    fi
  else
    ssh_run "$*"
  fi
}

scp_put() {
  local -a scp_identity_args=()
  if [[ "${AUTH_METHOD:-}" == "key" && -n "${AUTH_KEYFILE:-}" ]]; then
    scp_identity_args=(-i "${AUTH_KEYFILE}" -o BatchMode=yes)
  fi
  if [[ -n "${AUTH_PASSWORD:-}" ]]; then
    sshpass -p "$AUTH_PASSWORD" scp -q -P "$SSH_PORT" "${scp_identity_args[@]}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$1" "${TARGET_USER}@${TARGET_HOST}:$2"
  else
    scp -q -P "$SSH_PORT" "${scp_identity_args[@]}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$1" "${TARGET_USER}@${TARGET_HOST}:$2"
  fi
}

install_ssh_key() {
  # Unified function to install SSH keys for passwordless SSH
  # Args: $1=public_key_path (required), $2=private_key_path (optional)
  # If private_key is provided, installs both keys for cluster node-to-node communication
  # If only public_key is provided, only adds to authorized_keys for installer-to-host communication
  local pub_key_path="$1"
  local priv_key_path="${2:-}"
  local service_user="${SERVICE_USER}"
  # echo "Installing SSH key: $pub_key_path, with private key: $priv_key_path for user: $TARGET_USER"
  
  if [[ ! -f "$pub_key_path" ]]; then
    echo "Error: public key file $pub_key_path does not exist." >&2
    exit 1
  fi
  
  local pub_key_content
  pub_key_content="$(cat "$pub_key_path")"
  
  if [[ "$AUTH_METHOD" == "password" || -n "${AUTH_PASSWORD:-}" ]]; then
    if [[ -z "${AUTH_PASSWORD:-}" ]]; then
      read -rs -p "Enter SSH password for ${TARGET_USER}@${TARGET_HOST}: " AUTH_PASSWORD
      echo
    fi
    sshpass -p "$AUTH_PASSWORD" ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "${TARGET_USER}@${TARGET_HOST}" "set -euo pipefail; \
      mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; \
      if ! grep -qsF \"$pub_key_content\" ~/.ssh/authorized_keys 2>/dev/null; then \
        echo \"$pub_key_content\" >> ~/.ssh/authorized_keys; \
      fi" >/dev/null 2>&1 || true
    deduplicate_authorized_keys "$TARGET_USER"
  else
    if [[ -n "$priv_key_path" ]]; then
      # Cluster key: install both private and public keys for multiple users
      local tmp_priv="/tmp/.ozone_cluster_key_default"
      cat $priv_key_path > $tmp_priv
      scp_put "$tmp_priv" "$tmp_priv"
      ssh_run "set -euo pipefail; \
        install_for_user() { \
          local target_user=\"\$1\"; \
          local user_home; \
          if [ \"\$target_user\" = \"root\" ] || [ -z \"\$target_user\" ]; then \
            user_home=\"/root\"; \
          else \
            user_home=\$(getent passwd \"\$target_user\" | cut -d: -f6 2>/dev/null || echo \"/home/\$target_user\"); \
          fi; \
          local ssh_dir=\"\$user_home/.ssh\"; \
          mkdir -p \"\$ssh_dir\" && chmod 700 \"\$ssh_dir\"; \
          cp -f $tmp_priv \"\$ssh_dir/id_ed25519\"; \
          if command -v ssh-keygen >/dev/null 2>&1; then \
            ssh-keygen -y -f \"\$ssh_dir/id_ed25519\" > \"\$ssh_dir/id_ed25519.pub\" 2>/dev/null || true; \
          fi; \
          chmod 600 \"\$ssh_dir/id_ed25519\"; \
          chmod 644 \"\$ssh_dir/id_ed25519.pub\"; \
          local cfg=\"\$ssh_dir/config\"; \
          if [ ! -f \"\$cfg\" ] || ! grep -qsF 'StrictHostKeyChecking' \"\$cfg\" 2>/dev/null; then \
            printf 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519\n' >> \"\$cfg\"; \
            chmod 600 \"\$cfg\"; \
          fi; \
          if [ \"\$target_user\" != \"root\" ] && [ -n \"\$target_user\" ]; then \
            chown -R \$target_user:\$target_user \"\$ssh_dir\" 2>/dev/null || true; \
          fi; \
        }; \
        install_for_user \""$TARGET_USER"\"; \
        if [ -n \"$service_user\" ] && [ \"$service_user\" != \""$TARGET_USER"\" ]; then \
          install_for_user \"$service_user\"; \
        fi; \
        rm -f $tmp_priv"
      # In key auth mode, do not modify authorized_keys (assumed already configured)
      # Clean up local temporary key copy
      rm -f "$tmp_priv" 2>/dev/null || true
    else
      # Installer key: only add public key to authorized_keys
      local tmp_pub="/tmp/.ozone_key.pub"
      scp_put "$pub_key_path" "$tmp_pub"
      ssh_run "set -euo pipefail; \
        mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; \
        if ! grep -qsF \"\$(cat $tmp_pub)\" ~/.ssh/authorized_keys 2>/dev/null; then \
          cat $tmp_pub >> ~/.ssh/authorized_keys; \
        fi; \
        rm -f $tmp_pub"
      deduplicate_authorized_keys "$TARGET_USER"
    fi
  fi
}

install_shared_ssh_key() {
  # Install shared SSH keypair for cluster node-to-node communication
  # Args: $1=local_public_key_path, $2=local_private_key_path
  install_ssh_key "$1" "$2"
}

deduplicate_authorized_keys() {
  # Deduplicate entries in authorized_keys for a given user on the remote host
  # Args: $1=target_user (e.g., root, ozone)
  local target_user="$1"
  ssh_run "set -euo pipefail; \
    TU=\"$target_user\"; \
    if [ -z \"\$TU\" ] || [ \"\$TU\" = \"root\" ]; then \
      USER_HOME=\"/root\"; \
    else \
      USER_HOME=\$(getent passwd \"\$TU\" | cut -d: -f6 2>/dev/null || echo \"/home/\$TU\"); \
    fi; \
    AUTH_FILE=\"\$USER_HOME/.ssh/authorized_keys\"; \
    if [ -f \"\$AUTH_FILE\" ]; then \
      TMP=\$(mktemp); \
      awk '!seen[\$0]++' \"\$AUTH_FILE\" > \"\$TMP\" && mv \"\$TMP\" \"\$AUTH_FILE\"; \
      chmod 600 \"\$AUTH_FILE\" 2>/dev/null || true; \
      if [ \"\$TU\" != \"root\" ] && [ -n \"\$TU\" ]; then \
        chown \"\$TU\":\"\$TU\" \"\$AUTH_FILE\" 2>/dev/null || true; \
      fi; \
    fi"
}

ensure_passwordless_ssh() {
  local ssh_dir="$HOME/.ssh"
  local installer_priv="${AUTH_KEYFILE:-$ssh_dir/ozone_installer_key_default}"
  local installer_pub="${installer_priv}.pub"
  local new_key_created="no"
  mkdir -p "$ssh_dir"
  
  chmod 700 "$ssh_dir" 2>/dev/null || true
  if [[ "${AUTH_METHOD:-}" == "key" && -n "${AUTH_KEYFILE:-}" ]]; then
    chmod 600 "$installer_priv" 2>/dev/null || true
    if [[ ! -f "$installer_pub" ]]; then
      ssh-keygen -y -f "$installer_priv" > "$installer_pub"
      chmod 644 "$installer_pub" 2>/dev/null || true
    fi
  else
    if [[ ! -f "$installer_priv" ]]; then
      echo "Generating installer SSH key at $installer_priv (ed25519)..."
      ssh-keygen -t ed25519 -N "" -f "$installer_priv" >/dev/null
      new_key_created="yes"
    fi
    if [[ ! -f "$installer_pub" ]]; then
      ssh-keygen -y -f "$installer_priv" > "$installer_pub"
      chmod 644 "$installer_pub" 2>/dev/null || true
    fi
  fi
  # If we generated a new private key, add its pub key to local authorized_keys (per request)
  if [[ "$new_key_created" == "yes" ]]; then
    local auth_local="$ssh_dir/authorized_keys"
    touch "$auth_local"
    chmod 600 "$auth_local" 2>/dev/null || true
    if ! grep -qsF "$(cat "$installer_pub")" "$auth_local" 2>/dev/null; then
      cat "$installer_pub" >> "$auth_local"
    fi
  fi
  # Ensure ssh config uses the installer key for this host, user and port
  local ssh_cfg="$ssh_dir/config"
  touch "$ssh_cfg"
  chmod 600 "$ssh_cfg" 2>/dev/null || true
  # Prefer a single wildcard entry for the domain (e.g., *.vpc.cloudera.com)
  local domain="" host_entry=""
  if [[ "$TARGET_HOST" == *.* ]]; then
    domain="${TARGET_HOST#*.}"
  fi
  if [[ -n "$domain" ]]; then
    host_entry="*.$domain"
  else
    host_entry="${TARGET_HOST}"
  fi
  # If the Host line already exists, do not add again
  if ! grep -qsF "Host ${host_entry}" "$ssh_cfg" 2>/dev/null; then
    {
      echo "Host ${host_entry}"
      echo "  User ${TARGET_USER}"
      echo "  Port ${SSH_PORT}"
      echo "  IdentityFile ${installer_priv}"
      echo "  StrictHostKeyChecking no"
      echo "  UserKnownHostsFile /dev/null"
    } >> "$ssh_cfg"
  fi
  # Use the installer key for subsequent ssh/scp calls when key mode is enabled
  if [[ "${AUTH_METHOD:-}" == "key" ]]; then
    AUTH_KEYFILE="$installer_priv"
  fi
  echo "Setting up passwordless SSH to ${TARGET_USER}@${TARGET_HOST}:${SSH_PORT}..."
  install_ssh_key "$installer_pub" "$installer_priv"
  echo "Passwordless SSH is configured. Testing..."
  ssh_run "echo OK" >/dev/null
}

ensure_passwordless_ssh_to_host() {
  local host="$1"
  local prev_host="$TARGET_HOST" prev_port="$SSH_PORT"
  parse_host "$host"
  # Run quietly to avoid interleaved output when executed in parallel
  if ensure_passwordless_ssh >/dev/null 2>&1; then
    printf "[%s@%s:%s] Passwordless SSH OK\n" "${TARGET_USER}" "${TARGET_HOST}" "${SSH_PORT}"
  else
    printf "[%s@%s:%s] Passwordless SSH FAILED\n" "${TARGET_USER}" "${TARGET_HOST}" "${SSH_PORT}" >&2
  fi
  TARGET_HOST="$prev_host"; SSH_PORT="$prev_port"
}

remote_cleanup_dirs() {
  local install_base="$1" data_base="$2"
  ssh_run "set -euo pipefail; \
    SUDO_CMD=''; if command -v sudo >/dev/null 2>&1; then SUDO_CMD='sudo -n'; fi; \
    echo 'Cleaning install base: $install_base and data base: $data_base...' >&2; \
    \$SUDO_CMD rm -rf $install_base $data_base; \
    \$SUDO_CMD mkdir -p $install_base $data_base; \
    echo 'Stopping Ozone processes (if any)...' >&2; \
    for pat in 'Hdds[D]atanodeService' 'Ozone[M]anagerStarter' 'StorageContainerManager[S]tarter' '[R]econServer'; do \
      \$SUDO_CMD pkill -9 -f \"\$pat\" || true; \
    done; "
}

remote_upload_xmls() {
  local config_dir="$1" etc_dir="$2" host_sub="$3" data_base_sub="$4"
  local tmp_oz="$RANDOM.ozone-site.xml" tmp_core="$RANDOM.core-site.xml" tmp_env="$RANDOM.ozone-env.sh"
  local service_user="${SERVICE_USER}"
  local service_group="${SERVICE_GROUP}"
  local sed_hosts=""
  if [[ "$host_sub" == *","* ]]; then
    IFS=',' read -r -a _hosts <<<"$host_sub"
    local i=1
    for h in "${_hosts[@]}"; do
      sed_hosts+=" -e \"s|host${i}|${h}|g\""
      i=$((i+1))
    done
  else
    sed_hosts=" -e \"s|host1|${host_sub}|g\""
  fi
  if [[ -f "$config_dir/ozone-site.xml" ]]; then
    (
      scp_put "$config_dir/ozone-site.xml" "/tmp/$tmp_oz"
      ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f /tmp/$tmp_oz \"$etc_dir/ozone-site.xml\" && \
        sed -i${sed_hosts} -e \"s|DATA_BASE|${data_base_sub}|g\" \"$etc_dir/ozone-site.xml\" && \
        chown $service_user:$service_group \"$etc_dir/ozone-site.xml\" 2>/dev/null || true; "
    ) &
  fi
  if [[ -f "$config_dir/core-site.xml" ]]; then
    (
      scp_put "$config_dir/core-site.xml" "/tmp/$tmp_core"
      ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f /tmp/$tmp_core \"$etc_dir/core-site.xml\" && \
        sed -i${sed_hosts} \"$etc_dir/core-site.xml\" && \
        chown $service_user:$service_group \"$etc_dir/core-site.xml\" 2>/dev/null || true; "
    ) &
  fi
  if [[ -f "$config_dir/ozone-env.sh" ]]; then
    (
      scp_put "$config_dir/ozone-env.sh" "/tmp/$tmp_env"
      ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f /tmp/$tmp_env \"$etc_dir/ozone-env.sh\" && \
        sed -i${sed_hosts} \"$etc_dir/ozone-env.sh\" && \
        chown $service_user:$service_group \"$etc_dir/ozone-env.sh\" 2>/dev/null || true; "
    ) &
  fi
  wait
}

join_by() { local IFS="$1"; shift; echo "$*"; }

unique_hosts() {
  awk '!seen[$0]++' <(printf "%s\n" "$@")
}

stage_internal_or_local() {
  # Unified staging of Ozone binaries on target for "internal" and "local snapshot" modes
  # Usage:
  #   stage_internal_or_local internal <gbn> <os_flavor> <install_base>
  #   stage_internal_or_local local    <shared_path> <dir_name> <install_base>
  local mode="$1"; shift
  case "$mode" in
    local)
      local shared_path="$1" dir_name="$2" install_base="$3"
      local source_dir="$shared_path/$dir_name"
      local service_user="${SERVICE_USER}"
      local service_group="${SERVICE_GROUP}"
      if [[ ! -d "$source_dir" ]]; then
        echo "ERROR: Local Ozone snapshot directory does not exist: $source_dir" >&2
        exit 1
      fi
      local _tmp_dir _tar_file
      _tmp_dir="$(mktemp -d)"
      _tar_file="$_tmp_dir/${dir_name}.tar.gz"
      echo "Creating tarball from local snapshot: $source_dir -> $_tar_file"
      tar -C "$shared_path" -czf "$_tar_file" "$dir_name"
      echo "Uploading tarball to target: ${TARGET_USER}@${TARGET_HOST}:$install_base/${dir_name}.tar.gz"
      scp_put "$_tmp_dir/${dir_name}.tar.gz" "$install_base/${dir_name}.tar.gz"
      rm -rf "$_tmp_dir"
      ssh_run "set -euo pipefail; \
        mkdir -p \"$install_base\"; \
        cd \"$install_base\"; \
        echo 'Extracting snapshot tarball ...' >&2; \
        tar -xzf ${dir_name}.tar.gz -C \"$install_base\"; \
        rm -f current; ln -s \"$install_base/$dir_name\" current; \
        chown -R $service_user:$service_group \"$install_base\" 2>/dev/null || true; \
        rm -f ${dir_name}.tar.gz; \
        echo \"Linked <<$install_base/current>> to <<$install_base/$dir_name>>\" >&2"
      ;;
    *)
      echo "stage_internal_or_local: unknown mode '$mode' (expected 'internal' or 'local')" >&2
      return 1
      ;;
  esac
}

get_internal_os_options() {
  # Args: $1=GBN
  # Prints available OS flavors for the given GBN (one per line)
  local gbn="$1"
  local base="https://cloudera-build-2-us-west-2.vpc.cloudera.com/s3/build/${gbn}/cdh/7.x/"
  if ! command -v curl >/dev/null 2>&1; then
    echo "" ; return
  fi
  local html
  html="$(curl -fsSL "$base" 2>/dev/null || true)"
  if [[ -z "$html" ]]; then
    echo "" ; return
  fi
  # Extract directory names that look like OS flavors (e.g., redhat8, redhat8arm64, redhat9, redhat9arm64, sles15, ubuntu22)
  printf '%s\n' "$html" \
    | grep -Eo 'href="[^"]+/"' \
    | sed -E 's/^href="([^"/]+)\/".*$/\1/' \
    | grep -E '^(redhat[0-9]+(arm64)?|ubuntu[0-9]+|sles[0-9]+)$' \
    | sort -u
}

choose_internal_os_flavor() {
  # Args: $1=GBN
  # Echoes the selected OS flavor
  local gbn="$1"
  local options
  options="$(get_internal_os_options "$gbn")"
  local idx=0
  local -a arr=()
  if [[ -z "$options" ]]; then
    echo "Could not fetch OS flavors for GBN ${gbn}; showing known options:" >&2
    arr=(redhat8 redhat9 redhat8arm64 redhat9arm64 sles15 ubuntu22 ubuntu24)
  else
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      arr+=("$line")
    done <<<"$options"
  fi
  echo "Available OS flavors for GBN ${gbn}:" >&2
  for ((idx=0; idx<${#arr[@]}; idx++)); do
    printf "  %d) %s\n" "$((idx+1))" "${arr[$idx]}" >&2
  done
  local sel
  read -r -p "Select OS flavor [1-${#arr[@]}] (default 1): " sel || true
  if [[ -z "$sel" || ! "$sel" =~ ^[0-9]+$ || "$sel" -lt 1 || "$sel" -gt ${#arr[@]} ]]; then
    sel=1
  fi
  echo "${arr[$((sel-1))]}"
}

run_smoke_on_host() {
  local host="$1"
  local cluster_mode="$2"
  local timestamp=$(date +%Y%m%d%H%M%S)
  local tmpfile="/tmp/ozone_smoke_$timestamp.txt"
  parse_host "$host"
  echo "Waiting 15s for services to settle..."
  sleep 15
  ssh_run_as_user "set -euo pipefail; \
    echo 'Verifying safemode status...'; \
    ozone admin safemode status; \
    echo \"Creating temporary file: <<$tmpfile>>\"; \
    dd if=/dev/zero of=$tmpfile bs=1M count=1 status=none; \
    if ! ozone sh vol create demovol >/dev/null 2>&1; then echo 'ERROR: Failed to create volume' >&2; exit 1; fi; \
    if ! ozone sh bucket create demovol/demobuck >/dev/null 2>&1; then echo 'ERROR: Failed to create bucket' >&2; exit 1; fi; \
    if [ \"$cluster_mode\" == \"ha\" ]; then \
      if ! ozone sh key put demovol/demobuck/demokey $tmpfile >/dev/null; then echo 'ERROR: Failed to put key in HA mode' >&2; exit 1; fi; \
    else \
      if ! ozone sh key put -t RATIS -r ONE demovol/demobuck/demokey $tmpfile >/dev/null; then echo 'ERROR: Failed to put key in single-node mode' >&2; exit 1; fi; \
    fi; \
    if ! ozone sh key info demovol/demobuck/demokey >/dev/null 2>&1; then echo 'ERROR: Failed to get key info' >&2; exit 1; fi; \
    rm -f $tmpfile; \
    echo 'Smoke test: created demovol/demobuck and uploaded demokey.' >&2"
}

remote_install_java() {
  local major="$1"
  ssh_run "set -euo pipefail; \
    need_install='yes'; \
    jv='Not Found'; \
    if command -v java >/dev/null 2>&1; then \
      jv=\$(java -version 2>&1 | awk -F'\"' 'NR==1{print \$2}' | cut -d. -f1); \
      if [ -n \"\$jv\" ] && [ \"\$jv\" -eq $major ] 2>/dev/null; then \
        need_install='no'; \
      fi; \
    fi; \
    echo \"Whether to install Java: \$need_install, Current Default Java version: \$jv\" >&2; \
    if [ \"\$need_install\" == 'yes' ]; then \
      SUDO=; [ \"\$EUID\" -ne 0 ] && SUDO=sudo; \
      if command -v apt-get >/dev/null 2>&1; then \
        pkg=openjdk-$major-jdk; \
        \$SUDO apt-get update -y >/dev/null 2>&1 && \$SUDO apt-get install -y \$pkg >/dev/null 2>&1 || \
        \$SUDO apt-get install -y openjdk-$major-jdk-headless >/dev/null 2>&1; \
      elif command -v dnf >/dev/null 2>&1; then \
        \$SUDO dnf install -y java-$major-openjdk java-$major-openjdk-devel >/dev/null 2>&1; \
      elif command -v yum >/dev/null 2>&1; then \
        \$SUDO yum install -y java-$major-openjdk java-$major-openjdk-devel >/dev/null 2>&1 || \
        { \$SUDO yum install -y epel-release && \
          \$SUDO yum install -y java-$major-openjdk java-$major-openjdk-devel >/dev/null 2>&1; }; \
      else \
        echo 'Unsupported package manager for JDK installation. Supported: apt-get, dnf, yum.' >&2; \
        exit 1; \
      fi; \
    fi; "
}

remote_setup_java_home() {
  local major="$1"
  local java_marker="$JAVA_MARKER"
  ssh_run "set -euo pipefail; \
    mkdir -p \$(dirname $OZONE_ENV_FILE); \
    touch $OZONE_ENV_FILE; \
    NEED_UPDATE='yes'; \
    if grep -qsF \"$java_marker\" $OZONE_ENV_FILE 2>/dev/null; then \
      PROFILE_JAVA_HOME=\$(grep -A 1 \"# $java_marker\" $OZONE_ENV_FILE 2>/dev/null | sed -n \"s/^export JAVA_HOME=['\\\"]\\([^'\\\"]*\\)['\\\"]/\\1/p\" || echo ''); \
      if [ -n \"\$PROFILE_JAVA_HOME\" ] && [ -d \"\$PROFILE_JAVA_HOME\" ]; then \
        PROFILE_JAVA_BIN=\"\$PROFILE_JAVA_HOME/bin/java\"; \
        PROFILE_JAVA_MAJOR=\$(\"\$PROFILE_JAVA_BIN\" -version 2>&1 | awk -F'\"' 'NR==1{print \$2}' | cut -d. -f1 || echo ''); \
        if [ -n \"\$PROFILE_JAVA_MAJOR\" ] && [ \"\$PROFILE_JAVA_MAJOR\" == \"$major\" ]; then \
          NEED_UPDATE='no'; \
        fi; \
      fi; \
    fi; \
    echo \"Whether to update JAVA_HOME: \$NEED_UPDATE\" >&2; \
    if [ \"\$NEED_UPDATE\" == 'yes' ]; then \
      sed -i \"/# $java_marker/,/^$/d; /# $java_marker/{n;/JAVA_HOME/d;n;/PATH=/d}\" $OZONE_ENV_FILE 2>/dev/null || true; \
      for p in /usr/lib/jvm/java-$major-openjdk* /usr/lib/jvm/jre-$major-openjdk* /usr/lib/jvm/jdk-$major*; do \
        if [ -d \"\$p\" ]; then JAVA_HOME_DIR=\"\$p\"; break; fi; \
      done; \
      if [ -z \"\$JAVA_HOME_DIR\" ] || [ ! -d \"\$JAVA_HOME_DIR\" ]; then \
        echo 'Unable to determine JAVA_HOME after install' >&2; exit 1; \
      fi; \
      printf '# %s\\nexport JAVA_HOME=\"%s\"\\nexport PATH=\"\$PATH:\$JAVA_HOME/bin\"\\n\\n' \"$java_marker\" \"\$JAVA_HOME_DIR\" >> $OZONE_ENV_FILE; \
      echo \"JAVA_HOME_DIR: \$JAVA_HOME_DIR\" >&2; \
    fi"
  ensure_ozone_env_sourced
}

remote_setup_ozone_home() {
  local install_base="$1"
  local current_dir="$install_base/current"
  local env_marker="$ENV_MARKER"
  ssh_run "set -euo pipefail; \
    mkdir -p \$(dirname $OZONE_ENV_FILE); \
    touch $OZONE_ENV_FILE; \
    if ! grep -qsF \"$env_marker\" $OZONE_ENV_FILE 2>/dev/null; then \
      printf '\\n# %s\\nexport OZONE_HOME=\"%s\"\\nexport PATH=\"\$PATH:\$OZONE_HOME/bin\"\\n\\n' \"$env_marker\" \"$current_dir\" >> $OZONE_ENV_FILE; \
    fi"
  ensure_ozone_env_sourced
}

remote_create_service_user() {
  local service_user="$1"
  local service_group="$2"
  ssh_run "set -euo pipefail; \
    if ! id -u \"$service_user\" >/dev/null 2>&1; then \
      echo \"Creating service user: $service_user...\" >&2; \
      if ! command -v useradd >/dev/null 2>&1; then \
        echo 'useradd is required but not found. Please install useradd.' >&2; exit 1; \
      fi; \
      if ! getent group \"$service_group\" >/dev/null 2>&1; then \
        groupadd -r \"$service_group\" 2>/dev/null || groupadd \"$service_group\"; \
      fi; \
      useradd -r -g \"$service_group\" -d /home/$service_user -m -s /bin/bash \"$service_user\" 2>/dev/null || \
      useradd -g \"$service_group\" -d /home/$service_user -m -s /bin/bash \"$service_user\"; \
      echo \"Service user $service_user created successfully\" >&2; \
    else \
      echo \"Service user $service_user already exists\" >&2; \
    fi; \
    # Ensure user account is active and shell allows command execution \
    # Keep /bin/bash but ensure it works for command execution \
    if command -v usermod >/dev/null 2>&1; then \
      USER_SHELL=\$(getent passwd \"$service_user\" | cut -d: -f7); \
      if [ \"\$USER_SHELL\" = \"/usr/sbin/nologin\" ] || [ \"\$USER_SHELL\" = \"/sbin/nologin\" ] || [ \"\$USER_SHELL\" = \"/bin/false\" ]; then \
        usermod -s /bin/bash \"$service_user\" 2>/dev/null || true; \
      fi; \
      chsh -s /bin/bash \"$service_user\" 2>/dev/null || true; \
    fi; \
    # Unlock account if locked (some systems lock system accounts) \
    if command -v passwd >/dev/null 2>&1; then \
      passwd -u \"$service_user\" 2>/dev/null || true; \
    fi; \
    # Get actual home directory and ensure it exists with proper permissions \
    USER_HOME=\$(getent passwd \"$service_user\" | cut -d: -f6); \
    if [ -z \"\$USER_HOME\" ]; then \
      USER_HOME=\"/home/$service_user\"; \
    fi; \
    mkdir -p \"\$USER_HOME\" && chown -R $service_user:$service_group \"\$USER_HOME\" 2>/dev/null || true; \
    # Ensure the user can access their home directory (fix permissions) \
    chmod 755 \"\$USER_HOME\" 2>/dev/null || true; \
    # Set PWD to home directory to avoid getcwd errors \
    export PWD=\"\$USER_HOME\"; \
    cd \"\$USER_HOME\" 2>/dev/null || true; \
    echo \"Service user $service_user home directory: \$USER_HOME\" >&2"
}

remote_prepare_dirs() {
  local install_base="$1" data_base="$2"
  local service_user="${SERVICE_USER}"
  local service_group="${SERVICE_GROUP}"
  ssh_run "set -euo pipefail; \
    mkdir -p \"$install_base\" \"$data_base\" \"$data_base/dn\" \"$data_base/meta\"; \
    chown -R $service_user:$service_group \"$install_base\" \"$data_base\" 2>/dev/null || true; "
}

remote_download_and_extract() {
  local version="$1" install_base="$2"
  local base_url="${DL_URL%/}/${version}/"
  local base_dir="ozone-${version}"
  local file_name="ozone-${version}.tar.gz"
  local url="${base_url}${file_name}"
  local tgt="${install_base}/${file_name}"
  local link="${install_base}/current"
  local service_user="${SERVICE_USER}"
  local service_group="${SERVICE_GROUP}"
  ssh_run "set -euo pipefail; cd \"$install_base\"; \
    if ! command -v curl >/dev/null 2>&1; then \
      echo 'curl is required but not found on target host.' >&2; exit 1; \
    fi; \
    echo 'Running curl to download <<$tgt>> from <<$url>>...'; \
    curl -fSL -o $tgt $url >/dev/null 2>&1 && echo 'Downloaded <<$tgt>>' && success=1 || true; \
    if [ \"\$success\" -ne 1 ] || [ ! -s \"$tgt\" ]; then \
      echo 'Failed to download Ozone tarball from <<$url>>.' >&2; exit 1; \
    fi; \
    tar -xzf $tgt -C $install_base; echo 'Extracted to <<$install_base/$base_dir>>'; \
    rm -f $link $tgt; ln -s $install_base/$base_dir $link; echo 'Linked <<$link>> to <<$install_base/$base_dir>>'; \
    echo \"Chowning <<$install_base>> to <<$service_user>>...\" >&2; \
    chown -R $service_user:$service_group \"$install_base\" 2>/dev/null || true; "
}

remote_download_internal_and_extract() { stage_internal_or_local internal "$@"; }

remote_link_local_current() {
  local shared_path="$1" dir_name="$2" install_base="$3"
  local link="${install_base}/current"
  ssh_run "set -euo pipefail; \
    mkdir -p \"$install_base\"; \
    local_dir=\"$shared_path/$dir_name\"; \
    if [ ! -d \"\$local_dir\" ]; then \
      echo \"Local Ozone directory not found: \$local_dir\" >&2; \
      exit 1; \
    fi; \
    rm -f \"$link\"; \
    ln -s \"\$local_dir\" \"$link\"; \
    echo \"Linked <<$link>> to <<\$local_dir>>\""
}

upload_local_snapshot_to_target() { stage_internal_or_local local "$@"; }

remote_generate_configs() {
  local install_base="$1" data_base="$2" om_host="$3" scm_host="$4"
  local etc_dir="${install_base}/current/etc/hadoop"
  local hosts_yaml="${etc_dir}/ozone-hosts.yaml"
  local ozone_xml="${etc_dir}/ozone-site.xml"
  local ozone_env="${etc_dir}/ozone-env.sh"
  local core_xml="${etc_dir}/core-site.xml"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\"; \
    if [ -x \"$install_base/current/bin/ozone\" ]; then \"$install_base/current/bin/ozone\" genconf \"$etc_dir\" >/dev/null 2>&1 || true; fi"
  local tmp_hosts_file
  tmp_hosts_file="$(mktemp)"
  {
    echo "om:"
    echo "  - ${om_host}"
    echo "scm:"
    echo "  - ${scm_host}"
    echo "datanodes:"
    echo "  - ${om_host}"
    echo "recon:"
    echo "  - ${om_host}"
  } > "$tmp_hosts_file"
  local remote_tmp="/tmp/.ozone_hosts_$RANDOM.yaml"
  scp_put "$tmp_hosts_file" "$remote_tmp"
  rm -f "$tmp_hosts_file"
  local service_user="${SERVICE_USER}"
  local service_group="${SERVICE_GROUP}"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f \"$remote_tmp\" \"$hosts_yaml\" && \
    chown $service_user:$service_group \"$hosts_yaml\" 2>/dev/null || true"
  local cfg_dir="$CONFIG_DIR"
  remote_upload_xmls "$cfg_dir" "$etc_dir" "$om_host" "$data_base"
}

remote_generate_configs_ha() {
  local install_base="$1" data_base="$2" om_hosts_csv="$3" scm_hosts_csv="$4" dn_hosts_csv="$5" recon_hosts_csv="${6:-}"
  local etc_dir="${install_base}/current/etc/hadoop"
  local hosts_yaml="${etc_dir}/ozone-hosts.yaml"
  local ozone_xml="${etc_dir}/ozone-site.xml"
  local ozone_env="${etc_dir}/ozone-env.sh"
  local core_xml="${etc_dir}/core-site.xml"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\"; \
    if [ -x \"$install_base/current/bin/ozone\" ]; then \"$install_base/current/bin/ozone\" genconf \"$etc_dir\" >/dev/null 2>&1 || true; fi"
  local om_hosts_clean scm_hosts_clean dn_hosts_clean recon_hosts_clean
  om_hosts_clean="$(echo "$om_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  scm_hosts_clean="$(echo "$scm_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  dn_hosts_clean="$(echo "$dn_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  if [[ -n "$recon_hosts_csv" ]]; then
    recon_hosts_clean="$(echo "$recon_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  else
    recon_hosts_clean=""
  fi
  local first_recon_clean=""
  if [[ -n "$recon_hosts_clean" ]]; then
    first_recon_clean="$(echo "$recon_hosts_clean" | awk -F',' '{print $1}')"
  fi
  local tmp_hosts_file
  tmp_hosts_file="$(mktemp)"
  {
    echo "om:"
    printf "%s\n" "$(echo "$om_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')"
    echo "scm:"
    printf "%s\n" "$(echo "$scm_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')"
    echo "datanodes:"
    printf "%s\n" "$(echo "$dn_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')"
    echo "recon:"
    if [[ -n "$first_recon_clean" ]]; then
      printf "  - %s\n" "$first_recon_clean"
    else
      echo "  # - recon-host"
    fi
  } > "$tmp_hosts_file"
  local remote_tmp="/tmp/.ozone_hosts_$RANDOM.yaml"
  scp_put "$tmp_hosts_file" "$remote_tmp"
  rm -f "$tmp_hosts_file"
  local service_user="${SERVICE_USER}"
  local service_group="${SERVICE_GROUP}"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f \"$remote_tmp\" \"$hosts_yaml\" && \
    chown $service_user:$service_group \"$hosts_yaml\" 2>/dev/null || true"
  local om_nodes om_props
  om_nodes="$(echo "$om_hosts_clean" | awk -F',' '{for(i=1;i<=NF;i++) printf (i>1?",":"")"om"i}')"
  om_props=""
  local idx=1
  IFS=',' read -r -a _omh <<<"$om_hosts_clean"
  for h in "${_omh[@]}"; do
    om_props+="  <property>\n    <name>ozone.om.address.omservice.om${idx}</name>\n    <value>${h}:9862</value>\n  </property>\n"
    idx=$((idx+1))
  done
  local cfg_dir="$CONFIG_DIR"
  remote_upload_xmls "$cfg_dir" "$etc_dir" "$om_hosts_clean" "$data_base"
}

single_node_init_and_start() {
  local install_base="$1" data_base="$2"
  local host="$TARGET_HOST"
  local log_dir="${LOG_DIR}/${TARGET_HOST}"
  ssh_run_as_user "set -euo pipefail; if [ ! -x \"$install_base/current/bin/ozone\" ]; then echo 'ERROR: ozone binary missing or not executable' >&2; exit 1; fi"
  echo 'Initializing SCM on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone scm --init 2>&1" >> "${log_dir}/scm_init.log" 2>&1
  echo 'Starting SCM on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone --daemon start scm 2>&1" >> "${log_dir}/scm_start.log" 2>&1
  echo 'Initializing OM on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone om --init 2>&1" >> "${log_dir}/om_init.log" 2>&1
  echo 'Starting OM on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone --daemon start om 2>&1" >> "${log_dir}/om_start.log" 2>&1
  echo 'Starting Datanode on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone --daemon start datanode 2>&1" >> "${log_dir}/dn_start.log" 2>&1
  echo 'Starting Recon on <<$host>>' >&2;
  ssh_run_as_user "set -euo pipefail; mkdir -p $log_dir && ozone --daemon start recon 2>&1" >> "${log_dir}/recon_start.log" 2>&1
  echo 'Ozone services started (single-node).' >&2
}

ha_init_and_start() {
  local install_base="$1" data_base="$2"
  local first_scm first_om h
  first_scm="${SCM_HOSTS[0]}"
  local scm_log_dir="${LOG_DIR}/${first_scm}"
  first_om="${OM_HOSTS[0]}"
  local om_log_dir="${LOG_DIR}/${first_om}"
  mkdir -p $scm_log_dir $om_log_dir;
  parse_host "$first_scm"
  ssh_run_as_user "set -euo pipefail; if [ ! -x \"$install_base/current/bin/ozone\" ]; then echo 'ERROR: ozone binary missing or not executable' >&2; exit 1; fi"
  echo "Initializing SCM on <<$first_scm>>" >&2;
  ssh_run_as_user "set -euo pipefail; ozone scm --init 2>&1" >> "${scm_log_dir}/scm_init.log" 2>&1
  echo "Starting SCM on <<$first_scm>>" >&2;
  ssh_run_as_user "set -euo pipefail; ozone --daemon start scm 2>&1" >> "${scm_log_dir}/scm_start.log" 2>&1
  for h in "${SCM_HOSTS[@]:1}"; do
    (
    parse_host "$h"
    local scm_log_dir="${LOG_DIR}/${h}"
    mkdir -p $scm_log_dir;
    echo "Bootstrapping SCM on <<$h>>" >&2;
    ssh_run_as_user "set -euo pipefail; ozone scm --bootstrap 2>&1" >> "${scm_log_dir}/scm_bootstrap.log" 2>&1
    echo "Starting SCM on <<$h>>" >&2;
    ssh_run_as_user "set -euo pipefail; ozone --daemon start scm 2>&1" >> "${scm_log_dir}/scm_start.log" 2>&1
    ) &
  done
  wait
  parse_host "$first_om"
  echo "Initializing OM on <<$first_om>>" >&2;
  ssh_run_as_user "set -euo pipefail; ozone om --init 2>&1" >> "${om_log_dir}/om_init.log" 2>&1
  echo "Starting OM on <<$first_om>>" >&2;
  ssh_run_as_user "set -euo pipefail; ozone --daemon start om 2>&1" >> "${om_log_dir}/om_start.log" 2>&1
  for h in "${OM_HOSTS[@]:1}"; do
    (
    parse_host "$h"
    local om_log_dir="${LOG_DIR}/${h}"
    mkdir -p $om_log_dir;
    echo "Bootstrapping OM on <<$h>>" >&2;
    ssh_run_as_user "set -euo pipefail; ozone om --init 2>&1" >> "${om_log_dir}/om_init.log" 2>&1
    echo "Starting OM on <<$h>>" >&2;
    ssh_run_as_user "set -euo pipefail; ozone --daemon start om 2>&1" >> "${om_log_dir}/om_start.log" 2>&1
    ) &
  done
  wait
  for h in "${DN_HOSTS[@]}"; do
    (
      parse_host "$h"
      local dn_log_dir="${LOG_DIR}/${h}"
      mkdir -p $dn_log_dir;
      echo "Starting Datanode on <<$h>>" >&2;
      ssh_run_as_user "set -euo pipefail; ozone --daemon start datanode 2>&1" >> "${dn_log_dir}/dn_start.log" 2>&1
    ) &
  done
  wait
  # Start Recon only on one node based on host-map (first entry in RECON_HOSTS if provided)
  if [[ ${#RECON_HOSTS[@]} -gt 0 ]]; then
    local recon_target="${RECON_HOSTS[0]}"
    parse_host "$recon_target"
    local recon_log_dir="${LOG_DIR}/${recon_target}"
    mkdir -p $recon_log_dir;
    echo "Starting Recon on <<$recon_target>>" >&2;
    ssh_run_as_user "set -euo pipefail; ozone --daemon start recon 2>&1" >> "${recon_log_dir}/recon_start.log" 2>&1
  fi
}
