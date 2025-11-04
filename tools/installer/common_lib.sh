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
  if [[ -n "${SSH_USER:-}" ]]; then
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
      cmd="sudo -i bash -c $cmd_quoted"
    else
      cmd="sudo -i $cmd"
    fi
  fi
  ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "${TARGET_USER}@${TARGET_HOST}" "$cmd"
}

ssh_run_as_user() {
  local target_user="${SERVICE_USER:-}"
  if [[ -n "$target_user" ]]; then
    local cmd_quoted
    cmd_quoted=$(printf '%q' "$*")
    
    if [[ "${USE_SUDO:-no}" == "yes" ]]; then
      ssh_run "sudo -i -u $target_user bash -c $cmd_quoted"
    else
      ssh_run "if command -v sudo >/dev/null 2>&1; then sudo -i -u $target_user bash -c $cmd_quoted; else su $target_user -c \"bash -c $cmd_quoted\"; fi"
    fi
  else
    ssh_run "$*"
  fi
}

scp_put() {
  scp -P "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$1" "${TARGET_USER}@${TARGET_HOST}:$2"
}

remote_cleanup_dirs() {
  local install_base="$1" data_base="$2"
  ssh_run "set -euo pipefail; \
    echo 'Cleaning install base: $install_base and data base: $data_base...' >&2; \
    rm -rf $install_base $data_base; \
    mkdir -p $install_base $data_base; \
    echo 'Stopping Ozone processes (if any)...' >&2; \
    for pat in 'Hdds[D]atanodeService' 'Ozone[M]anagerStarter' 'StorageContainerManager[S]tarter'; do \
      pkill -9 -f \"\$pat\" || true; \
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
        if [ -n \"${SERVICE_USER:-}\" ]; then \
          chown $service_user:$service_group \"$etc_dir/ozone-site.xml\" 2>/dev/null || true; \
        fi"
    ) &
  fi
  if [[ -f "$config_dir/core-site.xml" ]]; then
    (
      scp_put "$config_dir/core-site.xml" "/tmp/$tmp_core"
      ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f /tmp/$tmp_core \"$etc_dir/core-site.xml\" && \
        sed -i${sed_hosts} \"$etc_dir/core-site.xml\" && \
        if [ -n \"${SERVICE_USER:-}\" ]; then \
          chown $service_user:$service_group \"$etc_dir/core-site.xml\" 2>/dev/null || true; \
        fi"
    ) &
  fi
  if [[ -f "$config_dir/ozone-env.sh" ]]; then
    (
      scp_put "$config_dir/ozone-env.sh" "/tmp/$tmp_env"
      ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\" && mv -f /tmp/$tmp_env \"$etc_dir/ozone-env.sh\" && \
        sed -i${sed_hosts} \"$etc_dir/ozone-env.sh\" && \
        if [ -n \"${SERVICE_USER:-}\" ]; then \
          chown $service_user:$service_group \"$etc_dir/ozone-env.sh\" 2>/dev/null || true; \
        fi"
    ) &
  fi
  wait
}

join_by() { local IFS="$1"; shift; echo "$*"; }

unique_hosts() {
  awk '!seen[$0]++' <(printf "%s\n" "$@")
}

install_shared_ssh_key() {
  # Install a shared SSH keypair on the TARGET_HOST so hosts can SSH to each other passwordlessly
  # Args: $1=local_private_key_path, $2=local_public_key_path
  local local_priv="$1" local_pub="$2"
  local service_user="${SERVICE_USER:-ozone}"
  local service_group="${SERVICE_GROUP:-ozone}"
  local tmp_priv="/tmp/.ozone_cluster_key" tmp_pub="/tmp/.ozone_cluster_key.pub"
  scp_put "$local_priv" "$tmp_priv"
  scp_put "$local_pub" "$tmp_pub"
  
  # Function to install SSH key for a specific user
  ssh_run "set -euo pipefail; \
    install_key_for_user() { \
      local target_user=\"\$1\"; \
      local target_group=\"\$2\"; \
      local user_home; \
      if [ \"\$target_user\" = \"root\" ] || [ -z \"\$target_user\" ]; then \
        user_home=\"/root\"; \
      else \
        user_home=\$(getent passwd \"\$target_user\" | cut -d: -f6 2>/dev/null || echo \"/home/\$target_user\"); \
      fi; \
      local ssh_dir=\"\$user_home/.ssh\"; \
      mkdir -p \"\$ssh_dir\" && chmod 700 \"\$ssh_dir\"; \
      touch \"\$ssh_dir/authorized_keys\" && chmod 600 \"\$ssh_dir/authorized_keys\"; \
      PUB_KEY=\$(cat $tmp_pub); \
      # Extract key type and key data (ignoring comments) for comparison
      KEY_TYPE=\$(echo \"\$PUB_KEY\" | awk '{print \$1}'); \
      KEY_DATA=\$(echo \"\$PUB_KEY\" | awk '{print \$2}'); \
      # Check if this exact key (type + data) already exists, ignoring comments
      if ! awk -v key_type=\"\$KEY_TYPE\" -v key_data=\"\$KEY_DATA\" \
        'BEGIN {found=0} \
         \$1 == key_type && \$2 == key_data {found=1; exit} \
         END {exit !found}' \"\$ssh_dir/authorized_keys\" 2>/dev/null; then \
        echo \"\$PUB_KEY\" >> \"\$ssh_dir/authorized_keys\"; \
      fi; \
      cp -f $tmp_priv \"\$ssh_dir/id_ed25519\"; \
      cp -f $tmp_pub  \"\$ssh_dir/id_ed25519.pub\"; \
      chmod 600 \"\$ssh_dir/id_ed25519\"; \
      chmod 644 \"\$ssh_dir/id_ed25519.pub\"; \
      local cfg=\"\$ssh_dir/config\"; \
      if [ ! -f \"\$cfg\" ] || ! grep -qsF 'StrictHostKeyChecking' \"\$cfg\" 2>/dev/null; then \
        printf 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n' >> \"\$cfg\"; \
        chmod 600 \"\$cfg\"; \
      fi; \
      # Set ownership for all files in ssh_dir
      if [ \"\$target_user\" != \"root\" ] && [ -n \"\$target_user\" ]; then \
        chown -R \$target_user:\$target_group \"\$ssh_dir\" 2>/dev/null || true; \
      fi; \
      # Deduplicate authorized_keys after adding
      if [ -f \"\$ssh_dir/authorized_keys\" ]; then \
        awk '{ \
          match(\$0, /^[^ ]* +([^ ]+)/, arr); \
          if (arr[1] != \"\") { \
            key = \$1 \" \" arr[1]; \
            if (!seen[key]++) print; \
          } else { \
            print; \
          } \
        }' \"\$ssh_dir/authorized_keys\" > \"\$ssh_dir/authorized_keys.tmp\" 2>/dev/null && \
        mv -f \"\$ssh_dir/authorized_keys.tmp\" \"\$ssh_dir/authorized_keys\" && \
        chmod 600 \"\$ssh_dir/authorized_keys\" || true; \
      fi; \
    }; \
    # Install for SSH user (current user)
    CURRENT_USER=\$(whoami); \
    install_key_for_user \"\$CURRENT_USER\" \"$service_group\"; \
    # Install for service user if different and set
    if [ -n \"$service_user\" ] && [ \"$service_user\" != \"\$CURRENT_USER\" ]; then \
      install_key_for_user \"$service_user\" \"$service_group\"; \
    fi; \
    rm -f $tmp_priv $tmp_pub"
  # Also deduplicate using our helper function for both users
  deduplicate_authorized_keys "$TARGET_USER"
  if [[ -n "$service_user" && "$service_user" != "$TARGET_USER" ]]; then
    deduplicate_authorized_keys "$service_user"
  fi
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
        \$SUDO apt-get update -y && \$SUDO apt-get install -y \$pkg || \
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

deduplicate_authorized_keys() {
  # Helper function to deduplicate authorized_keys file
  # This removes exact duplicate lines and also handles keys with same content but different comments
  local target_user="${1:-}"
  if [[ -n "$target_user" && "$target_user" != "root" ]]; then
    local user_home
    user_home=$(ssh_run "getent passwd \"$target_user\" | cut -d: -f6" 2>/dev/null || echo "")
    if [[ -z "$user_home" ]]; then
      user_home="/home/$target_user"
    fi
    ssh_run "set -euo pipefail; \
      AUTH_KEYS=\"$user_home/.ssh/authorized_keys\"; \
      if [ -f \"\$AUTH_KEYS\" ]; then \
        # Extract key type and key data (ignoring comments) for deduplication
        awk '{ \
          # Extract key type (first field) and key data (second field), ignoring comments \
          match(\$0, /^[^ ]* +([^ ]+)/, arr); \
          if (arr[1] != \"\") { \
            key = \$1 \" \" arr[1]; \
            if (!seen[key]++) print; \
          } else { \
            print; \
          } \
        }' \"\$AUTH_KEYS\" > \"\$AUTH_KEYS.tmp\" 2>/dev/null && \
        mv -f \"\$AUTH_KEYS.tmp\" \"\$AUTH_KEYS\" && \
        chmod 600 \"\$AUTH_KEYS\" || true; \
      fi"
  else
    ssh_run "set -euo pipefail; \
      AUTH_KEYS=\"\$HOME/.ssh/authorized_keys\"; \
      if [ -f \"\$AUTH_KEYS\" ]; then \
        # Extract key type and key data (ignoring comments) for deduplication
        awk '{ \
          # Extract key type (first field) and key data (second field), ignoring comments \
          match(\$0, /^[^ ]* +([^ ]+)/, arr); \
          if (arr[1] != \"\") { \
            key = \$1 \" \" arr[1]; \
            if (!seen[key]++) print; \
          } else { \
            print; \
          } \
        }' \"\$AUTH_KEYS\" > \"\$AUTH_KEYS.tmp\" 2>/dev/null && \
        mv -f \"\$AUTH_KEYS.tmp\" \"\$AUTH_KEYS\" && \
        chmod 600 \"\$AUTH_KEYS\" || true; \
      fi"
  fi
}

ensure_passwordless_ssh() {
  if [[ ! -f "$HOME/.ssh/id_ed25519" ]]; then
    echo "Generating a new SSH key (ed25519)..."
    ssh-keygen -t ed25519 -N "" -f "$HOME/.ssh/id_ed25519" >/dev/null
  fi
  local keyfile="$HOME/.ssh/id_ed25519.pub"

  echo "Setting up passwordless SSH to ${TARGET_USER}@${TARGET_HOST}:${SSH_PORT}..."
  if [[ "$AUTH_METHOD" == "password" ]]; then
    if [[ -z "${AUTH_PASSWORD:-}" ]]; then
      read -rs -p "Enter SSH password for ${TARGET_USER}@${TARGET_HOST}: " AUTH_PASSWORD
      echo
    fi
    # Check if key already exists before running ssh-copy-id
    local pub_key_content
    pub_key_content="$(cat "$keyfile")"
    sshpass -p "$AUTH_PASSWORD" ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "${TARGET_USER}@${TARGET_HOST}" "set -euo pipefail; \
      mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; \
      if ! grep -qsF \"$pub_key_content\" ~/.ssh/authorized_keys 2>/dev/null; then \
        echo \"$pub_key_content\" >> ~/.ssh/authorized_keys; \
      fi" >/dev/null 2>&1 || true
    # Always deduplicate after any key operation
    deduplicate_authorized_keys "$TARGET_USER"
  else
    local use_key
    use_key="${AUTH_KEYFILE:-$HOME/.ssh/id_ed25519}"
    if [[ ! -f "$use_key" ]]; then
      echo "Error: key file $use_key does not exist." >&2
      exit 1
    fi
    local tmp_pub
    tmp_pub="$(mktemp)"
    if ! ssh-keygen -y -f "$use_key" > "$tmp_pub" 2>/dev/null; then
      echo "Error: could not derive public key from $use_key" >&2
      rm -f "$tmp_pub"
      exit 1
    fi
    local pub_key_content
    pub_key_content="$(cat "$tmp_pub")"
    # Always check and add manually (don't use ssh-copy-id to avoid duplicates)
    if [[ -n "${AUTH_PASSWORD:-}" ]]; then
      # Use password to push the public key (check for duplicates first)
      sshpass -p "${AUTH_PASSWORD}" scp -P "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$tmp_pub" "${TARGET_USER}@${TARGET_HOST}:/tmp/.ozone_installer_key.pub" >/dev/null 2>&1
      sshpass -p "${AUTH_PASSWORD}" ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "${TARGET_USER}@${TARGET_HOST}" "set -euo pipefail; \
        mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; \
        if ! grep -qsF \"\$(cat /tmp/.ozone_installer_key.pub)\" ~/.ssh/authorized_keys 2>/dev/null; then \
          cat /tmp/.ozone_installer_key.pub >> ~/.ssh/authorized_keys; \
        fi; \
        rm -f /tmp/.ozone_installer_key.pub" >/dev/null 2>&1
    else
      # No password available; try our standard helpers (check for duplicates first)
      scp_put "$tmp_pub" "/tmp/.ozone_installer_key.pub"
      ssh_run "set -euo pipefail; \
        mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys; \
        if ! grep -qsF \"\$(cat /tmp/.ozone_installer_key.pub)\" ~/.ssh/authorized_keys 2>/dev/null; then \
          cat /tmp/.ozone_installer_key.pub >> ~/.ssh/authorized_keys; \
        fi; \
        rm -f /tmp/.ozone_installer_key.pub"
    fi
    rm -f "$tmp_pub"
    # Always deduplicate after any key operation
    deduplicate_authorized_keys "$TARGET_USER"
  fi
  echo "Passwordless SSH is configured. Testing..."
  ssh_run "echo OK" >/dev/null
}

ensure_passwordless_ssh_to_host() {
  local host="$1"
  local prev_host="$TARGET_HOST" prev_port="$SSH_PORT"
  parse_host "$host"
  ensure_passwordless_ssh
  TARGET_HOST="$prev_host"; SSH_PORT="$prev_port"
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
    if [ -n \"${SERVICE_USER:-}\" ]; then \
      chown -R $service_user:$service_group \"$install_base\" \"$data_base\" 2>/dev/null || true; \
    fi"
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
    if [ -n \"$service_user\" ]; then \
      echo \"Chowning <<$install_base>> to <<$service_user>>...\" >&2; \
      chown -R $service_user:$service_group \"$install_base\" 2>/dev/null || true; \
    fi"
}

remote_generate_configs() {
  local install_base="$1" data_base="$2" om_host="$3" scm_host="$4"
  local etc_dir="${install_base}/current/etc/hadoop"
  local hosts_yaml="${etc_dir}/ozone-hosts.yaml"
  local ozone_xml="${etc_dir}/ozone-site.xml"
  local ozone_env="${etc_dir}/ozone-env.sh"
  local core_xml="${etc_dir}/core-site.xml"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\"; \
    if [ -x \"$install_base/current/bin/ozone\" ]; then \"$install_base/current/bin/ozone\" genconf \"$etc_dir\" >/dev/null 2>&1 || true; fi"
  ssh_run "cat > \"$hosts_yaml\" <<'YAML'
om:
  - ${om_host}
scm:
  - ${scm_host}
datanodes:
  - ${om_host}
recon:
  # - recon-host
YAML"
  local cfg_dir="$CONFIG_DIR"
  remote_upload_xmls "$cfg_dir" "$etc_dir" "$om_host" "$data_base"
}

remote_generate_configs_ha() {
  local install_base="$1" data_base="$2" om_hosts_csv="$3" scm_hosts_csv="$4" dn_hosts_csv="$5"
  local etc_dir="${install_base}/current/etc/hadoop"
  local hosts_yaml="${etc_dir}/ozone-hosts.yaml"
  local ozone_xml="${etc_dir}/ozone-site.xml"
  local ozone_env="${etc_dir}/ozone-env.sh"
  local core_xml="${etc_dir}/core-site.xml"
  ssh_run "set -euo pipefail; mkdir -p \"$etc_dir\"; \
    if [ -x \"$install_base/current/bin/ozone\" ]; then \"$install_base/current/bin/ozone\" genconf \"$etc_dir\" >/dev/null 2>&1 || true; fi"
  local om_hosts_clean scm_hosts_clean dn_hosts_clean
  om_hosts_clean="$(echo "$om_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  scm_hosts_clean="$(echo "$scm_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  dn_hosts_clean="$(echo "$dn_hosts_csv" | tr ',' '\n' | sed 's/.*@//' | sed 's/:.*//' | paste -sd, -)"
  ssh_run "cat > \"$hosts_yaml\" <<YAML
om:
$(echo "$om_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')
scm:
$(echo "$scm_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')
datanodes:
$(echo "$dn_hosts_clean" | tr ',' '\n' | sed 's/^/  - /')
recon:
  # - recon-host
YAML"
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
  ssh_run_as_user "set -euo pipefail; \
    if [ ! -x \"$install_base/current/bin/ozone\" ]; then echo 'ERROR: ozone binary missing or not executable' >&2; exit 1; fi; \
    echo 'Initializing SCM on <<$TARGET_HOST>>' >&2; \
    if ! ozone scm --init >/dev/null 2>&1; then echo 'ERROR: Failed to initialize SCM on <<$TARGET_HOST>>' >&2; exit 1; fi; \
    echo 'Starting SCM on <<$TARGET_HOST>>' >&2; \
    if ! ozone --daemon start scm >/dev/null 2>&1; then echo 'ERROR: Failed to start SCM on <<$TARGET_HOST>>' >&2; exit 1; fi; \
    echo 'Initializing OM on <<$TARGET_HOST>>' >&2; \
    if ! ozone om --init >/dev/null 2>&1; then echo 'ERROR: Failed to initialize OM on <<$TARGET_HOST>>' >&2; exit 1; fi; \
    echo 'Starting OM on <<$TARGET_HOST>>' >&2; \
    if ! ozone --daemon start om >/dev/null 2>&1; then echo 'ERROR: Failed to start OM on <<$TARGET_HOST>>' >&2; exit 1; fi; \
    echo 'Starting Datanode on <<$TARGET_HOST>>' >&2; \
    if ! ozone --daemon start datanode >/dev/null 2>&1; then echo 'ERROR: Failed to start Datanode on <<$TARGET_HOST>>' >&2; exit 1; fi; \
    echo 'Ozone services started (single-node).' >&2"
}

ha_init_and_start() {
  local install_base="$1" data_base="$2"
  local first_scm first_om h
  first_scm="${SCM_HOSTS[0]}"
  first_om="${OM_HOSTS[0]}"
  parse_host "$first_scm"
  ssh_run_as_user "set -euo pipefail; \
    if [ ! -x \"$install_base/current/bin/ozone\" ]; then echo 'ERROR: ozone binary missing or not executable' >&2; exit 1; fi; \
    echo 'Initializing SCM on <<$first_scm>>' >&2; \
    if ! ozone scm --init >/dev/null 2>&1; then echo 'ERROR: Failed to initialize SCM on <<$first_scm>>' >&2; exit 1; fi; \
    echo 'Starting SCM on <<$first_scm>>' >&2; \
    if ! ozone --daemon start scm >/dev/null 2>&1; then echo 'ERROR: Failed to start SCM on <<$first_scm>>' >&2; exit 1; fi;"
  for h in "${SCM_HOSTS[@]:1}"; do
    (
    parse_host "$h"
    ssh_run_as_user "set -euo pipefail; \
      echo 'Bootstrapping SCM on <<$h>>' >&2; \
      if ! ozone scm --bootstrap >/dev/null 2>&1; then echo 'ERROR: Failed to bootstrap SCM on <<$h>>' >&2; exit 1; fi; \
      echo 'Starting SCM on <<$h>>' >&2; \
      if ! ozone --daemon start scm >/dev/null 2>&1; then echo 'ERROR: Failed to start SCM on <<$h>>' >&2; exit 1; fi;"
    ) &
  done
  wait
  parse_host "$first_om"
  ssh_run_as_user "set -euo pipefail; \
    echo 'Initializing OM on <<$first_om>>' >&2; \
    if ! ozone om --init >/dev/null 2>&1; then echo 'ERROR: Failed to initialize OM on <<$first_om>>' >&2; return; fi; \
    echo 'Starting OM on <<$first_om>>' >&2; \
    if ! ozone --daemon start om >/dev/null 2>&1; then echo 'ERROR: Failed to start OM on <<$first_om>>' >&2; return; fi;"
  for h in "${OM_HOSTS[@]:1}"; do
    (
    parse_host "$h"
    ssh_run_as_user "set -euo pipefail; \
      echo 'Bootstrapping OM on <<$h>>' >&2; \
      if ! ozone om --init >/dev/null 2>&1; then echo 'ERROR: Failed to initialize OM on <<$h>>' >&2; return; fi; \
      echo 'Starting OM on <<$h>>' >&2; \
      if ! ozone --daemon start om >/dev/null 2>&1; then echo 'ERROR: Failed to start OM on <<$h>>' >&2; return; fi;"
    ) &
  done
  wait
  for h in "${DN_HOSTS[@]}"; do
    (
      parse_host "$h"
      ssh_run_as_user "set -euo pipefail; \
        echo 'Starting Datanode on <<$h>>' >&2; \
        if ! ozone --daemon start datanode >/dev/null 2>&1; then echo 'ERROR: Failed to start Datanode on <<$h>>' >&2; exit 1; fi;"
    ) &
  done
  wait
}
