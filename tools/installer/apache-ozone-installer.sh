#!/usr/bin/env bash

set -euo pipefail

# Apache Ozone Upstream Installer (single-node friendly, multi-host ready)
# This script helps you:
#  - Setup passwordless SSH to a Linux host (password or key)
#  - Select an Apache Ozone release version
#  - Download and extract Ozone on target host
#  - Generate config templates (ozone-site.xml, core-site.xml, ozone-env.sh, ozone-hosts.yaml)
#  - Optionally initialize and start a single-node Ozone

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
# Load env first
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

DEFAULT_INSTALL_BASE="${DEFAULT_INSTALL_BASE:-/opt/ozone}"
DEFAULT_DATA_BASE="${DEFAULT_DATA_BASE:-/data/ozone}"
DEFAULT_VERSION="${DEFAULT_VERSION:-2.0.0}"
JDK_MAJOR="${JDK_MAJOR:-17}"
INSTALL_DIR_SET="no"
DATA_DIR_SET="no"
VERSION_SET="no"
JDK_SET="no"
SSH_USER="${SSH_USER:-root}"
DL_URL="${DL_URL:-https://dlcdn.apache.org/ozone/}"
SEPARATOR="${SEPARATOR:--------------------------------------------}"
SSH_PORT="${SSH_PORT:-22}"
OZONE_OPTS="${OZONE_OPTS:--Xmx1024m -XX:ParallelGCThreads=8}"
START_AFTER_INSTALL="${START_AFTER_INSTALL:-yes}"
TARGET_USER=""
TARGET_HOST=""
CLUSTER_MODE="${CLUSTER_MODE:-single}" # single|ha
ROLE_FILE="" # local YAML with om/scm/datanodes/recon lists
CONFIG_DIR=""
CLEAN_MODE="${CLEAN_MODE:-yes}"
CLEAN_MODE_SET="no"
HOSTS_ARG_RAW=""
VERSION=""
USE_SUDO="${USE_SUDO:-no}"
SERVICE_USER="${SERVICE_USER:-ozone}"
SERVICE_GROUP="${SERVICE_GROUP:-ozone}"

declare -a OM_HOSTS
declare -a SCM_HOSTS
declare -a DN_HOSTS
declare -a RECON_HOSTS

print_header() {
  echo "${SEPARATOR}"
  echo " Apache Ozone Upstream Installer"
  echo "${SEPARATOR}"
}

usage() {
  cat <<EOF
Usage: $SCRIPT_NAME [options]

Options:
  -H, --host <[user@]host[:port]>   Target host (single-node mode)
  -m, --auth-method <password|key>  SSH auth method (default: password)
  -p, --password <password>         SSH password (if --auth-method=password)
  -k, --keyfile <path>              SSH private key file (if --auth-method=key)
  -v, --version <x.y.z>             Ozone version (e.g., 1.4.0). If omitted, you can select interactively
  -i, --install-dir <path>          Install Root Directory on host (default: $DEFAULT_INSTALL_BASE)
  -d, --data-dir <path>             Data Root Directory on host (default: $DEFAULT_DATA_BASE)
  -s, --start                       Initialize and start single-node Ozone after install
  # Mode selection is automatic: HA if >=3 hosts provided (via -H or role-file), single-node otherwise
  -R, --role-file <filename>       Role file filename (path derived from --config-dir, required for HA)
  -J, --jdk-version <17|21>         JDK major version to install and set (default: 17)
  -C, --config-dir <path>           Local dir with ozone-site.xml, core-site.xml, ozone-env.sh, and role file (default: script_dir/configs/unsecure/single or ha)
  -x, --clean                       Cleanup install and data directories before install
  -l --ssh-user <user>             SSH username (overrides user@ in host)
  -S, --use-sudo                     Run remote commands via sudo
  -u, --service-user <user>          Service user to run Ozone commands (default: ozone)
  -g, --service-group <group>        Service group to run Ozone commands (default: ozone)
  -h, --help                        Show this help

Examples:
  $SCRIPT_NAME -H myhost -m password -p 'mypw' -v 1.4.0 -s -l ubuntu
  $SCRIPT_NAME -H 10.0.0.10:2222 -m key -k ~/.ssh/id_rsa -i /opt/ozone -d /data/ozone -l ubuntu
  $SCRIPT_NAME -H myhost -m key -k ~/.ssh/id_rsa -v 2.0.0 -S -l ubuntu
  $SCRIPT_NAME -H myhost -m key -k ~/.ssh/id_rsa -v 2.0.0 -u ozone -S -l ubuntu
  # HA with brace expansion: installer-{1..10}.domain expands to installer-1.domain,...,installer-10.domain
  $SCRIPT_NAME -H "installer-{1..10}.domain" -m key -k ~/.ssh/id_rsa -v 2.0.0
  # HA with roles mapping from YAML (om/scm/datanodes lists)
  $SCRIPT_NAME -M ha -C ./configs -R cluster.yaml -m key -k ~/.ssh/id_rsa -v 2.0.0
EOF
}

INSTALL_BASE="$DEFAULT_INSTALL_BASE"
DATA_BASE="$DEFAULT_DATA_BASE"

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -H|--host)
        HOSTS_ARG_RAW="$2"; parse_host "${2%%,*}"; shift 2 ;;
      -m|--auth-method)
        AUTH_METHOD="$2"; shift 2 ;;
      -p|--password)
        AUTH_PASSWORD="$2"; shift 2 ;;
      -k|--keyfile)
        AUTH_KEYFILE="$2"; shift 2 ;;
      -v|--version)
        VERSION="$2"; VERSION_SET="yes"; shift 2 ;;
      -i|--install-dir)
        INSTALL_BASE="$2"; INSTALL_DIR_SET="yes"; shift 2 ;;
      -d|--data-dir)
        DATA_BASE="$2"; DATA_DIR_SET="yes"; shift 2 ;;
      -s|--start)
        START_AFTER_INSTALL="yes"; shift 1 ;;
      -M|--cluster-mode)
        CLUSTER_MODE="$2"; shift 2 ;;
      -R|--role-file)
        ROLE_FILE="$(basename "$2")"; shift 2 ;;
      -J|--jdk-version)
        JDK_MAJOR="$2"; JDK_SET="yes"; shift 2 ;;
      -C|--config-dir)
        CONFIG_DIR="$2"; shift 2 ;;
      -x|--clean)
        CLEAN_MODE="yes"; CLEAN_MODE_SET="yes"; shift 1 ;;
      -l|--ssh-user)
        SSH_USER="$2"; shift 2 ;;
      -S|--use-sudo)
        USE_SUDO="yes"; shift 1 ;;
      -u|--service-user)
        SERVICE_USER="$2"; shift 2 ;;
      -g|--service-group)
        SERVICE_GROUP="$2"; shift 2 ;;
      -h|--help)
        usage; exit 0 ;;
      *)
        echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done
}

main() {
  print_header
  parse_args "$@"

  # Expand brace patterns in HOSTS_ARG_RAW if present
  if [[ -n "$HOSTS_ARG_RAW" ]]; then
    HOSTS_ARG_RAW=$(expand_brace_patterns "$HOSTS_ARG_RAW")
  fi

  if [[ "$AUTH_METHOD" != "password" && "$AUTH_METHOD" != "key" ]]; then
    echo "Error: --auth-method must be 'password' or 'key'" >&2
    exit 1
  fi

  if [[ "$AUTH_METHOD" == "key" && -z "${AUTH_KEYFILE:-}" ]]; then
    confirm_or_read "Path to SSH private key" AUTH_KEYFILE "$HOME/.ssh/id_ed25519"
  fi

  if [[ "$INSTALL_DIR_SET" != "yes" ]]; then
    confirm_or_read "Install Root Directory on host" INSTALL_BASE "$INSTALL_BASE"
  fi
  if [[ "$DATA_DIR_SET" != "yes" ]]; then
    confirm_or_read "Data Root Directory on host" DATA_BASE "$DATA_BASE"
  fi
  if [[ "$JDK_SET" != "yes" ]]; then
    confirm_or_read "JDK major version to install and set" JDK_MAJOR "$JDK_MAJOR"
  fi
  if [[ "$VERSION_SET" != "yes" ]]; then
    confirm_or_read "Ozone version to install" VERSION "$(select_version)"
  fi
  if [[ "$CLEAN_MODE_SET" != "yes" ]]; then
    confirm_or_read "Cleanup install and data directories" CLEAN_MODE "$CLEAN_MODE"
  fi

  ensure_local_prereqs

  # Auto-select mode based on number of hosts
  if [[ -n "$HOSTS_ARG_RAW" ]]; then
    IFS=',' read -r -a _auto_hosts <<<"$HOSTS_ARG_RAW"
    if [[ ${#_auto_hosts[@]} -ge 3 ]]; then
      CLUSTER_MODE="ha"
    else
      CLUSTER_MODE="single"
    fi
  fi

  # Prompt for CONFIG_DIR if not provided
  if [[ -z "$CONFIG_DIR" ]]; then
    local config_base="$SCRIPT_DIR/configs"
    local config_options
    config_options="$(find_config_dirs "$config_base")"
    
    if [[ -n "$config_options" ]]; then
      echo "Available config directories (containing ozone-site.xml):"
      local idx=1
      local -a config_array
      local -a config_paths
      while IFS= read -r option; do
        [[ -z "$option" ]] && continue
        echo "  $idx. $option"
        config_array+=("$option")
        config_paths+=("$config_base/$option")
        idx=$((idx + 1))
      done <<<"$config_options"
      
      local selected_num selected_path
      local default_option default_option_num=""
      if [[ ${#config_array[@]} -gt 0 ]]; then
        local default_option_str
        if [[ $CLUSTER_MODE == "ha" ]]; then
          default_option_str="unsecure/ha"
        else
          default_option_str="unsecure/single"
        fi
        # Find the index of the default option in config_array
        local idx=0
        for opt in "${config_array[@]}"; do
          idx=$((idx + 1))
          if [[ "$opt" == "$default_option_str" ]]; then
            default_option_num="$idx"
            default_option="$default_option_str"
            break
          fi
        done
        # If not found, use first option as default
        if [[ -z "$default_option_num" ]]; then
          default_option_num="1"
          default_option="${config_array[0]}"
        fi
      fi
      local prompt_msg="Select config option number (or enter custom path)"
      if [[ -n "$default_option_num" ]]; then
        prompt_msg+=" [default: $default_option_num for $CLUSTER_MODE mode]"
      fi
      read -r -p "$prompt_msg: " selected_num || true
      
      # If empty input, use default option number
      if [[ -z "$selected_num" && -n "$default_option_num" ]]; then
        selected_num="$default_option_num"
      fi
      
      # Check if input is a number
      if [[ "$selected_num" =~ ^[0-9]+$ ]] && [[ "$selected_num" -ge 1 ]] && [[ "$selected_num" -le ${#config_array[@]} ]]; then
        selected_path="${config_paths[$((selected_num - 1))]}"
        CONFIG_DIR="$selected_path"
      elif [[ -n "$selected_num" ]]; then
        # User entered a custom path
        CONFIG_DIR="$selected_num"
      else
        # No config directories found, use defaults
        echo "No config directories found, exiting..."
        exit 1
      fi
    fi
  else
    if [[ ! -f "$CONFIG_DIR/ozone-site.xml" ]]; then
      echo "Error: ozone-site.xml not found in config directory: $CONFIG_DIR" >&2
      exit 1
    fi
  fi

  # Prompt for ROLE_FILE if in HA mode and not provided
  if [[ "$CLUSTER_MODE" == "ha" ]]; then
    ROLE_FILE="${ROLE_FILE:-host-map.yml}"
    if [[ ! -f "$CONFIG_DIR/$ROLE_FILE" ]]; then
      confirm_or_read "Role file filename" ROLE_FILE "host-map.yml"
    fi
  fi

  # Summary table
    # Record start time
  START_TS=$(date +%s)
  START_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  printf "%s\n" "${SEPARATOR}"
  printf "%-18s | %s\n" "Started at" "$START_ISO"
  printf "%-18s-+-%s\n" "------------------" "----------------------"
  printf "%-18s | %s\n" "Property" "Value"
  printf "%-18s-+-%s\n" "------------------" "----------------------"
  printf "%-18s | %s\n" "Ozone version" "$VERSION"
  printf "%-18s | %s\n" "Install directory" "$INSTALL_BASE"
  printf "%-18s | %s\n" "Data directory" "$DATA_BASE"
  printf "%-18s | %s\n" "JDK version" "$JDK_MAJOR"
  printf "%-18s | %s\n" "Deployment Mode" "$CLUSTER_MODE"
  if [[ -n "$SERVICE_USER" ]]; then
    printf "%-18s | %s\n" "Service User" "$SERVICE_USER"
    printf "%-18s | %s\n" "Service Group" "$SERVICE_GROUP"
  fi
  if [[ "${USE_SUDO:-no}" == "yes" ]]; then
    printf "%-18s | %s\n" "Use Sudo" "yes"
  fi
  printf "%-18s-+-%s\n" "------------------" "----------------------"
  printf "%-18s | %s\n" "Config directory" "$CONFIG_DIR"
  if [[ "$CLUSTER_MODE" == "ha" ]]; then
    printf "%-18s | %s\n" "Host Map file" "$ROLE_FILE"
  fi
  printf "%-18s-+-%s\n" "------------------" "----------------------"


  if [[ "$CLUSTER_MODE" == "single" ]]; then
    if [[ -z "$TARGET_HOST" ]]; then
      confirm_or_read "Target host ([user@]host[:port])" TARGET_HOST ""
      parse_host "$TARGET_HOST"
    fi
    # ensure_passwordless_ssh

    if [[ "$CLEAN_MODE" == "yes" ]]; then
      echo "Cleaning install and data dirs on target..."
      remote_cleanup_dirs "$INSTALL_BASE" "$DATA_BASE"
    fi

    if [[ -n "$SERVICE_USER" ]]; then
      echo "Creating service user $SERVICE_USER with group $SERVICE_GROUP on target..."
      remote_create_service_user "$SERVICE_USER" "$SERVICE_GROUP"
    fi

    echo "Installing Java $JDK_MAJOR on target (if needed)..."
    remote_install_java "$JDK_MAJOR"
    echo "Setting JAVA_HOME on target..."
    remote_setup_java_home "$JDK_MAJOR"

    echo "Preparing directories on target..."
    remote_prepare_dirs "$INSTALL_BASE" "$DATA_BASE"

    echo "Downloading and extracting Ozone $VERSION on target..."
    remote_download_and_extract "$VERSION" "$INSTALL_BASE"

    echo "Setting OZONE_HOME on target..."
    remote_setup_ozone_home "$INSTALL_BASE"

    local omh sch
    omh="$TARGET_HOST"; sch="$TARGET_HOST"
    echo "Generating config templates on target..."
    remote_generate_configs "$INSTALL_BASE" "$DATA_BASE" "$omh" "$sch"

    if [[ "$START_AFTER_INSTALL" == "yes" ]]; then
      echo "Initializing and starting Ozone (single-node) on target..."
      single_node_init_and_start "$INSTALL_BASE" "$DATA_BASE"
      END_TS=$(date +%s)
      END_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
      echo "${SEPARATOR}"
      echo "Running sample test from <<$TARGET_HOST>>..."
      run_smoke_on_host "${TARGET_HOST}" "${CLUSTER_MODE}"
      echo "${SEPARATOR}"
    else
      echo "Skipping service start. Use --start to auto-start after install."
    fi

    cat <<EON

Done (Single node mode - configs & binaries distributed).

Initialization and start (manual steps) if you didn't use --start:
  ozone scm --init
  ozone --daemon start scm
  ozone om --init
  ozone --daemon start om
  ozone --daemon start datanode

Configs generated at:
  $INSTALL_BASE/current/etc/hadoop/ozone-site.xml
  $INSTALL_BASE/current/etc/hadoop/core-site.xml
  $INSTALL_BASE/current/etc/hadoop/ozone-env.sh
  $INSTALL_BASE/current/etc/hadoop/ozone-hosts.yaml

EON
  elif [[ "$CLUSTER_MODE" == "ha" ]]; then
    # ROLE_FILE is just a filename, derive full path from CONFIG_DIR
    local role_file_path="$CONFIG_DIR/$ROLE_FILE"
    if [[ ! -f "$role_file_path" ]]; then
      echo "Error: role file not found: $role_file_path (derived from --config-dir: $CONFIG_DIR and --role-file: $ROLE_FILE)" >&2
      exit 1
    fi

    # Build CLI host list from -H comma list (optional but recommended)
    local -a CLI_HOSTS=()
    if [[ -n "$HOSTS_ARG_RAW" ]]; then
      IFS=',' read -r -a CLI_HOSTS <<<"$HOSTS_ARG_RAW"
    fi
    local cli_len
    cli_len=${#CLI_HOSTS[@]}

    # Parse role file (very simple YAML list parser) and map host tokens (host1, host2, ...) to CLI hosts
    local line section entry idx
    while IFS= read -r line || [[ -n "$line" ]]; do
      if [[ "$line" == om:* ]]; then
        section="om"
      elif [[ "$line" == scm:* ]]; then
        section="scm"
      elif [[ "$line" == datanodes:* ]]; then
        section="dn"
      elif [[ "$line" == recon:* ]]; then
        section="recon"
      elif [[ "$line" =~ ^[[:space:]]*-[[:space:]]+ ]]; then
        entry="${line#*- }"
        # Map token hostN to CLI host at index N-1 if provided
        if [[ "$entry" =~ ^host([0-9]+)$ ]]; then
          idx="${BASH_REMATCH[1]}"; idx=$((idx-1))
          if [[ $cli_len -gt 0 && $idx -ge 0 && $idx -lt $cli_len ]]; then
            entry="${CLI_HOSTS[$idx]}"
          fi
        fi
        case "$section" in
          om) OM_HOSTS+=("$entry") ;;
          scm) SCM_HOSTS+=("$entry") ;;
          dn) DN_HOSTS+=("$entry") ;;
          recon) RECON_HOSTS+=("$entry") ;;
        esac
      fi
    done < "$role_file_path"

    # Fallback mapping from CLI hosts if roles were missing
    if [[ ${#CLI_HOSTS[@]} -gt 0 ]]; then
      if [[ ${#OM_HOSTS[@]} -lt 1 ]]; then OM_HOSTS=("${CLI_HOSTS[@]:0:3}"); fi
      if [[ ${#SCM_HOSTS[@]} -lt 1 ]]; then SCM_HOSTS=("${CLI_HOSTS[@]:0:3}"); fi
      if [[ ${#DN_HOSTS[@]} -lt 1 ]]; then DN_HOSTS=("${CLI_HOSTS[@]}"); fi
    fi

    if [[ ${#OM_HOSTS[@]} -lt 1 || ${#SCM_HOSTS[@]} -lt 1 || ${#DN_HOSTS[@]} -lt 1 ]]; then
      echo "Error: role file must define at least one 'om', one 'scm', and one 'datanodes' entry." >&2
      exit 1
    fi

    # Unique host list (prefer -H comma-delimited if provided; else derive from mapped roles)
    local all_hosts
    if [[ ${#CLI_HOSTS[@]} -gt 0 ]]; then
      all_hosts=("${CLI_HOSTS[@]}")
    else
      local uhout
      uhout="$(unique_hosts "${OM_HOSTS[@]}" "${SCM_HOSTS[@]}" "${DN_HOSTS[@]}" "${RECON_HOSTS[@]:-}")"
      IFS=$'\n' read -r -a all_hosts <<< "$uhout"
    fi
    # Enforce minimum 3 hosts for HA
    if [[ ${#all_hosts[@]} -lt 3 ]]; then
      echo "Error: HA mode requires at least 3 hosts (got ${#all_hosts[@]}). Provide more via --role-file or -H host1,host2,host3" >&2
      exit 1
    fi
    # Place datanodes on all hosts
    DN_HOSTS=("${all_hosts[@]}")

    # Ensure passwordless SSH from local to all nodes (parallel)
    echo "Configuring passwordless SSH to all nodes (parallel)..."
    for h in "${all_hosts[@]}"; do
      ensure_passwordless_ssh_to_host "$h" &
    done
    wait

    # Generate a runtime SSH keypair and deploy to all nodes so they can SSH each other passwordlessly
    echo "Generating runtime SSH keypair for cluster and deploying to all nodes..."
    local _tmp_dir _key_priv _key_pub
    _tmp_dir="$(mktemp -d)"
    _key_priv="$_tmp_dir/ozone_cluster_id_ed25519"
    _key_pub="${_key_priv}.pub"
    ssh-keygen -t ed25519 -N "" -f "$_key_priv" >/dev/null 2>&1
    for h in "${all_hosts[@]}"; do
      (
        parse_host "$h"
        install_shared_ssh_key "$_key_priv" "$_key_pub"
      ) &
    done
    wait
    rm -rf "$_tmp_dir"

    # Install to all
    local om_csv scm_csv dn_csv dn_csv_clean
    om_csv="$(join_by , "${OM_HOSTS[@]}")"
    scm_csv="$(join_by , "${SCM_HOSTS[@]}")"
    dn_csv="$(join_by , "${DN_HOSTS[@]}")"
    # Build cleaned DN hostnames (strip user and :port for workers)
    declare -a DN_HOSTS_CLEAN=()
    for h in "${DN_HOSTS[@]}"; do
      # strip user@
      h="${h#*@}"
      # strip :port
      h="${h%%:*}"
      DN_HOSTS_CLEAN+=("$h")
    done
    dn_csv_clean="$(join_by , "${DN_HOSTS_CLEAN[@]}")"

    for current_host in "${all_hosts[@]}"; do
      (
        parse_host "$current_host"
        if [[ "$CLEAN_MODE" == "yes" ]]; then
          echo "[$current_host] Cleaning install and data dirs..."
          remote_cleanup_dirs "$INSTALL_BASE" "$DATA_BASE"
        fi
        if [[ -n "$SERVICE_USER" ]]; then
          echo "[$current_host] Creating service user $SERVICE_USER with group $SERVICE_GROUP..."
          remote_create_service_user "$SERVICE_USER" "$SERVICE_GROUP"
        fi
        echo "[$current_host] Installing JDK $JDK_MAJOR (if needed)..."
        remote_install_java "$JDK_MAJOR"
        echo "[$current_host] Setting JAVA_HOME..."
        remote_setup_java_home "$JDK_MAJOR"
        echo "[$current_host] Preparing directories..."
        remote_prepare_dirs "$INSTALL_BASE" "$DATA_BASE"
        echo "[$current_host] Downloading Ozone $VERSION..."
        remote_download_and_extract "$VERSION" "$INSTALL_BASE"
        echo "[$current_host] Setting environment..."
        remote_setup_ozone_home "$INSTALL_BASE"
        echo "[$current_host] Generating HA configs..."
        remote_generate_configs_ha "$INSTALL_BASE" "$DATA_BASE" "$om_csv" "$scm_csv" "$dn_csv"
        # Also generate workers file to drive start-ozone.sh
        ssh_run "printf '%s' \"$dn_csv_clean\" | tr ',' '\n' > \"$INSTALL_BASE/current/etc/hadoop/workers\""
      ) &
    done
    wait

    if [[ "$START_AFTER_INSTALL" == "yes" ]]; then
      echo "Initializing and starting Ozone (HA) across nodes..."
      ha_init_and_start "$INSTALL_BASE" "$DATA_BASE"
      END_TS=$(date +%s)
      END_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
      echo "${SEPARATOR}"
      echo "Running sample test from <<${OM_HOSTS[0]}>>..."
      run_smoke_on_host "${OM_HOSTS[0]}" "${CLUSTER_MODE}"
      echo "${SEPARATOR}"
    else
      echo "Skipping HA service start. Use --start to auto-start after install."
    fi

    cat <<EON

Done (HA mode - configs & binaries distributed to all nodes).

Initialization and start (manual steps, follow HA docs) if you didn't use --start:
  - SCM HA: initialize first SCM, then bootstrap others, then start SCMs
  - OM HA: initialize first OM service, then join others, then start OMs
  - Finally start datanodes (or use start-ozone.sh after HA init)

Configs generated per node:
  $INSTALL_BASE/current/etc/hadoop/ozone-site.xml (HA settings)
  $INSTALL_BASE/current/etc/hadoop/core-site.xml (fs.defaultFS=ofs://omservice)
  $INSTALL_BASE/current/etc/hadoop/ozone-env.sh (Ozone environment variables)
  $INSTALL_BASE/current/etc/hadoop/ozone-hosts.yaml (roles)
  $INSTALL_BASE/current/etc/hadoop/workers (datanodes list)

EON
  else
    echo "Error: unknown --cluster-mode '$CLUSTER_MODE' (use 'single' or 'ha')." >&2
    exit 1
  fi

  # Record end time and total duration
  if [[ "$START_AFTER_INSTALL" == "no" ]]; then
    END_TS=$(date +%s)
    END_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  fi
  TOTAL_S=$((END_TS - START_TS))
  M=$(((TOTAL_S%3600)/60))
  S=$((TOTAL_S%60))
  printf "%s\n" "${SEPARATOR}"
  printf "Completed at: %s\n" "$END_ISO"
  printf "Total execution time: %02d:%02d >> %d seconds\n" "$M" "$S" "$TOTAL_S"
  printf "%s\n" "${SEPARATOR}"
}

# Load common_lib.sh if present (remote helpers)
if [[ -f "$SCRIPT_DIR/common_lib.sh" ]]; then
  # shellcheck disable=SC1090
  . "$SCRIPT_DIR/common_lib.sh"
fi

main "$@"

# ToDo: Add Recon to the role file
# ToDo: Add S3Gateway to the role file
# ToDo: Add Httpfs to the role file
# ToDo: Add Logger to a file
# ToDo: Kerberos Support
# ToDo: TDE Support
# ToDo: TLS Support