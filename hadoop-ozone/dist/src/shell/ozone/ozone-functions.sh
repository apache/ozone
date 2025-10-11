#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# we need to declare this globally as an array, which can only
# be done outside of a function
declare -a OZONE_SUBCMD_USAGE
declare -a OZONE_OPTION_USAGE
declare -a OZONE_SUBCMD_USAGE_TYPES

## @description  Print a message to stderr
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function ozone_error
{
  echo "$*" 1>&2
}

## @description  Print a message to stderr if --debug is turned on
## @audience     public
## @stability    stable
## @replaceable  no
## @param        string
function ozone_debug
{
  if [[ -n "${OZONE_SHELL_SCRIPT_DEBUG}" ]]; then
    echo "DEBUG: $*" 1>&2
  fi
}

## @description Displays usage text for the '--validate' option
## @audience private
## @stability evolving
## @replaceable yes
function ozone_validate_classpath_usage
{
  description=$'The --validate flag validates if all jars as indicated in the corresponding OZONE_RUN_ARTIFACT_NAME classpath file are present\n\n'
  usage_text=$'Usage I: ozone --validate classpath <ARTIFACTNAME>\nUsage II: ozone --validate [OPTIONS] --daemon start|status|stop csi|datanode|om|recon|s3g|scm\n\n'
  options=$'  OPTIONS is none or any of:\n\ncontinue\tcommand execution shall continue even if validation fails'
  ozone_error "${description}${usage_text}${options}"
  exit 1
}

## @description Validates if all jars as indicated in the corresponding OZONE_RUN_ARTIFACT_NAME classpath file are present
## @audience private
## @stability evolving
## @replaceable yes
function ozone_validate_classpath
{
  local OZONE_OPTION_DAEMON
  [[ "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" == true && "${OZONE_DAEMON_MODE}" =~ ^st(art|op|atus)$ ]] &&
  OZONE_OPTION_DAEMON=true || OZONE_OPTION_DAEMON=false

  if [[ "${OZONE_VALIDATE_CLASSPATH}" == true ]]; then
    if [[ ( "${OZONE_VALIDATE_FAIL_ON_MISSING_JARS}" == true && ( "${OZONE_OPTION_DAEMON}" == true ||
          "${OZONE_SUBCMD}" == classpath ) ) || ( "${OZONE_VALIDATE_FAIL_ON_MISSING_JARS}" == false &&
          "${OZONE_OPTION_DAEMON}" == true ) ]]; then
      ozone_validate_classpath_util
    else
      ozone_validate_classpath_usage
    fi
  fi
}

## @description  Given a filename or dir, return the absolute version of it
## @description  This works as an alternative to readlink, which isn't
## @description  portable.
## @audience     public
## @stability    stable
## @param        fsobj
## @replaceable  no
## @return       0 success
## @return       1 failure
## @return       stdout abspath
function ozone_abs
{
  declare obj=$1
  declare dir
  declare fn
  declare dirret

  if [[ ! -e ${obj} ]]; then
    return 1
  elif [[ -d ${obj} ]]; then
    dir=${obj}
  else
    dir=$(dirname -- "${obj}")
    fn=$(basename -- "${obj}")
    fn="/${fn}"
  fi

  dir=$(cd -P -- "${dir}" >/dev/null 2>/dev/null && pwd -P)
  dirret=$?
  if [[ ${dirret} = 0 ]]; then
    echo "${dir}${fn}"
    return 0
  fi
  return 1
}

## @description  Given variable $1 delete $2 from it
## @audience     public
## @stability    stable
## @replaceable  no
function ozone_delete_entry
{
  if [[ ${!1} =~ \ ${2}\  ]] ; then
    ozone_debug "Removing ${2} from ${1}"
    eval "${1}"=\""${!1// ${2} }"\"
  fi
}

## @description  Given variable $1 add $2 to it
## @audience     public
## @stability    stable
## @replaceable  no
function ozone_add_entry
{
  if [[ ! ${!1} =~ \ ${2}\  ]] ; then
    ozone_debug "Adding ${2} to ${1}"
    #shellcheck disable=SC2140
    eval "${1}"=\""${!1} ${2} "\"
  fi
}

## @description  Given variable $1 determine if $2 is in it
## @audience     public
## @stability    stable
## @replaceable  no
## @return       0 = yes, 1 = no
function ozone_verify_entry
{
  # this unfortunately can't really be tested by bats. :(
  # so if this changes, be aware that unit tests effectively
  # do this function in them
  [[ ${!1} =~ \ ${2}\  ]]
}

## @description  Check if an array has a given value
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        element
## @param        array
## @returns      0 = yes
## @returns      1 = no
function ozone_array_contains
{
  declare element=$1
  shift
  declare val

  if [[ "$#" -eq 0 ]]; then
    return 1
  fi

  for val in "${@}"; do
    if [[ "${val}" == "${element}" ]]; then
      return 0
    fi
  done
  return 1
}

## @description  Add the `appendstring` if `checkstring` is not
## @description  present in the given array
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        envvar
## @param        appendstring
function ozone_add_array_param
{
  declare arrname=$1
  declare add=$2

  declare arrref="${arrname}[@]"
  declare array=("${!arrref}")

  if ! ozone_array_contains "${add}" "${array[@]}"; then
    #shellcheck disable=SC1083,SC2086
    eval ${arrname}=\(\"\${array[@]}\" \"${add}\" \)
    ozone_debug "$1 accepted $2"
  else
    ozone_debug "$1 declined $2"
  fi
}

## @description  Sort an array (must not contain regexps)
## @description  present in the given array
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        arrayvar
function ozone_sort_array
{
  declare arrname=$1
  declare arrref="${arrname}[@]"
  declare array=("${!arrref}")
  declare oifs

  declare globstatus
  declare -a sa

  globstatus=$(set -o | grep noglob | awk '{print $NF}')

  set -f
  oifs=${IFS}

  # shellcheck disable=SC2034
  IFS=$'\n' sa=($(sort <<<"${array[*]}"))

  # shellcheck disable=SC1083
  eval "${arrname}"=\(\"\${sa[@]}\"\)

  IFS=${oifs}
  if [[ "${globstatus}" = off ]]; then
    set +f
  fi
}

## @description  Check if we are running with priv
## @description  by default, this implementation looks for
## @description  EUID=0.  For OSes that have true priv
## @description  separation, this should be something more complex
## @audience     private
## @stability    evolving
## @replaceable  yes
## @return       1 = no priv
## @return       0 = priv
function ozone_privilege_check
{
  [[ "${EUID}" = 0 ]]
}

## @description  Execute a command via su when running as root
## @description  if the given user is found or exit with
## @description  failure if not.
## @description  otherwise just run it.  (This is intended to
## @description  be used by the start-*/stop-* scripts.)
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        user
## @param        commandstring
## @return       exitstatus
function ozone_su
{
  declare user=$1
  shift

  if ozone_privilege_check; then
    if ozone_verify_user_resolves user; then
       su -l "${user}" -- "$@"
    else
      ozone_error "ERROR: Refusing to run as root: ${user} account is not found. Aborting."
      return 1
    fi
  else
    "$@"
  fi
}

## @description  Execute a command via su when running as root
## @description  with extra support for commands that might
## @description  legitimately start as root (e.g., datanode)
## @description  (This is intended to
## @description  be used by the start-*/stop-* scripts.)
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        user
## @param        commandstring
## @return       exitstatus
function ozone_uservar_su
{

  ## startup matrix:
  #
  # if $EUID != 0, then exec
  # if $EUID =0 then
  #    if ozone_subcmd_user is defined, call ozone_su to exec
  #    if ozone_subcmd_user is not defined, error
  #
  # For secure daemons, this means both the secure and insecure env vars need to be
  # defined.  e.g., OZONE_DATANODE_USER=root OZONE_DATANODE_SECURE_USER=ozone
  # This function will pick up the "normal" var, switch to that user, then
  # execute the command which will then pick up the "secure" version.
  #

  declare program=$1
  declare command=$2
  shift 2

  declare uprogram
  declare ucommand
  declare uvar
  declare svar

  if ozone_privilege_check; then
    uvar=$(ozone_build_custom_subcmd_var "${program}" "${command}" USER)

    svar=$(ozone_build_custom_subcmd_var "${program}" "${command}" SECURE_USER)

    if [[ -n "${!uvar}" ]]; then
      ozone_su "${!uvar}" "$@"
    elif [[ -n "${!svar}" ]]; then
      ## if we are here, then SECURE_USER with no USER defined
      ## we are already privileged, so just run the command and hope
      ## for the best
      "$@"
    else
      ozone_error "ERROR: Attempting to operate on ${program} ${command} as root"
      ozone_error "ERROR: but there is no ${uvar} defined. Aborting operation."
      return 1
    fi
  else
    "$@"
  fi
}

## @description  Add a subcommand to the usage output
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        subcommand
## @param        subcommandtype
## @param        subcommanddesc
function ozone_add_subcommand
{
  declare subcmd=$1
  declare subtype=$2
  declare text=$3

  ozone_debug "${subcmd} as a ${subtype}"

  ozone_add_array_param OZONE_SUBCMD_USAGE_TYPES "${subtype}"

  # done in this order so that sort works later
  OZONE_SUBCMD_USAGE[${OZONE_SUBCMD_USAGE_COUNTER}]="${subcmd}@${subtype}@${text}"
  ((OZONE_SUBCMD_USAGE_COUNTER=OZONE_SUBCMD_USAGE_COUNTER+1))
}

## @description  Add an option to the usage output
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        subcommand
## @param        subcommanddesc
function ozone_add_option
{
  local option=$1
  local text=$2

  OZONE_OPTION_USAGE[${OZONE_OPTION_USAGE_COUNTER}]="${option}@${text}"
  ((OZONE_OPTION_USAGE_COUNTER=OZONE_OPTION_USAGE_COUNTER+1))
}

## @description  Reset the usage information to blank
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_reset_usage
{
  OZONE_SUBCMD_USAGE=()
  OZONE_OPTION_USAGE=()
  OZONE_SUBCMD_USAGE_TYPES=()
  OZONE_SUBCMD_USAGE_COUNTER=0
  OZONE_OPTION_USAGE_COUNTER=0
}

## @description  Print a screen-size aware two-column output
## @description  if reqtype is not null, only print those requested
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        reqtype
## @param        array
function ozone_generic_columnprinter
{
  declare reqtype=$1
  shift
  declare -a input=("$@")
  declare -i i=0
  declare -i counter=0
  declare line
  declare text
  declare option
  declare giventext
  declare -i maxoptsize
  declare -i foldsize
  declare -a tmpa
  declare numcols
  declare brup

  if [[ -n "${COLUMNS}" ]]; then
    numcols=${COLUMNS}
  else
    numcols=$(tput cols) 2>/dev/null
    COLUMNS=${numcols}
  fi

  if [[ -z "${numcols}"
     || ! "${numcols}" =~ ^[0-9]+$ ]]; then
    numcols=75
  else
    ((numcols=numcols-5))
  fi

  while read -r line; do
    tmpa[${counter}]=${line}
    ((counter=counter+1))
    IFS='@' read -ra brup <<< "${line}"
    option="${brup[0]}"
    if [[ ${#option} -gt ${maxoptsize} ]]; then
      maxoptsize=${#option}
    fi
  done < <(for text in "${input[@]}"; do
    echo "${text}"
  done | sort)

  i=0
  ((foldsize=numcols-maxoptsize))

  until [[ $i -eq ${#tmpa[@]} ]]; do
    IFS='@' read -ra brup <<< "${tmpa[$i]}"

    option="${brup[0]}"
    cmdtype="${brup[1]}"
    giventext="${brup[2]}"

    if [[ -n "${reqtype}" ]]; then
      if [[ "${cmdtype}" != "${reqtype}" ]]; then
        ((i=i+1))
        continue
      fi
    fi

    if [[ -z "${giventext}" ]]; then
      giventext=${cmdtype}
    fi

    while read -r line; do
      printf "%-${maxoptsize}s   %-s\n" "${option}" "${line}"
      option=" "
    done < <(echo "${giventext}"| fold -s -w ${foldsize})
    ((i=i+1))
  done
}

## @description  generate standard usage output
## @description  and optionally takes a class
## @audience     private
## @stability    evolving
## @replaceable  no
## @param        execname
## @param        true|false
## @param        [text to use in place of SUBCOMMAND]
function ozone_generate_usage
{
  declare cmd=$1
  declare takesclass=$2
  declare subcmdtext=${3:-"SUBCOMMAND"}
  declare haveoptions
  declare optstring
  declare havesubs
  declare subcmdstring
  declare cmdtype

  cmd=${cmd##*/}

  if [[ -n "${OZONE_OPTION_USAGE_COUNTER}"
        && "${OZONE_OPTION_USAGE_COUNTER}" -gt 0 ]]; then
    haveoptions=true
    optstring=" [OPTIONS]"
  fi

  if [[ -n "${OZONE_SUBCMD_USAGE_COUNTER}"
        && "${OZONE_SUBCMD_USAGE_COUNTER}" -gt 0 ]]; then
    havesubs=true
    subcmdstring=" ${subcmdtext} [${subcmdtext} OPTIONS]"
  fi

  echo "Usage: ${cmd}${optstring}${subcmdstring}"
  if [[ ${takesclass} = true ]]; then
    echo " or    ${cmd}${optstring} CLASSNAME [CLASSNAME OPTIONS]"
    echo "  where CLASSNAME is a user-provided Java class"
  fi

  if [[ "${haveoptions}" = true ]]; then
    echo ""
    echo "  OPTIONS is none or any of:"
    echo ""

    ozone_generic_columnprinter "" "${OZONE_OPTION_USAGE[@]}"
  fi

  if [[ "${havesubs}" = true ]]; then
    echo ""
    echo "  ${subcmdtext} is one of:"
    echo ""

    if [[ "${#OZONE_SUBCMD_USAGE_TYPES[@]}" -gt 0 ]]; then

      ozone_sort_array OZONE_SUBCMD_USAGE_TYPES
      for subtype in "${OZONE_SUBCMD_USAGE_TYPES[@]}"; do
        #shellcheck disable=SC2086
        cmdtype="$(tr '[:lower:]' '[:upper:]' <<< ${subtype:0:1})${subtype:1}"
        printf "\n    %s Commands:\n\n" "${cmdtype}"
        ozone_generic_columnprinter "${subtype}" "${OZONE_SUBCMD_USAGE[@]}"
      done
    else
      ozone_generic_columnprinter "" "${OZONE_SUBCMD_USAGE[@]}"
    fi
    echo ""
    echo "${subcmdtext} may print help when invoked w/o parameters or with -h."
  fi
}

## @description  Print value of `var` if it is declared.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        var
function ozone_using_envvar
{
  local var=$1
  local val=${!var}

  if [[ -n "${!var*}" ]]; then
    ozone_debug "${var} = ${!var}"
  fi
}

## @description  Create the directory 'dir'.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        dir
function ozone_mkdir
{
  local dir=$1

  if [[ ! -w "${dir}" ]] && [[ ! -d "${dir}" ]]; then
    ozone_error "WARNING: ${dir} does not exist. Creating."
    if ! mkdir -p "${dir}"; then
      ozone_error "ERROR: Unable to create ${dir}. Aborting."
      exit 1
    fi
  fi
}

## @description Locate Ozone's libexec dir
## @audience private
## @stability evolving
## @replaceable no
function ozone_locate_libexec() {
  local _this libexec
  _this="${BASH_SOURCE-$0}"

  if [[ -n "${OZONE_LIBEXEC_DIR}" ]] && ozone_verify_libexec "${OZONE_LIBEXEC_DIR}"; then
    return 0
  fi

  if [[ -n "${OZONE_HOME}" ]] && ozone_verify_libexec "${OZONE_HOME}/libexec"; then
    OZONE_LIBEXEC_DIR="${OZONE_HOME}/libexec"
    ozone_using_envvar OZONE_LIBEXEC_DIR
    return 0
  fi

  libexec=$(ozone_abs $(cd -P -- "$(dirname -- "${_this}")" >/dev/null && pwd -P))
  if [[ -n "${dir}" ]] && ozone_verify_libexec "${libexec}"; then
    OZONE_LIBEXEC_DIR="${libexec}"
    ozone_using_envvar OZONE_LIBEXEC_DIR
    OZONE_HOME=$(ozone_abs ${OZONE_LIBEXEC_DIR}/..)
    ozone_using_envvar OZONE_HOME
    return 0
  fi

  return 1
}

## @description Check if ozone-config.sh exists in the given directory
## @audience private
## @stability stable
## @replaceable no
function ozone_verify_libexec() {
  local candidate=$1

  if [[ -n "${candidate}" ]] && [[ -e "${candidate}/ozone-config.sh" ]]; then
    ozone_debug "Found ozone-config.sh in ${candidate}"
  else
    ozone_debug "No ozone-config.sh in ${candidate}"
    return 1
  fi
}

## @description  Bootstraps the shell environment
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_bootstrap
{
  if [[ "${OZONE_BOOTSTRAPPED}" == "true" ]]; then
    ozone_debug "Already bootstrapped Ozone"
    return
  fi

  ozone_deprecate_envvar HADOOP_OZONE_HOME OZONE_HOME
  ozone_deprecate_hadoop_vars HOME LIBEXEC_DIR OPTS OS_TYPE

  if ! ozone_locate_libexec; then
    ozone_error "Please set OZONE_HOME or OZONE_LIBEXEC_DIR.  Exiting."
    exit 1
  fi

  local default_prefix
  default_prefix=$(cd -P -- "${OZONE_LIBEXEC_DIR}/.." >/dev/null && pwd -P)
  OZONE_HOME=${OZONE_HOME:-$default_prefix}
  export OZONE_HOME

  export HDDS_LIB_JARS_DIR="${OZONE_HOME}/share/ozone/lib"

  export OZONE_OS_TYPE=${OZONE_OS_TYPE:-$(uname -s)}
  export OZONE_OPTS=${OZONE_OPTS:-"-Djava.net.preferIPv4Stack=true"}
  ozone_using_envvar OZONE_OPTS
  JSVC_HOME=${JSVC_HOME:-"/usr/bin"}

  # reset variables related to command execution
  ozone_reset_usage
  OZONE_REEXECED_CMD=false
  OZONE_SUBCMD_SECURESERVICE=false
  OZONE_SUBCMD_SUPPORTDAEMONIZATION=false
  OZONE_VALIDATE_CLASSPATH=false
  OZONE_VALIDATE_FAIL_ON_MISSING_JARS=true

  ozone_set_deprecated_var HADOOP_OZONE_HOME OZONE_HOME
  ozone_set_deprecated_hadoop_vars HOME LIBEXEC_DIR OPTS OS_TYPE

  OZONE_BOOTSTRAPPED=true
}

## @description  Locate Ozone's configuration directory
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_find_confdir
{
  ozone_deprecate_envvar HADOOP_CONF_DIR OZONE_CONF_DIR

  local conf_dir=etc/hadoop

  if [[ -n "${OZONE_CONF_DIR}" ]] && ozone_verify_confdir "${OZONE_CONF_DIR}"; then
    : # OK
  elif [[ -n "${OZONE_HOME}" ]] && ozone_verify_confdir "${OZONE_HOME}/${conf_dir}"; then
    OZONE_CONF_DIR="${OZONE_HOME}/${conf_dir}"
  elif [[ -n "${OZONE_LIBEXEC_DIR}" ]] && ozone_verify_confdir "${OZONE_LIBEXEC_DIR}/../${conf_dir}"; then
    OZONE_CONF_DIR=$(ozone_abs "${OZONE_LIBEXEC_DIR}/../${conf_dir}")
  else
    OZONE_CONF_DIR="${OZONE_HOME}/${conf_dir}" # not verified yet
    ozone_error "WARNING: OZONE_CONF_DIR not defined and cannot be found, setting in OZONE_HOME: ${OZONE_CONF_DIR}."
  fi

  export OZONE_CONF_DIR
  ozone_using_envvar OZONE_CONF_DIR
  ozone_set_deprecated_var HADOOP_CONF_DIR OZONE_CONF_DIR
}

## @description  Validate ${OZONE_CONF_DIR}
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       will exit on failure conditions
function ozone_verify_confdir
{
  # Check ozone-site.xml by default.
  [[ -f "${1:?ozone_verify_confir requires parameter}/ozone-site.xml" ]]
}

## @description  Import the ozone-env.sh settings
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_exec_ozoneenv
{
  if [[ -z "${OZONE_ENV_PROCESSED}" ]]; then
    if [[ -f "${OZONE_CONF_DIR}/ozone-env.sh" ]]; then
      export OZONE_ENV_PROCESSED=true
      # shellcheck source=./hadoop-hdds/common/src/main/conf/ozone-env.sh
      . "${OZONE_CONF_DIR}/ozone-env.sh"
    fi
  fi
}

## @description  Import the replaced functions
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_exec_userfuncs
{
  if [[ -e "${OZONE_CONF_DIR}/ozone-user-functions.sh" ]]; then
    # shellcheck disable=SC1090
    . "${OZONE_CONF_DIR}/ozone-user-functions.sh"
  fi
}

## @description  Read the user's settings.  This provides for users to
## @description  override and/or append ozone-env.sh. It is not meant
## @description  as a complete system override.
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_exec_user_env
{
  if [[ -f "${HOME}/.ozone-env" ]]; then
    ozone_debug "Applying the user's .ozone-env"
    # shellcheck disable=SC1090
    . "${HOME}/.ozone-env"
  fi
}

## @description  Read the user's settings.  This provides for users to
## @description  run Shell API after system bootstrap
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_exec_ozonerc
{
  if [[ -f "${HOME}/.ozonerc" ]]; then
    ozone_debug "Applying the user's .ozonerc"
    # shellcheck disable=SC1090
    . "${HOME}/.ozonerc"
  fi
}

## @description  Import shellprofile.d content
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_import_shellprofiles
{
  local i
  local files1
  local files2

  if [[ -d "${OZONE_LIBEXEC_DIR}/shellprofile.d" ]]; then
    files1=(${OZONE_LIBEXEC_DIR}/shellprofile.d/*.sh)
    ozone_debug "shellprofiles: ${files1[*]}"
  else
    ozone_error "WARNING: ${OZONE_LIBEXEC_DIR}/shellprofile.d doesn't exist. Functionality may not work."
  fi

  if [[ -d "${OZONE_CONF_DIR}/shellprofile.d" ]]; then
    files2=(${OZONE_CONF_DIR}/shellprofile.d/*.sh)
  fi

  for i in "${files1[@]}" "${files2[@]}"
  do
    if [[ -n "${i}"
      && -f "${i}" ]]; then
      ozone_debug "Profiles: importing ${i}"
      # shellcheck disable=SC1090
      . "${i}"
    fi
  done
}

## @description  Initialize the registered shell profiles
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_shellprofiles_init
{
  local i

  for i in ${OZONE_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_init >/dev/null ; then
       ozone_debug "Profiles: ${i} init"
       # shellcheck disable=SC2086
       _${i}_hadoop_init
    fi
  done
}

## @description  Apply the shell profile classpath additions
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_shellprofiles_classpath
{
  local i

  for i in ${OZONE_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_classpath >/dev/null ; then
       ozone_debug "Profiles: ${i} classpath"
       # shellcheck disable=SC2086
       _${i}_hadoop_classpath
    fi
  done
}

## @description  Apply the shell profile native library additions
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_shellprofiles_nativelib
{
  local i

  for i in ${OZONE_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_nativelib >/dev/null ; then
       ozone_debug "Profiles: ${i} nativelib"
       # shellcheck disable=SC2086
       _${i}_hadoop_nativelib
    fi
  done
}

## @description  Apply the shell profile final configuration
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_shellprofiles_finalize
{
  local i

  for i in ${OZONE_SHELL_PROFILES}
  do
    if declare -F _${i}_hadoop_finalize >/dev/null ; then
       ozone_debug "Profiles: ${i} finalize"
       # shellcheck disable=SC2086
       _${i}_hadoop_finalize
    fi
  done
}

## @description  Initialize the shell environment, now that
## @description  user settings have been imported
## @audience     private
## @stability    evolving
## @replaceable  no
function ozone_basic_init
{
  # Some of these are also set in ozone-env.sh.
  # we still set them here just in case ozone-env.sh is
  # broken in some way, set up defaults, etc.
  #
  # but it is important to note that if you update these
  # you also need to update ozone-env.sh as well!!!

  CLASSPATH=""
  ozone_debug "Initialize CLASSPATH"

  ozone_deprecate_hadoop_vars DAEMON_ROOT_LOGGER LOGFILE LOGLEVEL LOG_DIR ROOT_LOGGER SECURE_LOG_DIR SECURITY_LOGGER \
    IDENT_STRING NICENESS POLICYFILE PID_DIR SECURE_PID_DIR SSH_OPTS SSH_PARALLEL STOP_TIMEOUT

  # default policy file for service-level authorization
  OZONE_POLICYFILE=${OZONE_POLICYFILE:-"hadoop-policy.xml"}

  OZONE_ORIGINAL_LOGLEVEL="${OZONE_LOGLEVEL:-}"
  OZONE_ORIGINAL_ROOT_LOGGER="${OZONE_ROOT_LOGGER:-}"

  # if for some reason the shell doesn't have $USER defined
  # (e.g., ssh'd in to execute a command)
  # let's get the effective username and use that
  USER=${USER:-$(id -nu)}
  OZONE_IDENT_STRING=${OZONE_IDENT_STRING:-$USER}
  OZONE_LOG_DIR=${OZONE_LOG_DIR:-"${OZONE_HOME}/logs"}
  OZONE_LOGFILE=${OZONE_LOGFILE:-ozone.log}
  OZONE_LOGLEVEL=${OZONE_LOGLEVEL:-INFO}
  OZONE_NICENESS=${OZONE_NICENESS:-0}
  OZONE_STOP_TIMEOUT=${OZONE_STOP_TIMEOUT:-60}
  OZONE_PID_DIR=${OZONE_PID_DIR:-/tmp}
  OZONE_ROOT_LOGGER=${OZONE_ROOT_LOGGER:-${OZONE_LOGLEVEL},console}
  OZONE_DAEMON_ROOT_LOGGER=${OZONE_DAEMON_ROOT_LOGGER:-${OZONE_LOGLEVEL},RFA}
  OZONE_SECURITY_LOGGER=${OZONE_SECURITY_LOGGER:-INFO,NullAppender}
  OZONE_SSH_OPTS=${OZONE_SSH_OPTS-"-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"}
  OZONE_SECURE_LOG_DIR=${OZONE_SECURE_LOG_DIR:-${OZONE_LOG_DIR}}
  OZONE_SECURE_PID_DIR=${OZONE_SECURE_PID_DIR:-${OZONE_PID_DIR}}
  OZONE_SSH_PARALLEL=${OZONE_SSH_PARALLEL:-10}

  ozone_set_deprecated_hadoop_vars DAEMON_ROOT_LOGGER LOGFILE LOGLEVEL LOG_DIR ROOT_LOGGER SECURE_LOG_DIR SECURITY_LOGGER \
    IDENT_STRING NICENESS POLICYFILE PID_DIR SECURE_PID_DIR SSH_OPTS SSH_PARALLEL STOP_TIMEOUT
}

## @description  Set the worker support information to the contents
## @description  of `filename`
## @audience     public
## @stability    stable
## @replaceable  no
## @param        filename
## @return       will exit if file does not exist
function ozone_populate_workers_file
{
  local workersfile=$1
  shift
  if [[ -f "${workersfile}" ]]; then
    OZONE_WORKERS="${workersfile}"
  elif [[ -f "${OZONE_CONF_DIR}/${workersfile}" ]]; then
    OZONE_WORKERS="${OZONE_CONF_DIR}/${workersfile}"
  else
    ozone_error "ERROR: Cannot find hosts file \"${workersfile}\""
    ozone_exit_with_usage 1
  fi
}

## @description  Rotates the given `file` until `number` of
## @description  files exist.
## @audience     public
## @stability    stable
## @replaceable  no
## @param        filename
## @param        [number]
## @return       $? will contain last mv's return value
function ozone_rotate_log
{
  #
  # Users are likely to replace this one for something
  # that gzips or uses dates or who knows what.
  #
  # be aware that &1 and &2 might go through here
  # so don't do anything too crazy...
  #
  local log=$1;
  local num=${2:-5};

  if [[ -f "${log}" ]]; then # rotate logs
    while [[ ${num} -gt 1 ]]; do
      #shellcheck disable=SC2086
      let prev=${num}-1
      if [[ -f "${log}.${prev}" ]]; then
        mv "${log}.${prev}" "${log}.${num}"
      fi
      num=${prev}
    done
    mv "${log}" "${log}.${num}"
  fi
}

## @description  Via ssh, log into `hostname` and run `command`
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        hostname
## @param        command
## @param        [...]
function ozone_actual_ssh
{
  # we are passing this function to xargs
  # should get hostname followed by rest of command line
  local worker=$1
  shift

  # shellcheck disable=SC2086
  ssh ${OZONE_SSH_OPTS} ${worker} $"${@// /\\ }" 2>&1 | sed "s/^/$worker: /"
}

## @description  Connect to ${OZONE_WORKERS} or ${OZONE_WORKER_NAMES}
## @description  and execute command.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        [...]
function ozone_connect_to_hosts
{
  # shellcheck disable=SC2124
  local params="$@"
  local worker_file
  local tmp_worker_names

  ozone_deprecate_hadoop_vars WORKERS WORKER_NAMES

  #
  # ssh (or whatever) to a host
  #
  # User can specify hostnames or a file where the hostnames are (not both)
  if [[ -n "${OZONE_WORKERS}" && -n "${OZONE_WORKER_NAMES}" ]] ; then
    ozone_error "ERROR: Both OZONE_WORKERS and OZONE_WORKER_NAMES were defined. Aborting."
    exit 1
  elif [[ -z "${OZONE_WORKER_NAMES}" ]]; then
    if [[ -n "${OZONE_WORKERS}" ]]; then
      worker_file=${OZONE_WORKERS}
    elif [[ -f "${OZONE_CONF_DIR}/workers" ]]; then
      worker_file=${OZONE_CONF_DIR}/workers
    fi
  fi

  # if pdsh is available, let's use it.  otherwise default
  # to a loop around ssh.  (ugh)
  if [[ -e '/usr/bin/pdsh' ]]; then
    if [[ -z "${OZONE_WORKER_NAMES}" ]] ; then
      # if we were given a file, just let pdsh deal with it.
      # shellcheck disable=SC2086
      PDSH_SSH_ARGS_APPEND="${OZONE_SSH_OPTS}" pdsh \
      -f "${OZONE_SSH_PARALLEL}" -w ^"${worker_file}" $"${@// /\\ }" 2>&1
    else
      # no spaces allowed in the pdsh arg host list
      # shellcheck disable=SC2086
      tmp_worker_names=$(echo ${OZONE_WORKER_NAMES} | tr -s ' ' ,)
      PDSH_SSH_ARGS_APPEND="${OZONE_SSH_OPTS}" pdsh \
        -f "${OZONE_SSH_PARALLEL}" \
        -w "${tmp_worker_names}" $"${@// /\\ }" 2>&1
    fi
  else
    if [[ -z "${OZONE_WORKER_NAMES}" ]]; then
      OZONE_WORKER_NAMES=$(sed 's/#.*$//;/^$/d' "${worker_file}")
    fi
    ozone_connect_to_hosts_without_pdsh "${params}"
  fi
}

## @description  Connect to ${OZONE_WORKER_NAMES} and execute command
## @description  under the environment which does not support pdsh.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        [...]
function ozone_connect_to_hosts_without_pdsh
{
  # shellcheck disable=SC2124
  local params="$@"
  local workers=(${OZONE_WORKER_NAMES})
  for (( i = 0; i < ${#workers[@]}; i++ ))
  do
    if (( i != 0 && i % OZONE_SSH_PARALLEL == 0 )); then
      wait
    fi
    # shellcheck disable=SC2086
    ozone_actual_ssh "${workers[$i]}" ${params} &
  done
  wait
}

## @description  Utility routine to handle --workers mode
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        commandarray
function ozone_worker_mode_execute
{
  #
  # input should be the command line as given by the user
  # in the form of an array
  #
  local argv=("$@")

  # if --workers is still on the command line, remove it
  # to prevent loops
  # Also remove --hostnames and --hosts along with arg values
  local argsSize=${#argv[@]};
  for (( i = 0; i < argsSize; i++ ))
  do
    if [[ "${argv[$i]}" =~ ^--workers$ ]]; then
      unset argv[$i]
    elif [[ "${argv[$i]}" =~ ^--hostnames$ ]] ||
      [[ "${argv[$i]}" =~ ^--hosts$ ]]; then
      unset argv[$i];
      let i++;
      unset argv[$i];
    fi
  done
  if [[ ${QATESTMODE} = true ]]; then
    echo "${argv[@]}"
    return
  fi
  ozone_connect_to_hosts -- "${argv[@]}"
}

## @description  Verify that a shell command was passed a valid
## @description  class name
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        classname
## @return       0 = success
## @return       1 = failure w/user message
function ozone_validate_classname
{
  local class=$1
  shift 1

  if [[ ! ${class} =~ \. ]]; then
    # assuming the arg is typo of command if it does not conatain ".".
    # class belonging to no package is not allowed as a result.
    ozone_error "ERROR: ${class} is not COMMAND nor fully qualified CLASSNAME."
    return 1
  fi
  return 0
}

## @description  Append the `appendstring` if `checkstring` is not
## @description  present in the given `envvar`
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        envvar
## @param        checkstring
## @param        appendstring
function ozone_add_param
{
  #
  # general param dedupe..
  # $1 is what we are adding to
  # $2 is the name of what we want to add (key)
  # $3 is the key+value of what we're adding
  #
  # doing it this way allows us to support all sorts of
  # different syntaxes, just so long as they are space
  # delimited
  #
  if [[ ! ${!1} =~ $2 ]] ; then
    #shellcheck disable=SC2140
    eval "$1"="'${!1} $3'"
    if [[ ${!1:0:1} = ' ' ]]; then
      #shellcheck disable=SC2140
      eval "$1"="'${!1# }'"
    fi
    ozone_debug "$1 accepted $3"
  else
    ozone_debug "$1 declined $3"
  fi
}

## @description  Register the given `shellprofile` to the Hadoop
## @description  shell subsystem
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        shellprofile
function ozone_add_profile
{
  # shellcheck disable=SC2086
  ozone_add_param OZONE_SHELL_PROFILES $1 $1
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the classpath. Optionally provide
## @description  a hint as to where in the classpath it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function ozone_add_classpath
{
  # However, with classpath (& JLP), we can do dedupe
  # along with some sanity checking (e.g., missing directories)
  # since we have a better idea of what is legal
  #
  # for wildcard at end, we can
  # at least check the dir exists
  if [[ $1 =~ ^.*\*$ ]]; then
    local mp
    mp=$(dirname "$1")
    if [[ ! -d "${mp}" ]]; then
      ozone_debug "Rejected CLASSPATH: $1 (not a dir)"
      return 1
    fi

    # no wildcard in the middle, so check existence
    # (doesn't matter *what* it is)
  elif [[ ! $1 =~ ^.*\*.*$ ]] && [[ ! -e "$1" ]]; then
    ozone_debug "Rejected CLASSPATH: $1 (does not exist)"
    return 1
  fi
  if [[ -z "${CLASSPATH}" ]]; then
    CLASSPATH=$1
    ozone_debug "Initial CLASSPATH=$1"
  elif [[ ":${CLASSPATH}:" != *":$1:"* ]]; then
    if [[ "$2" = "before" ]]; then
      CLASSPATH="$1:${CLASSPATH}"
      ozone_debug "Prepend CLASSPATH: $1"
    else
      CLASSPATH+=:$1
      ozone_debug "Append CLASSPATH: $1"
    fi
  else
    ozone_debug "Dupe CLASSPATH: $1"
  fi
  return 0
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the colonpath.  Optionally provide
## @description  a hint as to where in the colonpath it should go.
## @description  Prior to adding, objects are checked for duplication
## @description  and check for existence.  Many other functions use
## @description  this function as their base implementation
## @description  including `ozone_add_javalibpath` and `ozone_add_ldlibpath`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        envvar
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function ozone_add_colonpath
{
  # this is CLASSPATH, JLP, etc but with dedupe but no
  # other checking
  if [[ -d "${2}" ]] && [[ ":${!1}:" != *":$2:"* ]]; then
    if [[ -z "${!1}" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2'"
      ozone_debug "Initial colonpath($1): $2"
    elif [[ "$3" = "before" ]]; then
      # shellcheck disable=SC2086
      eval $1="'$2:${!1}'"
      ozone_debug "Prepend colonpath($1): $2"
    else
      # shellcheck disable=SC2086
      eval $1+=":'$2'"
      ozone_debug "Append colonpath($1): $2"
    fi
    return 0
  fi
  ozone_debug "Rejected colonpath($1): $2"
  return 1
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the Java JNI path.  Optionally
## @description  provide a hint as to where in the Java JNI path
## @description  it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function ozone_add_javalibpath
{
  # specialized function for a common use case
  ozone_add_colonpath JAVA_LIBRARY_PATH "$1" "$2"
}

## @description  Add a file system object (directory, file,
## @description  wildcard, ...) to the LD_LIBRARY_PATH.  Optionally
## @description  provide a hint as to where in the LD_LIBRARY_PATH
## @description  it should go.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        object
## @param        [before|after]
## @return       0 = success (added or duplicate)
## @return       1 = failure (doesn't exist or some other reason)
function ozone_add_ldlibpath
{
  local status
  # specialized function for a common use case
  ozone_add_colonpath LD_LIBRARY_PATH "$1" "$2"
  status=$?

  # note that we export this
  export LD_LIBRARY_PATH
  return ${status}
}

## @description  Add the user's custom classpath settings to the
## @description  environment
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_add_to_classpath_userpath
{
  # Add the classpath definition from argument (or OZONE_CLASSPATH if no
  # argument specified) to the official CLASSPATH env var if
  # OZONE_USE_CLIENT_CLASSLOADER is not set.
  # Add it first or last depending on if user has
  # set env-var OZONE_USER_CLASSPATH_FIRST
  # we'll also dedupe it, because we're cool like that.
  #
  declare -a array
  declare -i c=0
  declare -i j
  declare -i i
  declare idx
  local userpath="${1:-${OZONE_CLASSPATH}}"

  if [[ -n "${userpath}" ]]; then
    # I wonder if Java runs on VMS.
    for idx in $(echo "${userpath}" | tr : '\n'); do
      array[${c}]=${idx}
      ((c=c+1))
    done

    # bats gets confused by j getting set to 0
    ((j=c-1)) || ${QATESTMODE}

    if [[ -z "${OZONE_USE_CLIENT_CLASSLOADER}" ]]; then
      if [[ -z "${OZONE_USER_CLASSPATH_FIRST}" ]]; then
        for ((i=0; i<=j; i++)); do
          ozone_add_classpath "${array[$i]}" after
        done
      else
        for ((i=j; i>=0; i--)); do
          ozone_add_classpath "${array[$i]}" before
        done
      fi
    fi
  fi
}

## @description  Routine to configure any OS-specific settings.
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       may exit on failure conditions
function ozone_os_tricks
{
  local bindv6only

  OZONE_IS_CYGWIN=false
  case ${OZONE_OS_TYPE} in
    Darwin)
      if [[ -z "${JAVA_HOME}" ]]; then
        if [[ -x /usr/libexec/java_home ]]; then
          JAVA_HOME="$(/usr/libexec/java_home)"
          export JAVA_HOME
        else
          JAVA_HOME=/Library/Java/Home
          export JAVA_HOME
        fi
      fi
    ;;
    Linux)

      # Newer versions of glibc use an arena memory allocator that
      # causes virtual # memory usage to explode. This interacts badly
      # with the many threads that we use in Hadoop. Tune the variable
      # down to prevent vmem explosion.
      export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
      # we put this in QA test mode off so that non-Linux can test
      if [[ "${QATESTMODE}" = true ]]; then
        return
      fi

      # NOTE! OZONE_ALLOW_IPV6 is a developer hook.  We leave it
      # undocumented in ozone-env.sh because we don't want users to
      # shoot themselves in the foot while devs make IPv6 work.

      bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)

      if [[ -n "${bindv6only}" ]] &&
         [[ "${bindv6only}" -eq "1" ]] &&
         [[ "${OZONE_ALLOW_IPV6}" != "yes" ]]; then
        ozone_error "ERROR: \"net.ipv6.bindv6only\" is set to 1 "
        ozone_error "ERROR: Hadoop networking could be broken. Aborting."
        ozone_error "ERROR: For more info: http://wiki.apache.org/hadoop/HadoopIPv6"
        exit 1
      fi
    ;;
    CYGWIN*)
      # Flag that we're running on Cygwin to trigger path translation later.
      OZONE_IS_CYGWIN=true
    ;;
  esac
}

## @description  Configure/verify ${JAVA_HOME}
## @audience     public
## @stability    stable
## @replaceable  yes
## @return       may exit on failure conditions
function ozone_java_setup
{
  # Bail if we did not detect it
  if [[ -z "${JAVA_HOME}" ]]; then
    ozone_error "ERROR: JAVA_HOME is not set and could not be found."
    exit 1
  fi

  if [[ ! -d "${JAVA_HOME}" ]]; then
    ozone_error "ERROR: JAVA_HOME ${JAVA_HOME} does not exist."
    exit 1
  fi

  JAVA="${JAVA_HOME}/bin/java"

  if [[ ! -x "$JAVA" ]]; then
    ozone_error "ERROR: $JAVA is not executable."
    exit 1
  fi

  # Get the version string
  JAVA_VERSION_STRING=$(${JAVA} -version 2>&1 | head -n 1)

  # Extract the major version number
  JAVA_MAJOR_VERSION=$(echo "$JAVA_VERSION_STRING" | sed -E -n 's/.* version "([^.-]*).*"/\1/p' | cut -d' ' -f1)

  # Add JVM parameter (org.apache.ratis.thirdparty.io.netty.tryReflectionSetAccessible=true)
  # to allow netty unsafe memory allocation in Java 9+.
  RATIS_OPTS="${RATIS_OPTS:-}"

  if [[ "${JAVA_MAJOR_VERSION}" -ge 9 ]]; then
    RATIS_OPTS="-Dorg.apache.ratis.thirdparty.io.netty.tryReflectionSetAccessible=true ${RATIS_OPTS}"
  fi

  ozone_set_module_access_args
}

## @description  Set OZONE_MODULE_ACCESS_ARGS based on Java version
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_set_module_access_args
{
  if [[ -z "${JAVA_MAJOR_VERSION}" ]]; then
    return
  fi

  # populate JVM args based on java version
  if [[ "${JAVA_MAJOR_VERSION}" -ge 17 ]]; then
    OZONE_MODULE_ACCESS_ARGS="${OZONE_MODULE_ACCESS_ARGS} --add-opens java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED"
    OZONE_MODULE_ACCESS_ARGS="${OZONE_MODULE_ACCESS_ARGS} --add-exports java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED"
  fi
  if [[ "${JAVA_MAJOR_VERSION}" -ge 9 ]]; then
    OZONE_MODULE_ACCESS_ARGS="${OZONE_MODULE_ACCESS_ARGS} --add-opens java.base/java.nio=ALL-UNNAMED"
    OZONE_MODULE_ACCESS_ARGS="${OZONE_MODULE_ACCESS_ARGS} --add-opens java.base/java.lang=ALL-UNNAMED"
    OZONE_MODULE_ACCESS_ARGS="${OZONE_MODULE_ACCESS_ARGS} --add-opens java.base/java.lang.reflect=ALL-UNNAMED"
  fi
}

## @description  Finish Java JNI paths prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_finalize_libpaths
{
  if [[ -n "${JAVA_LIBRARY_PATH}" ]]; then
    ozone_translate_cygwin_path JAVA_LIBRARY_PATH
    ozone_add_param OZONE_OPTS java.library.path \
      "-Djava.library.path=${JAVA_LIBRARY_PATH}"
    export LD_LIBRARY_PATH
  fi
}

## @description  Finish Java heap parameters prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_finalize_heap
{
  ozone_deprecate_hadoop_vars HEAPSIZE HEAPSIZE_MAX HEAPSIZE_MIN

  if [[ -n "${OZONE_HEAPSIZE_MAX}" ]]; then
    if [[ "${OZONE_HEAPSIZE_MAX}" =~ ^[0-9]+$ ]]; then
      OZONE_HEAPSIZE_MAX="${OZONE_HEAPSIZE_MAX}m"
    fi
    ozone_add_param OZONE_OPTS Xmx "-Xmx${OZONE_HEAPSIZE_MAX}"
  fi

  # backwards compatibility
  if [[ -n "${OZONE_HEAPSIZE}" ]]; then
    if [[ "${OZONE_HEAPSIZE}" =~ ^[0-9]+$ ]]; then
      OZONE_HEAPSIZE="${OZONE_HEAPSIZE}m"
    fi
    ozone_add_param OZONE_OPTS Xmx "-Xmx${OZONE_HEAPSIZE}"
  fi

  if [[ -n "${OZONE_HEAPSIZE_MIN}" ]]; then
    if [[ "${OZONE_HEAPSIZE_MIN}" =~ ^[0-9]+$ ]]; then
      OZONE_HEAPSIZE_MIN="${OZONE_HEAPSIZE_MIN}m"
    fi
    ozone_add_param OZONE_OPTS Xms "-Xms${OZONE_HEAPSIZE_MIN}"
  fi
}

## @description  Converts the contents of the variable name
## @description  `varnameref` into the equivalent Windows path.
## @description  If the second parameter is true, then `varnameref`
## @description  is treated as though it was a path list.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        varnameref
## @param        [true]
function ozone_translate_cygwin_path
{
  if [[ "${OZONE_IS_CYGWIN}" = "true" ]]; then
    if [[ "$2" = "true" ]]; then
      #shellcheck disable=SC2016
      eval "$1"='$(cygpath -p -w "${!1}" 2>/dev/null)'
    else
      #shellcheck disable=SC2016
      eval "$1"='$(cygpath -w "${!1}" 2>/dev/null)'
    fi
  fi
}

## @description  Adds default GC parameters
## @description  Only for server components and only if no other -XX parameters
## @description  are set
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_add_default_gc_opts
{
  java_major_version=$(ozone_get_java_major_version)
  if [[ "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" == true ]]; then
    if [[ ! "$OZONE_OPTS" =~ "-XX" ]] ; then
      OZONE_OPTS="${OZONE_OPTS} -XX:ParallelGCThreads=8"
      if [[ "$java_major_version" -lt 15 ]]; then
        OZONE_OPTS="${OZONE_OPTS} -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled"
        ozone_error "No '-XX:...' jvm parameters are set. Adding safer GC settings '-XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled' to the OZONE_OPTS"
      else
        ozone_error "No '-XX:...' jvm parameters are set. Adding safer GC settings '-XX:ParallelGCThreads=8' to the OZONE_OPTS"
      fi
    fi
  fi
}

## @description  Get Java Major version
## @audience     private
## @stability    yes
## @replaceable  yes
function ozone_get_java_major_version
{
  version=$("${JAVA}" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "$version" =~ ^1\..* ]]; then
    major=$(echo $version | cut -d. -f2)
  else
    major=$(echo $version | cut -d. -f1)
  fi
  echo "$major"
}

## @description  Adds the OZONE_CLIENT_OPTS variable to
## @description  OZONE_OPTS if OZONE_SUBCMD_SUPPORTDAEMONIZATION is false
## @audience     public
## @stability    stable
## @replaceable  yes
function ozone_add_client_opts
{
  if [[ "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" = false
     || -z "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" ]]; then
    ozone_debug "Appending OZONE_CLIENT_OPTS onto OZONE_OPTS"
    OZONE_OPTS="${OZONE_OPTS} ${OZONE_CLIENT_OPTS}"
  fi
}

## @description  Adds the OZONE_SERVER_OPTS variable to OZONE_OPTS if OZONE_SUBCMD_SUPPORTDAEMONIZATION is true
## @audience     public
## @stability    stable
## @replaceable  yes
function ozone_add_server_opts
{
  if [[ "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" == "true" ]] && [[ -n "${OZONE_SERVER_OPTS:-}" ]]; then
    ozone_debug "Appending OZONE_SERVER_OPTS onto OZONE_OPTS"
    OZONE_OPTS="${OZONE_OPTS} ${OZONE_SERVER_OPTS}"
  fi
}

## @description  Finish configuring Hadoop specific system properties
## @description  prior to executing Java
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_finalize_opts
{
  ozone_translate_cygwin_path OZONE_LOG_DIR
  ozone_add_param OZONE_OPTS hadoop.log.dir "-Dhadoop.log.dir=${OZONE_LOG_DIR}"
  ozone_add_param OZONE_OPTS hadoop.log.file "-Dhadoop.log.file=${OZONE_LOGFILE}"
  ozone_translate_cygwin_path OZONE_HOME
  export OZONE_HOME
  ozone_add_param OZONE_OPTS hadoop.home.dir "-Dhadoop.home.dir=${OZONE_HOME}"
  ozone_add_param OZONE_OPTS hadoop.id.str "-Dhadoop.id.str=${OZONE_IDENT_STRING}"
  ozone_add_param OZONE_OPTS hadoop.root.logger "-Dhadoop.root.logger=${OZONE_ROOT_LOGGER}"
  ozone_add_param OZONE_OPTS hadoop.policy.file "-Dhadoop.policy.file=${OZONE_POLICYFILE}"
  ozone_add_param OZONE_OPTS hadoop.security.logger "-Dhadoop.security.logger=${OZONE_SECURITY_LOGGER}"
}

## @description  Finish Java classpath prior to execution
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_finalize_classpath
{
  ozone_add_classpath "${OZONE_CONF_DIR}" before

  # user classpath gets added at the last minute. this allows
  # override of CONF dirs and more
  ozone_add_to_classpath_userpath
  ozone_translate_cygwin_path CLASSPATH true
}

## @description  Finish all the remaining environment settings prior
## @description  to executing Java.  This is a wrapper that calls
## @description  the other `finalize` routines.
## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_finalize
{
  ozone_shellprofiles_finalize

  ozone_finalize_classpath
  ozone_finalize_libpaths
  ozone_finalize_heap
  ozone_finalize_opts

  ozone_translate_cygwin_path OZONE_HOME
  ozone_translate_cygwin_path OZONE_CONF_DIR
}

## @description  Print usage information and exit with the passed
## @description  `exitcode`
## @audience     public
## @stability    stable
## @replaceable  no
## @param        exitcode
## @return       This function will always exit.
function ozone_exit_with_usage
{
  local exitcode=$1
  if [[ -z $exitcode ]]; then
    exitcode=1
  fi
  # shellcheck disable=SC2034
  if declare -F ozone_usage >/dev/null ; then
    ozone_usage
  elif [[ -x /usr/bin/cowsay ]]; then
    /usr/bin/cowsay -f elephant "Sorry, no help available."
  else
    ozone_error "Sorry, no help available."
  fi
  exit $exitcode
}

## @description  Verify that prerequisites have been met prior to
## @description  excuting a privileged program.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @return       This routine may exit.
function ozone_verify_secure_prereq
{
  # if you are on an OS like Illumos that has functional roles
  # and you are using pfexec, you'll probably want to change
  # this.

  if ! ozone_privilege_check && [[ -z "${OZONE_SECURE_COMMAND}" ]]; then
    ozone_error "ERROR: You must be a privileged user in order to run a secure service."
    exit 1
  else
    return 0
  fi
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_setup_secure_service
{
  # need a more complicated setup? replace me!

  OZONE_PID_DIR=${OZONE_SECURE_PID_DIR}
  OZONE_LOG_DIR=${OZONE_SECURE_LOG_DIR}
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_verify_piddir
{
  if [[ -z "${OZONE_PID_DIR}" ]]; then
    ozone_error "No pid directory defined."
    exit 1
  fi
  ozone_mkdir "${OZONE_PID_DIR}"
  touch "${OZONE_PID_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Unable to write in ${OZONE_PID_DIR}. Aborting."
    exit 1
  fi
  rm "${OZONE_PID_DIR}/$$" >/dev/null 2>&1
}

## @audience     private
## @stability    evolving
## @replaceable  yes
function ozone_verify_logdir
{
  if [[ -z "${OZONE_LOG_DIR}" ]]; then
    ozone_error "No log directory defined."
    exit 1
  fi
  ozone_mkdir "${OZONE_LOG_DIR}"
  touch "${OZONE_LOG_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Unable to write in ${OZONE_LOG_DIR}. Aborting."
    exit 1
  fi
  rm "${OZONE_LOG_DIR}/$$" >/dev/null 2>&1
}

## @description  Determine the status of the daemon referenced
## @description  by `pidfile`
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        pidfile
## @return       (mostly) LSB 4.1.0 compatible status
function ozone_status_daemon
{
  #
  # LSB 4.1.0 compatible status command (1)
  #
  # 0 = program is running
  # 1 = dead, but still a pid (2)
  # 2 = (not used by us)
  # 3 = not running
  #
  # 1 - this is not an endorsement of the LSB
  #
  # 2 - technically, the specification says /var/run/pid, so
  #     we should never return this value, but we're giving
  #     them the benefit of a doubt and returning 1 even if
  #     our pid is not in in /var/run .
  #

  local pidfile=$1
  shift

  local pid
  local pspid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "${pidfile}")
    if pspid=$(ps -o args= -p"${pid}" 2>/dev/null); then
      # this is to check that the running process we found is actually the same
      # daemon that we're interested in
      if [[ ${pspid} =~ -Dproc_${daemonname} ]]; then
        return 0
      fi
    fi
    return 1
  fi
  return 3
}

## @description  Execute the Java `class`, passing along any `options`.
## @description  Additionally, set the Java property -Dproc_`command`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        class
## @param        [options]
function ozone_java_exec
{
  # run a java command.  this is used for
  # non-daemons

  local command=$1
  local class=$2
  shift 2

  ozone_debug "Final CLASSPATH: ${CLASSPATH}"
  ozone_debug "Final OZONE_OPTS: ${OZONE_OPTS}"
  ozone_debug "Final JAVA_HOME: ${JAVA_HOME}"
  ozone_debug "java: ${JAVA}"
  ozone_debug "Class name: ${class}"
  ozone_debug "Command line options: $*"

  export CLASSPATH
  #shellcheck disable=SC2086
  exec "${JAVA}" "-Dproc_${command}" ${OZONE_OPTS} "${class}" "$@"
}

## @description  Start a non-privileged daemon in the foreground.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        pidfile
## @param        [options]
function ozone_start_daemon
{
  # this is our non-privileged daemon starter
  # that fires up a daemon in the *foreground*
  # so complex! so wow! much java!
  local command=$1
  local class=$2
  local pidfile=$3
  shift 3

  ozone_debug "Final CLASSPATH: ${CLASSPATH}"
  ozone_debug "Final OZONE_OPTS: ${OZONE_OPTS}"
  ozone_debug "Final JAVA_HOME: ${JAVA_HOME}"
  ozone_debug "java: ${JAVA}"
  ozone_debug "Class name: ${class}"
  ozone_debug "Command line options: $*"

  # this is for the non-daemon pid creation
  #shellcheck disable=SC2086
  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR:  Cannot write ${command} pid ${pidfile}."
  fi

  export CLASSPATH
  #shellcheck disable=SC2086
  exec "${JAVA}" "-Dproc_${command}" ${OZONE_OPTS} "${class}" "$@"
}

## @description  Start a non-privileged daemon in the background.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        pidfile
## @param        outfile
## @param        [options]
function ozone_start_daemon_wrapper
{
  local daemonname=$1
  local class=$2
  local pidfile=$3
  local outfile=$4
  shift 4

  local counter

  ozone_rotate_log "${outfile}"

  ozone_start_daemon "${daemonname}" \
    "$class" \
    "${pidfile}" \
    "$@" >> "${outfile}" 2>&1 < /dev/null &

  # we need to avoid a race condition here
  # so let's wait for the fork to finish
  # before overriding with the daemonized pid
  (( counter=0 ))
  while [[ ! -f ${pidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  # this is for daemon pid creation
  #shellcheck disable=SC2086
  echo $! > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR:  Cannot write ${daemonname} pid ${pidfile}."
  fi

  # shellcheck disable=SC2086
  renice "${OZONE_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Cannot set priority of ${daemonname} process $!"
  fi

  # shellcheck disable=SC2086
  disown %+ >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Cannot disconnect ${daemonname} process $!"
  fi
  sleep 1

  # capture the ulimit output
  ulimit -a >> "${outfile}" 2>&1

  # shellcheck disable=SC2086
  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

## @description  Start a privileged daemon in the foreground.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        daemonerrfile
## @param        wrapperpidfile
## @param        [options]
function ozone_start_secure_daemon
{
  # this is used to launch a secure daemon in the *foreground*
  #
  local daemonname=$1
  local class=$2

  # pid file to create for our daemon
  local daemonpidfile=$3

  # where to send stdout. jsvc has bad habits so this *may* be &1
  # which means you send it to stdout!
  local daemonoutfile=$4

  # where to send stderr.  same thing, except &2 = stderr
  local daemonerrfile=$5
  local privpidfile=$6
  shift 6

  ozone_rotate_log "${daemonoutfile}"
  ozone_rotate_log "${daemonerrfile}"

  # shellcheck disable=SC2153
  jsvc="${JSVC_HOME}/jsvc"
  if [[ ! -f "${jsvc}" ]]; then
    ozone_error "JSVC_HOME is not set or set incorrectly. jsvc is required to run secure"
    ozone_error "or privileged daemons. Please download and install jsvc from "
    ozone_error "http://archive.apache.org/dist/commons/daemon/binaries/ "
    ozone_error "and set JSVC_HOME to the directory containing the jsvc binary."
    exit 1
  fi

  # note that shellcheck will throw a
  # bogus for-our-use-case 2086 here.
  # it doesn't properly support multi-line situations

  ozone_debug "Final CLASSPATH: ${CLASSPATH}"
  ozone_debug "Final OZONE_OPTS: ${OZONE_OPTS}"
  ozone_debug "Final JSVC_HOME: ${JSVC_HOME}"
  ozone_debug "jsvc: ${jsvc}"
  ozone_debug "Final OZONE_DAEMON_JSVC_EXTRA_OPTS: ${OZONE_DAEMON_JSVC_EXTRA_OPTS}"
  ozone_debug "Class name: ${class}"
  ozone_debug "Command line options: $*"

  #shellcheck disable=SC2086
  echo $$ > "${privpidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR:  Cannot write ${daemonname} pid ${privpidfile}."
  fi

  # shellcheck disable=SC2086
  exec "${jsvc}" \
    "-Dproc_${daemonname}" \
    ${OZONE_DAEMON_JSVC_EXTRA_OPTS} \
    -outfile "${daemonoutfile}" \
    -errfile "${daemonerrfile}" \
    -pidfile "${daemonpidfile}" \
    -nodetach \
    -user "${OZONE_SECURE_USER}" \
    -cp "${CLASSPATH}" \
    ${OZONE_OPTS} \
    "${class}" "$@"
}

## @description  Start a privileged daemon in the background.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        wrapperpidfile
## @param        warpperoutfile
## @param        daemonerrfile
## @param        [options]
function ozone_start_secure_daemon_wrapper
{
  # this wraps ozone_start_secure_daemon to take care
  # of the dirty work to launch a daemon in the background!
  local daemonname=$1
  local class=$2

  # same rules as ozone_start_secure_daemon except we
  # have some additional parameters

  local daemonpidfile=$3

  local daemonoutfile=$4

  # the pid file of the subprocess that spawned our
  # secure launcher
  local jsvcpidfile=$5

  # the output of the subprocess that spawned our secure
  # launcher
  local jsvcoutfile=$6

  local daemonerrfile=$7
  shift 7

  local counter

  ozone_rotate_log "${jsvcoutfile}"

  ozone_start_secure_daemon \
    "${daemonname}" \
    "${class}" \
    "${daemonpidfile}" \
    "${daemonoutfile}" \
    "${daemonerrfile}" \
    "${jsvcpidfile}"  "$@" >> "${jsvcoutfile}" 2>&1 < /dev/null &

  # we need to avoid a race condition here
  # so let's wait for the fork to finish
  # before overriding with the daemonized pid
  (( counter=0 ))
  while [[ ! -f ${daemonpidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  #shellcheck disable=SC2086
  if ! echo $! > "${jsvcpidfile}"; then
    ozone_error "ERROR:  Cannot write ${daemonname} pid ${jsvcpidfile}."
  fi

  sleep 1
  #shellcheck disable=SC2086
  renice "${OZONE_NICENESS}" $! >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Cannot set priority of ${daemonname} process $!"
  fi
  if [[ -f "${daemonpidfile}" ]]; then
    #shellcheck disable=SC2046
    renice "${OZONE_NICENESS}" $(cat "${daemonpidfile}" 2>/dev/null) >/dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      ozone_error "ERROR: Cannot set priority of ${daemonname} process $(cat "${daemonpidfile}" 2>/dev/null)"
    fi
  fi
  #shellcheck disable=SC2046
  disown %+ >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    ozone_error "ERROR: Cannot disconnect ${daemonname} process $!"
  fi
  # capture the ulimit output
  su "${OZONE_SECURE_USER}" -c 'bash -c "ulimit -a"' >> "${jsvcoutfile}" 2>&1
  #shellcheck disable=SC2086
  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

## @description  Wait till process dies or till timeout
## @audience     private
## @stability    evolving
## @param        pid
## @param        timeout
function wait_process_to_die_or_timeout
{
  local pid=$1
  local timeout=$2

  # Normalize timeout
  # Round up or down
  timeout=$(printf "%.0f\n" "${timeout}")
  if [[ ${timeout} -lt 1  ]]; then
    # minimum 1 second
    timeout=1
  fi

  # Wait to see if it's still alive
  for (( i=0; i < "${timeout}"; i++ ))
  do
    if kill -0 "${pid}" > /dev/null 2>&1; then
      sleep 1
    else
      break
    fi
  done
}

## @description  Stop the non-privileged `command` daemon with that
## @description  that is running at `pidfile`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        pidfile
function ozone_stop_daemon
{
  local cmd=$1
  local pidfile=$2
  shift 2

  local pid
  local cur_pid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")

    kill "${pid}" >/dev/null 2>&1

    wait_process_to_die_or_timeout "${pid}" "${OZONE_STOP_TIMEOUT}"

    if kill -0 "${pid}" > /dev/null 2>&1; then
      ozone_error "WARNING: ${cmd} did not stop gracefully after ${OZONE_STOP_TIMEOUT} seconds: Trying to kill with kill -9"
      kill -9 "${pid}" >/dev/null 2>&1
    fi
    wait_process_to_die_or_timeout "${pid}" "${OZONE_STOP_TIMEOUT}"
    if ps -p "${pid}" > /dev/null 2>&1; then
      ozone_error "ERROR: Unable to kill ${pid}"
    else
      cur_pid=$(cat "$pidfile")
      if [[ "${pid}" = "${cur_pid}" ]]; then
        rm -f "${pidfile}" >/dev/null 2>&1
      else
        ozone_error "WARNING: pid has changed for ${cmd}, skip deleting pid file"
      fi
    fi
  fi
}

## @description  Stop the privileged `command` daemon with that
## @description  that is running at `daemonpidfile` and launched with
## @description  the wrapper at `wrapperpidfile`.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        daemonpidfile
## @param        wrapperpidfile
function ozone_stop_secure_daemon
{
  local command=$1
  local daemonpidfile=$2
  local privpidfile=$3
  shift 3
  local ret

  local daemon_pid
  local priv_pid
  local cur_daemon_pid
  local cur_priv_pid

  daemon_pid=$(cat "$daemonpidfile")
  priv_pid=$(cat "$privpidfile")

  ozone_stop_daemon "${command}" "${daemonpidfile}"
  ret=$?

  cur_daemon_pid=$(cat "$daemonpidfile")
  cur_priv_pid=$(cat "$privpidfile")

  if [[ "${daemon_pid}" = "${cur_daemon_pid}" ]]; then
    rm -f "${daemonpidfile}" >/dev/null 2>&1
  else
    ozone_error "WARNING: daemon pid has changed for ${command}, skip deleting daemon pid file"
  fi

  if [[ "${priv_pid}" = "${cur_priv_pid}" ]]; then
    rm -f "${privpidfile}" >/dev/null 2>&1
  else
    ozone_error "WARNING: priv pid has changed for ${command}, skip deleting priv pid file"
  fi
  return ${ret}
}

## @description  Manage a non-privileged daemon.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [start|stop|status|default]
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        [options]
function ozone_daemon_handler
{
  local daemonmode=$1
  local daemonname=$2
  local class=$3
  local daemon_pidfile=$4
  local daemon_outfile=$5
  shift 5

  case ${daemonmode} in
    status)
      ozone_status_daemon "${daemon_pidfile}"
      exit $?
    ;;

    stop)
      ozone_stop_daemon "${daemonname}" "${daemon_pidfile}"
      exit $?
    ;;

    ##COMPAT  -- older hadoops would also start daemons by default
    start|default)
      ozone_verify_piddir
      ozone_verify_logdir
      ozone_status_daemon "${daemon_pidfile}"
      if [[ $? == 0  ]]; then
        ozone_error "${daemonname} is running as process $(cat "${daemon_pidfile}").  Stop it first."
        exit 1
      else
        # stale pid file, so just remove it and continue on
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi
      ##COMPAT  - differenticate between --daemon start and nothing
      # "nothing" shouldn't detach
      if [[ "$daemonmode" = "default" ]]; then
        ozone_start_daemon "${daemonname}" "${class}" "${daemon_pidfile}" "$@"
      else
        ozone_start_daemon_wrapper "${daemonname}" \
        "${class}" "${daemon_pidfile}" "${daemon_outfile}" "$@"
      fi
    ;;
  esac
}

## @description  Manage a privileged daemon.
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [start|stop|status|default]
## @param        command
## @param        class
## @param        daemonpidfile
## @param        daemonoutfile
## @param        wrapperpidfile
## @param        wrapperoutfile
## @param        wrappererrfile
## @param        [options]
function ozone_secure_daemon_handler
{
  local daemonmode=$1
  local daemonname=$2
  local classname=$3
  local daemon_pidfile=$4
  local daemon_outfile=$5
  local priv_pidfile=$6
  local priv_outfile=$7
  local priv_errfile=$8
  shift 8

  case ${daemonmode} in
    status)
      ozone_status_daemon "${daemon_pidfile}"
      exit $?
    ;;

    stop)
      ozone_stop_secure_daemon "${daemonname}" \
      "${daemon_pidfile}" "${priv_pidfile}"
      exit $?
    ;;

    ##COMPAT  -- older hadoops would also start daemons by default
    start|default)
      ozone_verify_piddir
      ozone_verify_logdir
      ozone_status_daemon "${daemon_pidfile}"
      if [[ $? == 0  ]]; then
        ozone_error "${daemonname} is running as process $(cat "${daemon_pidfile}").  Stop it first."
        exit 1
      else
        # stale pid file, so just remove it and continue on
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi

      ##COMPAT  - differenticate between --daemon start and nothing
      # "nothing" shouldn't detach
      if [[ "${daemonmode}" = "default" ]]; then
        ozone_start_secure_daemon "${daemonname}" "${classname}" \
        "${daemon_pidfile}" "${daemon_outfile}" \
        "${priv_errfile}" "${priv_pidfile}" "$@"
      else
        ozone_start_secure_daemon_wrapper "${daemonname}" "${classname}" \
        "${daemon_pidfile}" "${daemon_outfile}" \
        "${priv_pidfile}" "${priv_outfile}" "${priv_errfile}"  "$@"
      fi
    ;;
  esac
}

## @description autodetect whether this is a priv subcmd
## @description by whether or not a priv user var exists
## @description and if OZONE_SECURE_CLASSNAME is defined
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        subcommand
## @return       1 = not priv
## @return       0 = priv
function ozone_detect_priv_subcmd
{
  declare program=$1
  declare command=$2

  if [[ -z "${OZONE_SECURE_CLASSNAME}" ]]; then
    ozone_debug "No secure classname defined."
    return 1
  fi

  uvar=$(ozone_build_custom_subcmd_var "${program}" "${command}" SECURE_USER)
  if [[ -z "${!uvar}" ]]; then
    ozone_debug "No secure user defined."
    return 1
  fi
  return 0
}

## @description  Build custom subcommand var
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        subcommand
## @param        customid
## @return       string
function ozone_build_custom_subcmd_var
{
  declare program=$1
  declare command=$2
  declare custom=$3
  declare uprogram
  declare ucommand

  if [[ -z "${BASH_VERSINFO[0]}" ]] \
     || [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    uprogram=$(echo "${program}" | tr '[:lower:]' '[:upper:]')
    ucommand=$(echo "${command}" | tr '[:lower:]' '[:upper:]')
  else
    uprogram=${program^^}
    ucommand=${command^^}
  fi

  echo "${uprogram}_${ucommand}_${custom}"
}

## @description  Verify that username in a var converts to user id
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        userstring
## @return       0 for success
## @return       1 for failure
function ozone_verify_user_resolves
{
  declare userstr=$1

  if [[ -z ${userstr} || -z ${!userstr} ]] ; then
    return 1
  fi

  id -u "${!userstr}" >/dev/null 2>&1
}

## @description  Verify that ${USER} is allowed to execute the
## @description  given subcommand.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        command
## @param        subcommand
## @return       return 0 on success
## @return       exit 1 on failure
function ozone_verify_user_perm
{
  declare program=$1
  declare command=$2
  declare uvar

  if [[ ${command} =~ \. ]]; then
    return 1
  fi

  uvar=$(ozone_build_custom_subcmd_var "${program}" "${command}" USER)

  if [[ -n ${!uvar} ]]; then
    if [[ ${!uvar} !=  "${USER}" ]]; then
      ozone_error "ERROR: ${command} can only be executed by ${!uvar}."
      exit 1
    fi
  fi
  return 0
}

## @description  Verify that ${USER} is allowed to execute the
## @description  given subcommand.
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        subcommand
## @return       1 on no re-exec needed
## @return       0 on need to re-exec
function ozone_need_reexec
{
  declare program=$1
  declare command=$2
  declare uvar

  # we've already been re-execed, bail

  if [[ "${OZONE_REEXECED_CMD}" = true ]]; then
    return 1
  fi

  if [[ ${command} =~ \. ]]; then
    return 1
  fi

  # if we have privilege, and the _USER is defined, and _USER is
  # set to someone who isn't us, then yes, we should re-exec.
  # otherwise no, don't re-exec and let the system deal with it.

  if ozone_privilege_check; then
    uvar=$(ozone_build_custom_subcmd_var "${program}" "${command}" USER)
    if [[ -n ${!uvar} ]]; then
      if [[ ${!uvar} !=  "${USER}" ]]; then
        return 0
      fi
    fi
  fi
  return 1
}

## @description  Add custom (program)_(command)_OPTS to OZONE_OPTS.
## @description  Also handles the deprecated cases from pre-3.x.
## @audience     public
## @stability    evolving
## @replaceable  yes
## @param        program
## @param        subcommand
## @return       will exit on failure conditions
function ozone_subcommand_opts
{
  declare program=$1
  declare command=$2
  declare uvar
  declare depvar
  declare uprogram
  declare ucommand

  if [[ -z "${program}" || -z "${command}" ]]; then
    return 1
  fi

  if [[ ${command} =~ \. ]]; then
    return 1
  fi

  # bash 4 and up have built-in ways to upper and lower
  # case the contents of vars.  This is faster than
  # calling tr.

  if [[ -z "${BASH_VERSINFO[0]}" ]] \
     || [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
    uprogram=$(echo "${program}" | tr '[:lower:]' '[:upper:]')
    ucommand=$(echo "${command}" | tr '[:lower:]' '[:upper:]')
  else
    uprogram=${program^^}
    ucommand=${command^^}
  fi

  uvar="${uprogram}_${ucommand}_OPTS"

  # Let's handle all of the deprecation cases early
  # TODO remove, as now this is no-op, since $program == 'ozone'

  depvar="OZONE_${ucommand}_OPTS"
  if [[ "${depvar}" != "${uvar}" ]]; then
    if [[ -n "${!depvar}" ]]; then
      ozone_deprecate_envvar "${depvar}" "${uvar}"
    fi
  fi

  if [[ -n ${!uvar} ]]; then
    ozone_debug "Appending ${uvar} onto OZONE_OPTS"
    OZONE_OPTS="${OZONE_OPTS} ${!uvar}"
    return 0
  fi
}

## @description  Add custom (program)_(command)_SECURE_EXTRA_OPTS to OZONE_OPTS.
## @description  This *does not* handle the pre-3.x deprecated cases
## @audience     public
## @stability    stable
## @replaceable  yes
## @param        program
## @param        subcommand
## @return       will exit on failure conditions
function ozone_subcommand_secure_opts
{
  declare program=$1
  declare command=$2
  declare uvar
  declare uprogram
  declare ucommand

  if [[ -z "${program}" || -z "${command}" ]]; then
    return 1
  fi

  # OZONE_DATANODE_SECURE_EXTRA_OPTS
  # OZONE_OM_SECURE_EXTRA_OPTS
  # ...
  uvar=$(ozone_build_custom_subcmd_var "${program}" "${command}" SECURE_EXTRA_OPTS)

  if [[ -n ${!uvar} ]]; then
    ozone_debug "Appending ${uvar} onto OZONE_OPTS"
    OZONE_OPTS="${OZONE_OPTS} ${!uvar}"
    return 0
  fi
}

## @description  Perform the 'ozone classpath', etc subcommand with the given
## @description  parameters
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [parameters]
## @return       will print & exit with no params
function ozone_do_classpath_subcommand
{
  if [[ "$#" -gt 1 ]]; then
    eval "$1"=org.apache.hadoop.util.Classpath
  else
    ozone_finalize
    echo "${CLASSPATH}"
    exit 0
  fi
}

## @description  generic shell script option parser.  sets
## @description  OZONE_PARSE_COUNTER to set number the
## @description  caller should shift
## @audience     private
## @stability    evolving
## @replaceable  yes
## @param        [parameters, typically "$@"]
function ozone_parse_args
{
  OZONE_DAEMON_MODE="default"
  OZONE_PARSE_COUNTER=0

  # not all of the options supported here are supported by all commands
  # however these are:
  ozone_add_option "--config dir" "Ozone config directory"
  ozone_add_option "--debug" "turn on shell script debug mode"
  ozone_add_option "--help" "usage information"

  while true; do
    ozone_debug "ozone_parse_args: processing $1"
    case $1 in
      --buildpaths)
        OZONE_ENABLE_BUILD_PATHS=true
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
      ;;
      --jvmargs)
        shift
        ozone_add_param OZONE_OPTS "$1" "$1"
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
      ;;
      --config)
        shift
        confdir=$1
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
        if [[ -d "${confdir}" ]]; then
          OZONE_CONF_DIR="${confdir}"
        elif [[ -z "${confdir}" ]]; then
          ozone_error "ERROR: No parameter provided for --config "
          ozone_exit_with_usage 1
        else
          ozone_error "ERROR: Cannot find configuration directory \"${confdir}\""
          ozone_exit_with_usage 1
        fi
      ;;
      --daemon)
        shift
        OZONE_DAEMON_MODE=$1
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
        if [[ -z "${OZONE_DAEMON_MODE}" || \
          ! "${OZONE_DAEMON_MODE}" =~ ^st(art|op|atus)$ ]]; then
          ozone_error "ERROR: --daemon must be followed by either \"start\", \"stop\", or \"status\"."
          ozone_exit_with_usage 1
        fi
      ;;
      --validate)
        shift
        OZONE_VALIDATE_CLASSPATH=true
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
        if [[ "${1}" == "continue" ]]; then
          OZONE_VALIDATE_FAIL_ON_MISSING_JARS=false
          shift
          ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
        fi
      ;;
      --debug)
        shift
        OZONE_SHELL_SCRIPT_DEBUG=true
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
      ;;
      --help|-help|-h|help|--h|--\?|-\?|\?)
        ozone_exit_with_usage 0
      ;;
      --hostnames)
        shift
        OZONE_WORKER_NAMES="$1"
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
      ;;
      --hosts)
        shift
        ozone_populate_workers_file "$1"
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
      ;;
      --loglevel)
        shift
        # shellcheck disable=SC2034
        OZONE_LOGLEVEL="$1"
        shift
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+2))
      ;;
      --reexec)
        shift
        if [[ "${OZONE_REEXECED_CMD}" = true ]]; then
          ozone_error "ERROR: re-exec fork bomb prevention: --reexec already called"
          exit 1
        fi
        OZONE_REEXECED_CMD=true
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
      ;;
      --workers)
        shift
        # shellcheck disable=SC2034
        OZONE_WORKER_MODE=true
        ((OZONE_PARSE_COUNTER=OZONE_PARSE_COUNTER+1))
      ;;
      *)
        break
      ;;
    esac
  done

  ozone_debug "ozone_parse: asking caller to skip ${OZONE_PARSE_COUNTER}"

  ozone_deprecate_hadoop_vars SHELL_SCRIPT_DEBUG
}

## @description Handle subcommands from main program entries
## @audience private
## @stability evolving
## @replaceable yes
function ozone_generic_java_subcmd_handler
{
  declare priv_outfile
  declare priv_errfile
  declare priv_pidfile
  declare daemon_outfile
  declare daemon_pidfile
  declare secureuser

  # The default/expected way to determine if a daemon is going to run in secure
  # mode is defined by ozone_detect_priv_subcmd.  If this returns true
  # then setup the secure user var and tell the world we're in secure mode

  if ozone_detect_priv_subcmd "${OZONE_SHELL_EXECNAME}" "${OZONE_SUBCMD}"; then
    OZONE_SUBCMD_SECURESERVICE=true
    secureuser=$(ozone_build_custom_subcmd_var "${OZONE_SHELL_EXECNAME}" "${OZONE_SUBCMD}" SECURE_USER)

    if ! ozone_verify_user_resolves "${secureuser}"; then
      ozone_error "ERROR: User defined in ${secureuser} (${!secureuser}) does not exist. Aborting."
      exit 1
    fi

    OZONE_SECURE_USER="${!secureuser}"
  fi

  # check if we're running in secure mode.
  # breaking this up from the above lets 3rd parties
  # do things a bit different
  # secure services require some extra setup
  # if yes, then we need to define all of the priv and daemon stuff
  # if not, then we just need to define daemon stuff.
  # note the daemon vars are purposefully different between the two

  if [[ "${OZONE_SUBCMD_SECURESERVICE}" = true ]]; then

    ozone_subcommand_secure_opts "${OZONE_SHELL_EXECNAME}" "${OZONE_SUBCMD}"

    ozone_verify_secure_prereq
    ozone_setup_secure_service
    priv_outfile="${OZONE_LOG_DIR}/ozone-privileged-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.out"
    priv_errfile="${OZONE_LOG_DIR}/ozone-privileged-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.err"
    priv_pidfile="${OZONE_PID_DIR}/ozone-privileged-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}.pid"
    daemon_outfile="${OZONE_LOG_DIR}/ozone-${OZONE_SECURE_USER}-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.out"
    daemon_pidfile="${OZONE_PID_DIR}/ozone-${OZONE_SECURE_USER}-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}.pid"
  else
    daemon_outfile="${OZONE_LOG_DIR}/ozone-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.out"
    daemon_pidfile="${OZONE_PID_DIR}/ozone-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}.pid"
  fi

  # are we actually in daemon mode?
  # if yes, use the daemon logger and the appropriate log file.
  if [[ "${OZONE_DAEMON_MODE}" != "default" ]]; then
    OZONE_ROOT_LOGGER="${OZONE_DAEMON_ROOT_LOGGER}"
    if [[ "${OZONE_SUBCMD_SECURESERVICE}" = true ]]; then
      OZONE_LOGFILE="ozone-${OZONE_SECURE_USER}-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.log"
    else
      OZONE_LOGFILE="ozone-${OZONE_IDENT_STRING}-${OZONE_SUBCMD}-${HOSTNAME}.log"
    fi
  fi

  # finish defining the environment: system properties, env vars, class paths, etc.
  ozone_finalize

  # do the hard work of launching a daemon or just executing our interactive
  # java class
  if [[ "${OZONE_SUBCMD_SUPPORTDAEMONIZATION}" = true ]]; then
    if [[ "${OZONE_SUBCMD_SECURESERVICE}" = true ]]; then
      ozone_secure_daemon_handler \
        "${OZONE_DAEMON_MODE}" \
        "${OZONE_SUBCMD}" \
        "${OZONE_SECURE_CLASSNAME}" \
        "${daemon_pidfile}" \
        "${daemon_outfile}" \
        "${priv_pidfile}" \
        "${priv_outfile}" \
        "${priv_errfile}" \
        "${OZONE_SUBCMD_ARGS[@]}"
    else
      ozone_daemon_handler \
        "${OZONE_DAEMON_MODE}" \
        "${OZONE_SUBCMD}" \
        "${OZONE_CLASSNAME}" \
        "${daemon_pidfile}" \
        "${daemon_outfile}" \
        "${OZONE_SUBCMD_ARGS[@]}"
    fi
    exit $?
  else
    ozone_java_exec "${OZONE_SUBCMD}" "${OZONE_CLASSNAME}" "${OZONE_SUBCMD_ARGS[@]}"
  fi
}

## @description Utility function of ozone_validate_classpath
## @audience private
## @stability evolving
## @replaceable yes
function ozone_validate_classpath_util
{
  local CLASSPATH_FILE
  CLASSPATH_FILE="${OZONE_HOME}/share/ozone/classpath/${OZONE_RUN_ARTIFACT_NAME}.classpath"
  echo "Validating classpath file: $CLASSPATH_FILE"
  if [[ ! -e "$CLASSPATH_FILE" ]]; then
    echo "ERROR: Classpath file descriptor $CLASSPATH_FILE is missing"
    exit 1
  fi

  source "$CLASSPATH_FILE"
  OIFS=$IFS
  IFS=':'

  local found_missing_jars
  found_missing_jars=false
  for jar in $classpath; do
    if [[ ! -e "$jar" ]]; then
      found_missing_jars=true
      echo "ERROR: Jar file $jar is missing"
    fi
  done

  IFS=$OIFS

  if [[ "$found_missing_jars" == true && "$OZONE_VALIDATE_FAIL_ON_MISSING_JARS" == false ]]; then
    echo "Validation FAILED due to missing jar files! Continuing command execution..."
  elif [[ "$found_missing_jars" == true && "$OZONE_VALIDATE_FAIL_ON_MISSING_JARS" == true ]]; then
    echo "Validation FAILED due to missing jar files!"
    exit 1
  else
    echo "Validation SUCCESSFUL, all required jars are present!"
  fi
}

## @description Add items from .classpath file to the classpath
## @audience private
## @stability evolving
## @replaceable no
function ozone_add_classpath_from_file() {
  local classpath_file="$1"

  if [[ ! -e "$classpath_file" ]]; then
    echo "Skip non-existent classpath file: $classpath_file" >&2
    return
  fi

  local classpath
  # shellcheck disable=SC1090,SC2086
  source "$classpath_file"
  local original_ifs=$IFS
  IFS=':'

  local jar
  # shellcheck disable=SC2154
  for jar in $classpath; do
    ozone_add_classpath "$jar"
  done

  IFS=$original_ifs
}

## @description Add all the required jar files to the classpath
## @audience private
## @stability evolving
## @replaceable yes
function ozone_assemble_classpath() {
  #
  # Setting up classpath based on the generate classpath descriptors
  #
  if [[ -z "$OZONE_RUN_ARTIFACT_NAME" ]]; then
    echo "ERROR: Ozone components require to set OZONE_RUN_ARTIFACT_NAME to set the classpath"
    exit 255
  fi

  local CLASSPATH_FILE
  CLASSPATH_FILE="${OZONE_HOME}/share/ozone/classpath/${OZONE_RUN_ARTIFACT_NAME}.classpath"
  if [[ ! -e "$CLASSPATH_FILE" ]]; then
    echo "ERROR: Classpath file descriptor $CLASSPATH_FILE is missing"
    exit 255
  fi
  ozone_add_classpath_from_file "$CLASSPATH_FILE"
  ozone_add_classpath "${OZONE_HOME}/share/ozone/web"

  #Add optional jars to the classpath
  local OPTIONAL_CLASSPATH_DIR
  OPTIONAL_CLASSPATH_DIR="${HDDS_LIB_JARS_DIR}/${OZONE_RUN_ARTIFACT_NAME}"
  if [[ -d "$OPTIONAL_CLASSPATH_DIR" ]]; then
    ozone_add_classpath "$OPTIONAL_CLASSPATH_DIR/*"
  fi
}

## @description  Fallback to value of `oldvar` if `newvar` is undefined
## @audience     public
## @stability    stable
## @replaceable  no
## @param        oldvar
## @param        newvar
function ozone_deprecate_envvar
{
  local oldvar=$1
  local newvar=$2

  if ozone_set_var_for_compatibility "$newvar" "$oldvar" && \
    [[ "${OZONE_DEPRECATION_WARNING:-true}" != "false" ]]; then
    ozone_error "WARNING: ${oldvar} has been deprecated by ${newvar}."
  fi
}

## @description  Propagate value of `newvar` if `oldvar` is undefined
## @audience     public
## @stability    stable
## @replaceable  no
## @param        oldvar
## @param        newvar
function ozone_set_deprecated_var
{
  local oldvar=$1
  local newvar=$2

  if ozone_set_var_for_compatibility "$oldvar" "$newvar"; then
    ozone_debug "WARNING: Setting deprecated ${oldvar} to match ${newvar} for backward compatibility."
  fi
}

## @description  Make `targetvar` same as `sourcevar` if the former is undefined
## @audience     private
## @stability    stable
## @replaceable  no
## @param        targetvar
## @param        sourcevar
## @return       0 if `targetvar` was set
## @return       1 if `targetvar` was not updated (is already defined or `sourcevar` is undefined)
function ozone_set_var_for_compatibility
{
  local targetvar=$1
  local sourcevar=$2

  if ! declare -p "${targetvar}" >& /dev/null && declare -p "${sourcevar}" >& /dev/null; then
    local targetval=${!targetvar}
    local sourceval=${!sourcevar}

    ozone_debug "${targetvar} = ${sourceval}"

    # shellcheck disable=SC2086
    eval ${targetvar}=\"${sourceval}\"

    return 0
  fi

  return 1
}

## @description  Initialize OZONE_x variables from HADOOP_x
## @audience     private
## @stability    stable
## @replaceable  no
## @param        [suffixes]
function ozone_deprecate_hadoop_vars
{
  for suffix in "$@"; do
    ozone_deprecate_envvar "HADOOP_${suffix}" "OZONE_${suffix}"
  done
}

## @description  Set HADOOP_x variables from OZONE_x
## @audience     private
## @stability    stable
## @replaceable  no
## @param        [suffixes]
function ozone_set_deprecated_hadoop_vars
{
  for suffix in "$@"; do
    ozone_set_deprecated_var "HADOOP_${suffix}" "OZONE_${suffix}"
  done
}
