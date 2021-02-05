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

# included in all the ozone scripts with source command
# should not be executed directly

function ozone_subproject_init
{
  if [[ -z "${OZONE_ENV_PROCESSED}" ]]; then
    if [[ -e "${OZONE_CONF_DIR}/ozone-env.sh" ]]; then
      . "${OZONE_CONF_DIR}/ozone-env.sh"
      export OZONE_ENV_PROCESSED=true
    fi
  fi
}

if [[ -z "${OZONE_LIBEXEC_DIR}" ]]; then
  _this="${BASH_SOURCE-$0}"
  OZONE_LIBEXEC_DIR=$(cd -P -- "$(dirname -- "${_this}")" >/dev/null && pwd -P)
fi

#
# IMPORTANT! We are not executing user provided code yet!
#

# Let's go!  Base definitions so we can move forward
ozone_bootstrap

# let's find our conf.
#
# first, check and process params passed to us
# we process this in-line so that we can directly modify $@
# if something downstream is processing that directly,
# we need to make sure our params have been ripped out
# note that we do many of them here for various utilities.
# this provides consistency and forces a more consistent
# user experience


# save these off in case our caller needs them
# shellcheck disable=SC2034
OZONE_USER_PARAMS=("$@")

ozone_parse_args "$@"
shift "${OZONE_PARSE_COUNTER}"

#
# Setup the base-line environment
#
ozone_find_confdir
ozone_exec_ozoneenv
ozone_import_shellprofiles
ozone_exec_userfuncs

#
# IMPORTANT! User provided code is now available!
#

ozone_exec_user_env

ozone_deprecate_envvar HADOOP_SLAVES HADOOP_WORKERS
ozone_deprecate_envvar HADOOP_SLAVE_NAMES HADOOP_WORKER_NAMES
ozone_deprecate_envvar HADOOP_SLAVE_SLEEP HADOOP_WORKER_SLEEP

# do all the OS-specific startup bits here
# this allows us to get a decent JAVA_HOME,
# call crle for LD_LIBRARY_PATH, etc.
ozone_os_tricks

ozone_java_setup

ozone_basic_init

# inject any sub-project overrides, defaults, etc.
if declare -F ozone_subproject_init >/dev/null ; then
  ozone_subproject_init
fi

ozone_shellprofiles_init

# get the basic java class path for these subprojects
# in as quickly as possible since other stuff
# will definitely depend upon it.

ozone_shellprofiles_classpath

# user API commands can now be run since the runtime
# environment has been configured
ozone_exec_ozonerc
