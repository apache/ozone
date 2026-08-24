#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Ozone-specific environment variables here.

# Enable core dump when crash in C++.
# Ozone may invoke native components (e.g., RocksDB, JNI, or C++ libraries).
# When a native crash occurs, enabling core dumps can help debugging.
#
# However, raising the core file size limit is not always allowed.
# - 'ulimit -Hc' returns the hard limit for core size.
#   If the hard limit is 0, core dumps are explicitly forbidden
#   (common for non-root users or Docker containers started with
#   '--ulimit core=0'). In such environments, attempting
#   'ulimit -c unlimited' would always fail and print a warning.
#
# To avoid noisy warnings during CLI execution (e.g., ozone version),
# we only attempt to raise the limit when the hard limit is non-zero,
# and we silence any possible error for safety.
if [ "$(ulimit -Hc 2>/dev/null || echo 0)" != 0 ]; then
  ulimit -c unlimited 2>/dev/null || true
fi

# Many of the options here are built from the perspective that users
# may want to provide OVERWRITING values on the command line.
# For example:
#
#  JAVA_HOME=/usr/java/testing ozone fs -ls
#
# Therefore, the vast majority (BUT NOT ALL!) of these defaults
# are configured for substitution and not append.  If append
# is preferable, modify this file accordingly.

###
# Generic settings
###

# Technically, the only required environment variable is JAVA_HOME.
# All others are optional.  However, the defaults are probably not
# preferred.  Many sites configure these options outside of Ozone,
# such as in /etc/profile.d

# The java implementation to use. By default, this environment
# variable is REQUIRED on ALL platforms except OS X!
# export JAVA_HOME=

# Location of Ozone.  By default, Ozone will attempt to determine
# this location based upon its execution path.
# export OZONE_HOME=

# Location of Ozone's configuration information.  i.e., where this
# file is living. If this is not defined, Ozone will attempt to
# locate it based upon its execution path.
#
# NOTE: It is recommend that this variable not be set here but in
# /etc/profile.d or equivalent.  Some options (such as
# --config) may react strangely otherwise.
#
# export OZONE_CONF_DIR=${OZONE_HOME}/etc/hadoop

# The maximum amount of heap to use (Java -Xmx).  If no unit
# is provided, it will be converted to MB.  Daemons will
# prefer any Xmx setting in their respective _OPT variable.
# There is no default; the JVM will autoscale based upon machine
# memory size.
# export OZONE_HEAPSIZE_MAX=

# The minimum amount of heap to use (Java -Xms).  If no unit
# is provided, it will be converted to MB.  Daemons will
# prefer any Xms setting in their respective _OPT variable.
# There is no default; the JVM will autoscale based upon machine
# memory size.
# export OZONE_HEAPSIZE_MIN=

# Extra Java runtime options for all Ozone commands. We don't support
# IPv6 yet/still, so by default the preference is set to IPv4.
# export OZONE_OPTS="-Djava.net.preferIPv4Stack=true"
# For Kerberos debugging, an extended option set logs more information
# export OZONE_OPTS="-Djava.net.preferIPv4Stack=true -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug"

# Some parts of the shell code may do special things dependent upon
# the operating system.  We have to set this here. See the next
# section as to why....
export OZONE_OS_TYPE=${OZONE_OS_TYPE:-$(uname -s)}

# Extra Java runtime options for some Ozone commands
# and clients (e.g., ozone sh).  These get appended to OZONE_OPTS for
# such commands.  In most cases, this should be left empty and
# let users supply it on the command line.
# export OZONE_CLIENT_OPTS=""

#
# A note about classpaths.
#
# By default, Apache Ozone overrides Java's CLASSPATH
# environment variable.  It is configured such
# that it starts out blank with new entries added after passing
# a series of checks (file/dir exists, not already listed aka
# de-deduplication).  During de-deduplication, wildcards and/or
# directories are *NOT* expanded to keep it simple. Therefore,
# if the computed classpath has two specific mentions of
# awesome-methods-1.0.jar, only the first one added will be seen.
# If two directories are in the classpath that both contain
# awesome-methods-1.0.jar, then Java will pick up both versions.

# An additional, custom CLASSPATH. Site-wide configs should be
# handled via the shellprofile functionality, utilizing the
# ozone_add_classpath function for greater control and much
# harder for apps/end-users to accidentally override.
# Similarly, end users should utilize ${HOME}/.ozonerc .
# This variable should ideally only be used as a short-cut,
# interactive way for temporary additions on the command line.
# export OZONE_CLASSPATH="/some/cool/path/on/your/machine"

# Should OZONE_CLASSPATH be first in the official CLASSPATH?
# export OZONE_USER_CLASSPATH_FIRST="yes"

# If OZONE_USE_CLIENT_CLASSLOADER is set, OZONE_CLASSPATH and
# OZONE_USER_CLASSPATH_FIRST are ignored.
# export OZONE_USE_CLIENT_CLASSLOADER=true

###
# Options for remote shell connectivity
###

# There are some optional components of ozone that allow for
# command and control of remote hosts.  For example,
# start-ozone.sh will attempt to bring up all OMs, SCMs, DNs, etc.

# Options to pass to SSH when one of the "log into a host and
# start/stop daemons" scripts is executed
# export OZONE_SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s"

# The built-in ssh handler will limit itself to 10 simultaneous connections.
# For pdsh users, this sets the fanout size ( -f )
# Change this to increase/decrease as necessary.
# export OZONE_SSH_PARALLEL=10

# Filename which contains all of the hosts for any remote execution
# helper scripts # such as workers.sh, start-ozone.sh, etc.
# export OZONE_WORKERS="${OZONE_CONF_DIR}/workers"

###
# Options for all daemons
###
#

#
# Many options may also be specified as Java properties.  It is
# very common, and in many cases, desirable, to hard-set these
# in daemon _OPTS variables.  Where applicable, the appropriate
# Java property is also identified.  Note that many are re-used
# or set differently in certain contexts (e.g., secure vs
# non-secure)
#

# Where (primarily) daemon log files are stored.
# ${OZONE_HOME}/logs by default.
# Java property: hadoop.log.dir
# export OZONE_LOG_DIR=${OZONE_HOME}/logs

# A string representing this instance of Ozone. $USER by default.
# This is used in writing log and pid files, so keep that in mind!
# Java property: hadoop.id.str
# export OZONE_IDENT_STRING=$USER

# How many seconds to pause after stopping a daemon
# export OZONE_STOP_TIMEOUT=5

# Where pid files are stored.  /tmp by default.
# export OZONE_PID_DIR=/tmp

# Default log4j setting for interactive commands
# Java property: hadoop.root.logger
# export OZONE_ROOT_LOGGER=INFO,console

# Default log4j setting for daemons spawned explicitly by
# --daemon option of ozone command.
# Java property: hadoop.root.logger
# export OZONE_DAEMON_ROOT_LOGGER=INFO,RFA

# Default log level and output location for security-related messages.
# You will almost certainly want to change this on a per-daemon basis via
# the Java property (i.e., -Dhadoop.security.logger=foo).
# Java property: hadoop.security.logger
# export OZONE_SECURITY_LOGGER=INFO,NullAppender

# Default process priority level
# Note that sub-processes will also run at this level!
# export OZONE_NICENESS=0

# Default name for the service level authorization file
# Java property: hadoop.policy.file
# export OZONE_POLICYFILE="hadoop-policy.xml"

#
# NOTE: this is not used by default!  <-----
# You can define variables right here and then re-use them later on.
# For example, it is common to use the same garbage collection settings
# for all the daemons.  So one could define:
#
# export OZONE_GC_SETTINGS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
#
# .. and then use it when setting OZONE_OM_OPTS, etc. below

###
# Secure/privileged execution
###

#
# Out of the box, Ozone uses jsvc from Apache Commons to launch daemons
# on privileged ports.  This functionality can be replaced by providing
# custom functions.  See ozone-functions.sh for more information.
#

# The jsvc implementation to use. Jsvc is required to run secure datanodes
# that bind to privileged ports to provide authentication of data transfer
# protocol.  Jsvc is not required if SASL is configured for authentication of
# data transfer protocol using non-privileged ports.
# export JSVC_HOME=/usr/bin

#
# This directory contains pids for secure and privileged processes.
#export OZONE_SECURE_PID_DIR=${OZONE_PID_DIR}

#
# This directory contains the logs for secure and privileged processes.
# Java property: hadoop.log.dir
# export OZONE_SECURE_LOG=${OZONE_LOG_DIR}

#
# When running a secure daemon, the default value of OZONE_IDENT_STRING
# ends up being a bit bogus.  Therefore, by default, the code will
# replace OZONE_IDENT_STRING with OZONE_xx_SECURE_USER.  If one wants
# to keep OZONE_IDENT_STRING untouched, then uncomment this line.
# export OZONE_SECURE_IDENT_PRESERVE="true"

###
# Ozone Manager specific parameters
###
# Specify the JVM options to be used when starting the Ozone Manager.
# These options will be appended to the options specified as OZONE_OPTS
# and therefore may override any similar flags set in OZONE_OPTS
#
# export OZONE_OM_OPTS=""

###
# Ozone DataNode specific parameters
###
# Specify the JVM options to be used when starting Ozone DataNodes.
# These options will be appended to the options specified as OZONE_OPTS
# and therefore may override any similar flags set in OZONE_OPTS
#
# export OZONE_DATANODE_OPTS=""

###
# HDFS StorageContainerManager specific parameters
###
# Specify the JVM options to be used when starting the HDFS Storage Container Manager.
# These options will be appended to the options specified as OZONE_OPTS
# and therefore may override any similar flags set in OZONE_OPTS
#
# export OZONE_SCM_OPTS=""

###
# Advanced Users Only!
###

#
# When building Ozone, one can add the class paths to the commands
# via this special env var:
# export OZONE_ENABLE_BUILD_PATHS="true"

#
# To prevent accidents, shell commands be (superficially) locked
# to only allow certain users to execute certain subcommands.
# It uses the format of (command)_(subcommand)_USER.
#
# For example, to limit who can execute the om command,
# export OZONE_OM_USER=ozone
