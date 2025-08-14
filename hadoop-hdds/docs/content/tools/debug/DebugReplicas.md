---
title: "Debug Replicas"
date: 2025-07-28
summary: Debug commands for replica-related issues.
menu: debug
weight: 4
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Debug commands for retrieving information and performing various checks on the key replicas in datanodes.

```bash
Usage: ozone debug replicas [--verbose] [COMMAND]
Debug commands for replica-related issues, retrieving replica information from
the OM and performing checks over the network against a running cluster.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  chunk-info  Returns chunk location information about an existing key
  verify      Run checks to verify data across replicas. By default prints only
                the keys with failed checks.
```

## chunk-info
For a given URI of a key, the command returns all the chunks' location information.

```bash
Usage: ozone debug replicas chunk-info [-hV] [--verbose] <value>
Returns chunk location information about an existing key
      <value>     URI of the key (format: volume/bucket/key).
                  Ozone URI could either be a full URI or short URI.
                  Full URI should start with o3://, in case of non-HA
                  clusters it should be followed by the host name and
                  optionally the port number. In case of HA clusters
                  the service id should be used. Service id provides a
                  logical name for multiple hosts and it is defined
                  in the property ozone.om.service.ids.
                  Example of a full URI with host name and port number
                  for a key:
                  o3://omhostname:9862/vol1/bucket1/key1
                  With a service id for a volume:
                  o3://omserviceid/vol1/
                  Short URI should start from the volume.
                  Example of a short URI for a bucket:
                  vol1/bucket1
                  Any unspecified information will be identified from
                  the config files.

  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
```

## verify
Verify data across replicas. There are multiple checks available, which can be selected using the command line options:

```bash 
Usage: ozone debug replicas verify [-hV] [--all-results] [--verbose]
                                   [--container-cache-size=<containerCacheSize>]
                                    [-id=<scmServiceId>] [--scm=<scm>]
                                   ([--checksums] [--block-existence]
                                   [--container-state]) <uri>
Run checks to verify data across replicas. By default prints only the keys with
failed checks.
      <uri>               Ozone URI could either be a full URI or short URI.
                          Full URI should start with o3://, in case of non-HA
                          clusters it should be followed by the host name and
                          optionally the port number. In case of HA clusters
                          the service id should be used. Service id provides a
                          logical name for multiple hosts and it is defined
                          in the property ozone.om.service.ids.
                          Example of a full URI with host name and port number
                          for a key:
                          o3://omhostname:9862/vol1/bucket1/key1
                          With a service id for a volume:
                          o3://omserviceid/vol1/
                          Short URI should start from the volume.
                          Example of a short URI for a bucket:
                          vol1/bucket1
                          Any unspecified information will be identified from
                          the config files.

      --all-results       Print results for all passing and failing keys
      --block-existence   Check for block existence on datanodes.
      --checksums         Do client side data checksum validation of all
                            replicas.
      --container-cache-size=<containerCacheSize>
                          Size (in number of containers) of the in-memory cache
                            for container state verification
                            '--container-state'. Default is 1 million
                            containers (which takes around 43MB). Value must be
                            greater than zero, otherwise the default of 1
                            million is considered. Note: This option is ignored
                            if '--container-state' option is not used.
      --container-state   Check the container and replica states. Containers in
                            [DELETING, DELETED] states, or it's replicas in
                            [DELETED, UNHEALTHY, INVALID] states fail the check.
  -h, --help              Show this help message and exit.
      -id, --service-id=<scmServiceId>
                          ServiceId of SCM HA Cluster
      --scm=<scm>         The destination scm (host:port)
  -V, --version           Print version information and exit.
      --verbose           More verbose output. Show the stack trace of the
                            errors.
```
