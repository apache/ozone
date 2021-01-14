<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Compose file with optional monitoring and profiling configs

This directory contains a docker-compose definition for an Ozone cluster with all components (including S3 Gateway and Recon).

There are two optional add-ons:

 * monitoring: adds Grafana, Jaeger and Prometheus services, and configures Ozone to work with them
 * profiling: allows sampling Ozone CPU/memory using [async-profiler](https://github.com/jvm-profiling-tools/async-profiler)

## How to start

TL;DR:

1. single datanode:
   ```
   ./run.sh -d
   ```
2. three datanodes for replication:
   ```
   export OZONE_REPLICATION_FACTOR=3
   ./run.sh -d
   ```

### Basics

The cluster can be started with regular `docker-compose up` command.  Use `-d` to start the cluster in the background.

You can change the number of datanodes to start using the `--scale` option.  Eg. to start 3 datanodes: `docker-compose up -d --scale datanode=3`.

The cluster's replication factor (1 or 3) can be controlled by setting the `OZONE_REPLICATION_FACTOR` environment variable.  It defaults to 1 to match the number of datanodes started by default, without the `--scale` option.

For convenience the `run.sh` script can be used to make sure the replication factor and the number of datanodes match.  It also passes any additional arguments provided on the command-line (eg. `-d`) to `docker-compose`.

### Add-ons

Monitoring and/or performance add-ons can be enabled via docker-compose's ability to use multiple compose files (by using the [`-f` option repeatedly](https://docs.docker.com/compose/reference/overview/#specifying-multiple-compose-files), or more easily by defining the [`COMPOSE_FILE` environment variable](https://docs.docker.com/compose/reference/envvars/#compose_file)):

```
# no COMPOSE_FILE var                                                  # => only Ozone
export COMPOSE_FILE=docker-compose.yaml:monitoring.yaml                # => add monitoring
export COMPOSE_FILE=docker-compose.yaml:profiling.yaml                 # => add profiling
export COMPOSE_FILE=docker-compose.yaml:monitoring.yaml:profiling.yaml # => add both
```

Once the variable is defined, Ozone cluster with add-ons can be started/scaled/stopped etc. using the same `docker-compose` commands as for the base cluster.

### Load generator

Ozone comes with a load generator called Freon.

You can enter one of the containers (eg. SCM) and start a Freon test:

```
docker-compose exec scm bash
ozone freon ockg -n1000
```

You can also start two flavors of Freon as separate services, which allows scaling them up.  Once all the datanodes are started, start Freon by adding its definition to `COMPOSE_FILE` and re-running the `docker-compose up` or `run.sh` command:

```
export COMPOSE_FILE="${COMPOSE_FILE}:freon-ockg.yaml"

docker-compose up -d --no-recreate --scale datanode=3
# OR
./run.sh -d
```

## How to use

You can check the ozone web ui:

OzoneManager: http://localhost:9874
SCM: http://localhost:9876

### Monitoring

 * Prometheus: follows a pull based approach where metrics are published on an HTTP endpoint.  Metrics can be checked on [Prometheus' web UI](http://localhost:9090/)
 * Grafana: comes with two [dashboards](http://localhost:3000) for Ozone
   * Ozone - Object Metrics
   * Ozone - RPC Metrics
 * Jaeger: collects distributed tracing information from Ozone, can be queried on the [Jaeger web UI](http://localhost:16686)

### Profiling

Start by hitting the `/prof` endpoint on the service to be profiled, eg. http://localhost:9876/prof for SCM.  [Detailed instructions](https://cwiki.apache.org/confluence/display/HADOOP/Java+Profiling+of+Ozone) can be found in the Hadoop wiki.
