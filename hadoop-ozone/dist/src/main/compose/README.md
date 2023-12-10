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

# Docker cluster definitions

This directory contains Docker Compose cluster definitions that allow running Ozone in a pseudo multi-node environment locally.

These provide:

 * examples of how to configure various Ozone services in different environments (HA/non-HA, secure/unsecure, etc.)
 * an environment for developers and CI workflows to test Ozone
 * playground for experiments

Obviously none of these definitions are for production.

## Structure

Clusters are defined in `docker-compose.yaml`, cluster-wide settings are externalized in `docker-config` (see [envtoconf](https://github.com/flokkr/launcher#envtoconf-simple-configuration-loading) for syntax details).

## Usage

Standard `docker-compose` commands can be used to start, stop, or destroy the cluster, check logs, or execute commands in the containers.

Most environments allow scaling the number of datanodes.

Key ports (web UI, RPC) are usually published on the docker host.  Datanode ports are published on random local ports, since there can be multiple instances of the same service.

### Starting the Cluster

```
docker-compose up -d
```

### Checking Service Logs

```
docker-compose logs              # all services
docker-compose logs <service>    # specific service, e.g. `scm`
docker-compose logs -f <service> # follow logs, as in `tail -f`
```

### Scaling Services

```
docker-compose up -d --no-restart --scale datanode=5
```

### Executing Commands

```
docker-compose exec <service> ozone version # one-off command
docker-compose exec <service> bash          # interactive shell
```

### Stopping the Cluster

```
docker-compose stop <service>
docker-compose start <service> # start specific service (again)
docker-compose stop            # stop the cluster
docker-compose down  -v        # stop and destroy
```

## Tests

Most subdirectories contain `test.sh`, some also have other `test-*.sh` scripts.  These are run in CI by Github Actions, but can also be run locally (requires: bash, docker, docker-compose, jq).

Tests are implemented in `../smoketest/` using Robot Framework.

To add an acceptance test with specific Ozone configuration, create a shell script that follows the naming convention `test-*.sh`, and add your custom confing in a YAML file.  See `test-legacy-bucket.sh` and `legacy-bucket.yaml` in `ozone/` for example.
