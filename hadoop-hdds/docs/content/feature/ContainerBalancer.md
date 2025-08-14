---
title: Container Balancer
menu:
  main:
    parent: Features
summary: How to use the Container Balancer in Ozone.
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


## Overview

The Container Balancer is a tool in Apache Ozone that balances data containers across the cluster. Its primary goal is to ensure an even distribution of data based on disk space usage on datanodes. This helps to prevent some datanodes from becoming full while others remain underutilized.

The balancer operates by moving `CLOSED` container replicas, which means it doesn't interfere with active I/O operations. It is designed to work with both regular and Erasure Coded (EC) containers. To maintain cluster stability, the Container Balancer's startup is delayed after a Storage Container Manager (SCM) failover.

## Command Line Usage

The Container Balancer is managed through the `ozone admin containerbalancer` command.

### Start

To start the Container Balancer with default settings:

```bash
ozone admin containerbalancer start
```

You can also start the balancer with specific options:

```bash
ozone admin containerbalancer start [options]
```

**Options:**

| Option                                                | Description                                                                                                                            |
|-------------------------------------------------------| -------------------------------------------------------------------------------------------------------------------------------------- |
| `-t`, `--threshold`                                   | The percentage deviation from the average utilization of the cluster after which a datanode will be rebalanced. Default is 10%.          |
| `-i`, `--iterations`                                  | The maximum number of consecutive iterations the balancer will run for. Default is 10. Use -1 for infinite iterations.                 |
| `-d`, `--maxDatanodesPercentageToInvolvePerIteration` | The maximum percentage of healthy, in-service datanodes that can be involved in balancing in one iteration. Default is 20%.      |
| `-s`, `--maxSizeToMovePerIterationInGB`               | The maximum size of data in GB to be moved in one iteration. Default is 500GB.                                                         |
| `-e`, `--maxSizeEnteringTargetInGB`                   | The maximum size in GB that can enter a target datanode in one iteration. Default is 26GB.                                             |
| `-l`, `--maxSizeLeavingSourceInGB`                    | The maximum size in GB that can leave a source datanode in one iteration. Default is 26GB.                                             |
| `--balancing-iteration-interval-minutes`              | The interval in minutes between each iteration of the Container Balancer. Default is 70 minutes.                                       |
| `--move-timeout-minutes`                              | The time in minutes to allow a single container to move from source to target. Default is 65 minutes.                                  |
| `--move-replication-timeout-minutes`                  | The time in minutes to allow a single container's replication from source to target as part of a container move. Default is 50 minutes. |
| `--move-network-topology-enable`                      | Whether to consider network topology when selecting a target for a source. Default is false.                                           |
| `--include-datanodes`                                 | A comma-separated list of datanode hostnames or IP addresses to be included in balancing.                                              |
| `--exclude-datanodes`                                 | A comma-separated list of datanode hostnames or IP addresses to be excluded from balancing.                                            |

### Status

To check the status of the Container Balancer:

```bash
ozone admin containerbalancer status
```

To get a more detailed status, including the history of iterations:

```bash
ozone admin containerbalancer status -v --history
```

### Stop

To stop the Container Balancer:

```bash
ozone admin containerbalancer stop
```

## Configuration

The Container Balancer can also be configured through the `ozone-site.xml` file.

| Property                                               | Description                                                                                                                            | Default Value |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `hdds.container.balancer.utilization.threshold`        | A cluster is considered balanced if for each datanode, the utilization of the datanode differs from the utilization of the cluster no more than this threshold. | 10%           |
| `hdds.container.balancer.datanodes.involved.max.percentage.per.iteration` | Maximum percentage of healthy, in-service datanodes that can be involved in balancing in one iteration.                                | 20%           |
| `hdds.container.balancer.size.moved.max.per.iteration` | The maximum size of data that will be moved by Container Balancer in one iteration.                                                    | 500GB         |
| `hdds.container.balancer.size.entering.target.max`     | The maximum size that can enter a target datanode in each iteration.                                                                   | 26GB          |
| `hdds.container.balancer.size.leaving.source.max`      | The maximum size that can leave a source datanode in each iteration.                                                                   | 26GB          |
| `hdds.container.balancer.iterations`                   | The number of iterations that Container Balancer will run for.                                                                         | 10            |
| `hdds.container.balancer.exclude.containers`           | A comma-separated list of container IDs to exclude from balancing.                                                                     | ""            |
| `hdds.container.balancer.move.timeout`                 | The amount of time to allow a single container to move from source to target.                                                          | 65m           |
| `hdds.container.balancer.move.replication.timeout`     | The amount of time to allow a single container's replication from source to target as part of a container move.                        | 50m           |
| `hdds.container.balancer.balancing.iteration.interval` | The interval period between each iteration of Container Balancer.                                                                      | 70m           |
| `hdds.container.balancer.include.datanodes`            | A comma-separated list of Datanode hostnames or IP addresses. Only the Datanodes specified in this list are balanced.                  | ""            |
| `hdds.container.balancer.exclude.datanodes`            | A comma-separated list of Datanode hostnames or IP addresses. The Datanodes specified in this list are excluded from balancing.        | ""            |
| `hdds.container.balancer.move.networkTopology.enable`  | Whether to take network topology into account when selecting a target for a source.                                                    | false         |
| `hdds.container.balancer.trigger.du.before.move.enable`| Whether to send a command to all healthy and in-service data nodes to run `du` immediately before starting a balance iteration.        | false         |
