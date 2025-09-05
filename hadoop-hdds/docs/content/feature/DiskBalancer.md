---
title: "DiskBalancer"
weight: 1
menu:
   main:
      parent: Features
summary: DiskBalancer For DataNodes.
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
**Apache Ozone** works well to distribute all containers evenly across all multiple disks on each Datanode.
This initial spread ensures that I/O load is balanced from the start. However, over the operational lifetime of a
cluster **disk imbalance** can occur due to the following reasons:
- **Adding new disks** to expand datanode storage space.
- **Replacing old broken disks** with new disks.
- Massive **block** or **replica deletions**.

This uneven utilisation of disks can create performance bottlenecks, as **over-utilised disks** become **hotspots** 
limiting the overall throughput of the Datanode. As a result, this new feature, **DiskBalancer**, is introduced to
ensure even data distribution across disks within a Datanode.

A disk is considered a candidate for balancing if its
`VolumeDataDensity` exceeds a configurable `threshold`. DiskBalancer can be triggered manually by **CLI commands**.


![Data spread across disks](diskBalancer.png)

## Feature Flag

The Disk Balancer feature is introduced with a feature flag. By default, this feature is disabled to prevent it from
running until it has undergone thorough testing.

The feature can be **enabled** by setting the following property to `true` in the `ozone-site.xml` configuration file:
`hdds.datanode.disk.balancer.enabled = false`

## Command Line Usage
The DiskBalancer is managed through the `ozone admin datanode diskbalancer` command.

**Note:** This command is hidden from the main help message (`ozone admin datanode --help`). This is because the feature
is currently considered experimental and is disabled by default. Hiding the command prevents accidental use and keeps
the help output clean for general users. The command is, however, fully functional for those who wish to enable and use
the feature.

### **Start DiskBalancer**
To start diskBalancer on all Datanodes with default configurations :

```shell
ozone admin datanode diskbalancer start -a
```

You can also start DiskBalancer with specific options:

```shell
ozone admin datanode diskbalancer start [options]
```

### **Update Configurations**
To update DiskBalancer configurations you can use following command:

```shell
ozone admin datanode diskbalancer update [options]
```
**Options include:**

| Options                      | Description                                                                                           |                                                                                                                                                             
|------------------------------|-------------------------------------------------------------------------------------------------------|
| `-t, --threshold`            | Percentage deviation from average utilization of the disks after which a datanode will be rebalanced. |
| `-b, --bandwith-in-mb`       | Maximum bandwidth for DiskBalancer per second.                                                        |
| `-p, --parallel-thread`      | Max parallelThread for DiskBalancer.                                                                  |
| `-s, --stop-after-disk-even` | Stop DiskBalancer automatically after disk utilization is even.                                       |
| `-a, --all`                  | Run commands on all datanodes.                                                                        |
| `-d, --datanodes`            | Run commands on specific datanodes                                                                    |

### **Stop DiskBalancer**
To stop DiskBalancer on all Datanodes:

```shell
ozone admin datanode diskbalancer stop -a
```
You can also stop DiskBalancer on specific Datanodes:

```shell
ozone admin datanode diskbalancer stop -d <datanode1>
```
### **DiskBalancer Status**
To check the status of DiskBalancer on all Datanodes:

```shell
ozone admin datanode diskbalancer status
```
You can also check status of DiskBalancer on specific Datanodes:

```shell
ozone admin datanode diskbalancer status -d <datanode1>
```
### **DiskBalancer Report**
To get a **volumeDataDensity** of DiskBalancer of top **N** Datanodes (displayed in descending order),
by default N=25, if not specified:

```shell
ozone admin datanode diskbalancer report --count <N>
```

## **DiskBalancer Configurations**

The DiskBalancer's behavior can be controlled using the following configuration properties in `ozone-site.xml`.

| Property                                                    | Default Value                                                                          | Description                                                                                                                                                               |
|-------------------------------------------------------------|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hdds.datanode.disk.balancer.enabled`                       | `false`                                                                                | if false, the DiskBalancer service on the Datanode is disabled. Configure it to true for diskBalancer to be enabled.                                                      |                                                            |                                                                                        |                                                                                                                                                                              |
| `hdds.datanode.disk.balancer.volume.density.threshold`      | `10.0`                                                                                 | A percentage (0-100). A datanode is considered balanced if for each volume, its utilization differs from the average datanode utilization by no more than this threshold. |
| `hdds.datanode.disk.balancer.max.disk.throughputInMBPerSec` | `10`                                                                                   | The maximum bandwidth (in MB/s) that the balancer can use for moving data, to avoid impacting client I/O.                                                                 |
| `hdds.datanode.disk.balancer.parallel.thread`               | `5`                                                                                    | The number of worker threads to use for moving containers in parallel.                                                                                                    |
| `hdds.datanode.disk.balancer.service.interval`              | `60s`                                                                                  | The time interval at which the Datanode DiskBalancer service checks for imbalance and updates its configuration.                                                          |
| `hdds.datanode.disk.balancer.stop.after.disk.even`          | `true`                                                                                 | If true, the DiskBalancer will automatically stop its balancing activity once disks are considered balanced (i.e., all volume densities are within the threshold).        |
| `hdds.datanode.disk.balancer.volume.choosing.policy`        | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy`    | The policy class for selecting source and destination volumes for balancing.                                                                                              |
| `hdds.datanode.disk.balancer.container.choosing.policy`     | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy` | The policy class for selecting which containers to move from a source volume to destination volume.                                                                       |
| `hdds.datanode.disk.balancer.service.timeout`               | `300s`                                                                                 | Timeout for the Datanode DiskBalancer service operations.                                                                                                                 |
| `hdds.datanode.disk.balancer.should.run.default`            | `false`                                                                                | If the balancer fails to read its persisted configuration, this value determines if the service should run by default.                                                    |

