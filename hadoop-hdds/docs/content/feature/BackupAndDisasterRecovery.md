---
title: "Backup and Disaster Recovery"
weight: 2
menu:
   main:
      parent: Features
summary: Backup and Disaster Recovery Strategy Guide for Ozone.
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

## Backup and Disaster Recovery Strategy Guide

### System Prerequisites

Before implementing the strategies in this guide, please ensure your environment meets the following prerequisites:

*   **Two Ozone Clusters:** A primary cluster and a secondary (DR) cluster in a separate geographical location.
*   **Hadoop 3.4.0 or newer:** The `hadoop distcp -diff` command with Ozone snapshots is supported in Hadoop 3.4.0 and later versions.
*   **Network Connectivity:** The primary and DR clusters must have network connectivity to allow `distcp` to transfer data.
*   **Ozone Snapshot Feature Enabled:** The snapshot feature must be enabled on your Ozone clusters.

This guide provides a high-level overview of backup and disaster recovery (DR) strategies for Apache Ozone, with a focus on leveraging the snapshot feature.

### Best Practices for Using Snapshots

Ozone's snapshot feature is a powerful tool for data protection. Here are some best practices to follow:

*   **Regularly Scheduled Snapshots:** Automate the creation of snapshots on a regular schedule (e.g., hourly, daily, weekly). The frequency should be determined by your Recovery Point Objective (RPO).

    ```bash
    # Create a daily snapshot with a timestamp
    ozone sh snapshot create /vol1/bucket1 snapshot-`date +%Y-%m-%d`
    ```

*   **Retention Policies:** Establish a clear retention policy for your snapshots. Don't keep snapshots indefinitely. A common practice is to keep a larger number of recent snapshots and gradually reduce the number of older snapshots.

*   **Monitor Snapshot Creation:** Ensure that your snapshot creation process is monitored. Alerts should be configured to notify administrators if a snapshot fails.

*   **Test Your Backups:** Regularly test your backups by restoring a snapshot to a non-production environment. This ensures that your backups are valid and that your recovery procedure works as expected.

*   **Secure Your Snapshots:** Snapshots are a critical part of your DR strategy. Ensure that access to snapshot creation and deletion is restricted to authorized personnel.

### Cross-Cluster Replication with DistCp and Snapshot Diffs

> **Note on Replication Granularity:** Ozone snapshots are taken at the bucket level. Therefore, incremental replication using `hadoop distcp -diff` is also performed at the bucket level. While full cluster or volume-level *replication* is not directly supported by this method, full cluster *recovery* or volume-level *recovery* is still achievable by restoring individual buckets or promoting a DR cluster. To back up an entire cluster, it is recommended to set up multiple replication schedules, one for each bucket.

For disaster recovery, it's essential to have a copy of your data in a separate, geographically distant location. Ozone's native snapshot capability can be used with `hadoop distcp -diff` to efficiently replicate data between clusters by sending only the differences between two snapshots.

> **Note:** Support for `distcp -diff` with Ozone snapshots was added in Hadoop 3.4.0. Please ensure your Hadoop version is 3.4.0 or newer.

The basic workflow is as follows:

1.  **Initial Data Copy:** Perform an initial `DistCp` of the entire bucket to the DR cluster. Assume the source cluster is `ozone-1` and the destination is `ozone-2`.

    ```bash
    hadoop distcp o3fs://bucket1.vol1.ozone-1/ o3fs://bucket1.vol1.ozone-2/
    ```

2.  **Create Initial Snapshot:** On both the primary and DR clusters, create a snapshot with the same name.

    ```bash
    # Primary Cluster
    ozone sh snapshot create /vol1/bucket1 snap1
    # DR Cluster
    ozone sh snapshot create /vol1/bucket1 snap1
    ```

3.  **Incremental Replication:**
    a. After some data changes on the primary cluster, create a new snapshot.

    ```bash
    ozone sh snapshot create /vol1/bucket1 snap2
    ```

    b. Use `hadoop distcp -update -diff` to replicate the changes to the DR cluster. This command compares `snap1` and `snap2` on the source and applies the differences to the destination.

    ```bash
    hadoop distcp -update -diff o3fs://bucket1.vol1.ozone-1/.snapshot/snap1 o3fs://bucket1.vol1.ozone-1/.snapshot/snap2 o3fs://bucket1.vol1.ozone-2/
    ```

    c. After the `distcp` is complete, create the `snap2` snapshot on the DR cluster to prepare for the next incremental update.

    ```bash
    ozone sh snapshot create /vol1/bucket1 snap2
    ```

This approach significantly reduces the amount of data that needs to be transferred over the network, making it a highly efficient method for keeping a DR site in sync.

### Automating with Cron Jobs

To ensure regular backups and replication, you can automate the process using a cron job. This example shows a simple shell script that can be run by cron to create a daily snapshot and replicate it to a DR cluster.

> **Note on `date` command:** The `date` command syntax for calculating `PREVIOUS_DAY` (`date -d "1 day ago"`) is specific to GNU `date` (common on Linux). On macOS (BSD `date`), you might need to use `date -v-1d` or similar.

> **Important for first run:** For the `distcp -diff` command to work, the `snapshot-${PREVIOUS_DAY}` snapshot must exist on both the primary and DR clusters. Ensure you have created an initial snapshot on both clusters before the first scheduled run of this script.

**`run_backup.sh`**
```bash
#!/bin/bash

# Variables
PRIMARY_CLUSTER="ozone-1"
DR_CLUSTER="ozone-2"
VOLUME="vol1"
BUCKET="bucket1"

# Create a timestamped snapshot
TODAY=$(date +%Y-%m-%d)
PREVIOUS_DAY=$(date -d "1 day ago" +%Y-%m-%d)

# Create snapshot on the primary cluster
ozone sh snapshot create /${VOLUME}/${BUCKET} snapshot-${TODAY}

# Replicate the changes to the DR cluster
hadoop distcp -update -diff \
  o3fs://${BUCKET}.${VOLUME}.${PRIMARY_CLUSTER}/.snapshot/snapshot-${PREVIOUS_DAY} \
  o3fs://${BUCKET}.${VOLUME}.${PRIMARY_CLUSTER}/.snapshot/snapshot-${TODAY} \
  o3fs://${BUCKET}.${VOLUME}.${DR_CLUSTER}/

# Create the corresponding snapshot on the DR cluster
ozone --uri o3fs://${DR_CLUSTER}:9878 sh snapshot create /${VOLUME}/${BUCKET} snapshot-${TODAY}

# Clean up old snapshots (e.g., keep the last 7 days)
SEVEN_DAYS_AGO=$(date -d "7 days ago" +%Y-%m-%d)
ozone sh snapshot delete /${VOLUME}/${BUCKET} snapshot-${SEVEN_DAYS_AGO}
ozone --uri o3fs://${DR_CLUSTER}:9878 sh snapshot delete /${VOLUME}/${BUCKET} snapshot-${SEVEN_DAYS_AGO}
```

**Crontab Entry**

To run this script every day at midnight, you would add the following entry to your crontab:

```
0 0 * * * /path/to/run_backup.sh
```

This is a basic example. A production-ready script would need more robust error handling (e.g., `set -e`, checking command exit codes), logging, and a more sophisticated way to manage the snapshot lifecycle.


### Recovery Procedures

In the event of a disaster, you'll need to recover your data from the DR cluster. The recovery procedure will depend on the nature of the disaster.

*   **Full Cluster Recovery:** If the primary cluster is completely lost, you can promote the DR cluster to be the new primary. This will involve reconfiguring your applications to point to the new cluster.

*   **Bucket-Level Recovery:** If only a specific bucket is lost or corrupted, you can restore it from a snapshot on the DR cluster. This can be done by using `DistCp` to copy the data from the snapshot on the DR cluster back to the primary cluster.

    ```bash
    # Restore the entire bucket from a snapshot on the DR cluster
    hadoop distcp o3fs://bucket1.vol1.ozone-2/.snapshot/snap2/ o3fs://bucket1.vol1.ozone-1/
    ```

*   **Individual File Recovery:** If individual files are accidentally deleted or corrupted, you can restore them from a snapshot. This can be done by accessing the snapshot and copying the specific files back to the active bucket.

    ```bash
    # Copy a specific file from a snapshot back to the active bucket
    ozone fs cp o3fs://bucket1.vol1.ozone-1/.snapshot/snap2/path/to/file o3fs://bucket1.vol1.ozone-1/path/to/
    ```

A detailed, step-by-step recovery plan should be documented and regularly tested to ensure a smooth recovery process.
