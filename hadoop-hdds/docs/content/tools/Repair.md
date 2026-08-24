---
title: "Ozone Repair"
date: 2025-07-22
summary: Advanced tool to repair Ozone.
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

Ozone Repair (`ozone repair`) is an advanced tool to repair Ozone. Check the `--help` output of the subcommand for the respective role status requirements.
Note: All repair commands support a `--dry-run` option which allows a user to see what repair the command will be performing without actually making any changes to the cluster.
Use the `--force` flag to override the running service check in false-positive cases.

```bash
Usage: ozone repair [-hV] [--verbose] [-conf=<configurationPath>]
                    [-D=<String=String>]... [COMMAND]
Advanced tool to repair Ozone. Check the --help output of the subcommand for
the respective role status requirements.
      -conf=<configurationPath>

  -D, --set=<String=String>

  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  datanode  Tools to repair Datanode
  ldb       Operational tool to repair ldb.
  om        Operational tool to repair OM.
  scm       Operational tool to repair SCM.
```
For more detailed usage see the output of `--help` for each of the subcommands.

## ozone repair datanode
Operational tool to repair datanode.

### upgrade-container-schema
Upgrade all schema V2 containers to schema V3 for a datanode in offline mode.
Optionally takes `--volume` option to specify which volume needs the upgrade.
Datanode should be stopped before running this tool.

## ozone repair ldb
Operational tool to repair ldb.

### compact
Compact a column family in the DB to clean up tombstones while the service is offline.
The corresponding OM, SCM or Datanode role should be stopped before running this tool.
```bash
Usage: ozone repair ldb compact [-hV] [--dry-run] [--force] [--verbose]
                                --cf=<columnFamilyName> --db=<dbPath>
CLI to compact a column-family in the DB while the service is offline.
Note: If om.db is compacted with this tool then it will negatively impact the
Ozone Manager\'s efficient snapshot diff. The corresponding OM, SCM or Datanode
role should be stopped for this tool.
      --cf, --column-family, --column_family=<columnFamilyName>
                      Column family name
      --db=<dbPath>   Database File Path
```

## ozone repair om
Operational tool to repair OM.

#### Subcommands under OM
- fso-tree
- snapshot
- update-transaction
- quota
- compact
- skip-ratis-transaction

### fso-tree
Identify and repair a disconnected FSO tree by marking unreferenced entries for deletion.
Reports the reachable, unreachable (pending delete) and unreferenced (orphaned) directories and files.
OM should be stopped before running this tool.
```bash
Usage: ozone repair om fso-tree [-hV] [--dry-run] [--force] [--verbose]
                                [-b=<bucketFilter>] --db=<omDBPath>
                                [-v=<volumeFilter>]
Identify and repair a disconnected FSO tree by marking unreferenced entries for
deletion. OM should be stopped for this tool.
  -b, --bucket=<bucketFilter>
                        Filter by bucket name
      --db=<omDBPath>   Path to OM RocksDB
  -v, --volume=<volumeFilter>
                        Filter by volume name. Add '/' before the volume name.
```

### snapshot
Subcommand for all snapshot related repairs.

#### chain
Update global and path previous snapshot for a snapshot in case snapshot chain is corrupted.
OM should be stopped before running this tool.
```bash
Usage: ozone repair om snapshot chain [-hV] [--dry-run] [--force] [--verbose]
                                      --db=<dbPath>
                                      --gp=<globalPreviousSnapshotId>
                                      --pp=<pathPreviousSnapshotId> <value>
                                      <snapshotName>
CLI to update global and path previous snapshot for a snapshot in case snapshot
chain is corrupted. OM should be stopped for this tool.
      <value>          URI of the bucket (format: volume/bucket).
      <snapshotName>   Snapshot name to update
      --db=<dbPath>    Database File Path
      --gp, --global-previous=<globalPreviousSnapshotId>
                       Global previous snapshotId to set for the given snapshot
      --pp, --path-previous=<pathPreviousSnapshotId>
                       Path previous snapshotId to set for the given snapshot
```

### update-transaction
To avoid modifying Ratis logs and only update the latest applied transaction, use `update-transaction` command. 
This updates the highest transaction index in the OM transaction info table. The OM role should be stopped before running this tool.
```bash
Usage: ozone repair om update-transaction [-hV] [--dry-run] [--force]
       [--verbose] --db=<dbPath> --index=<highestTransactionIndex>
       --term=<highestTransactionTerm>
CLI to update the highest index in transaction info table. The corresponding OM
or SCM role should be stopped for this tool.
      --db=<dbPath>   Database File Path
      --index=<highestTransactionIndex>
                      Highest index to set. The input should be non-zero long
                        integer.
      --term=<highestTransactionTerm>
                      Highest term to set. The input should be non-zero long
                        integer.
```

### quota
Operational tool to repair quota in OM DB. OM should be running for this tool.

#### start
To trigger quota repair use the `start` command.
```bash
Usage: ozone repair om quota start [-hV] [--dry-run] [--force] [--verbose]
                                   [--buckets=<buckets>]
                                   [--service-host=<omHost>]
                                   [--service-id=<omServiceId>]
CLI to trigger quota repair.
      --buckets=<buckets>   start quota repair for specific buckets. Input will
                              be list of uri separated by comma as
                              /<volume>/<bucket>[,...]
      --service-host=<omHost>
                            Ozone Manager Host. If OM HA is enabled, use
                              --service-id instead. If you must use
                              --service-host with OM HA, this must point
                              directly to the leader OM. This option is
                              required when --service-id is not provided or
                              when HA is not enabled.
      --service-id, --om-service-id=<omServiceId>
                            Ozone Manager Service ID
```

#### status
Get the status of last triggered quota repair.
```bash
Usage: ozone repair om quota status [-hV] [--verbose] [--service-host=<omHost>]
                                    [--service-id=<omServiceId>]
CLI to get the status of last trigger quota repair if available.
      --service-host=<omHost>
                  Ozone Manager Host. If OM HA is enabled, use --service-id
                    instead. If you must use --service-host with OM HA, this
                    must point directly to the leader OM. This option is
                    required when --service-id is not provided or when HA is
                    not enabled.
      --service-id, --om-service-id=<omServiceId>
                  Ozone Manager Service ID
```

### compact
Compact a column family in the OM DB to clean up tombstones. The compaction happens asynchronously. Requires admin privileges.
OM should be running for this tool.
```bash
Usage: ozone repair om compact [-hV] [--dry-run] [--force] [--verbose]
                               --cf=<columnFamilyName> [--node-id=<nodeId>]
                               [--service-id=<omServiceId>]
CLI to compact a column family in the om.db. The compaction happens
asynchronously. Requires admin privileges. OM should be running for this tool.
      --cf, --column-family, --column_family=<columnFamilyName>
                           Column family name
      --node-id=<nodeId>   NodeID of the OM for which db needs to be compacted.
      --service-id, --om-service-id=<omServiceId>
                           Ozone Manager Service ID
```

### skip-ratis-transaction, srt
Omit a raft log in a ratis segment file by replacing the specified index with a dummy EchoOM command. 
This is an offline tool meant to be used only when all 3 OMs crash on the same transaction. 
If the issue is isolated to one OM, manually copy the DB from a healthy OM instead.
OM should be stopped before running this tool.
```bash
Usage: ozone repair om skip-ratis-transaction [-hV] [--dry-run] [--force]
       [--verbose] -b=<backupDir> --index=<index> (-s=<segmentFile> |
       -d=<logDir>)
CLI to omit a raft log in a ratis segment file. The raft log at the index
specified is replaced with an EchoOM command (which is a dummy command). It is
an offline command i.e., doesn\'t require OM to be running. The command should
be run for the same transaction on all 3 OMs only when all the OMs are crashing
while applying the same transaction. If only one OM is crashing and the other
OMs have executed the log successfully, then the DB should be manually copied
from one of the good OMs to the crashing OM instead. OM should be stopped for
this tool.
  -b, --backup=<backupDir>   Directory to put the backup of the original
                               repaired segment file before the repair.
  -d, --ratis-log-dir=<logDir>
                             Path of the ratis log directory
      --index=<index>        Index of the failing transaction that should be
                               removed
  -s, --segment-path=<segmentFile>
                             Path of the input segment file
```

## ozone repair scm
Operational tool to repair SCM.

#### Subcommands under SCM
- cert
- update-transaction

### cert
Subcommand for all certificate related repairs on SCM

#### recover
Recover Deleted SCM Certificate from RocksDB. SCM should be stopped before running this tool.
```bash
Usage: ozone repair scm cert recover [-hV] [--dry-run] [--force] [--verbose]
                                     --db=<dbPath>
Recover Deleted SCM Certificate from RocksDB. SCM should be stopped for this
tool.
      --db=<dbPath>   SCM DB Path
```

### update-transaction
To avoid modifying Ratis logs and only update the latest applied transaction, use `update-transaction` command.
This updates the highest transaction index in the SCM transaction info table. The SCM role should be stopped before running this tool.
```bash
Usage: ozone repair scm update-transaction [-hV] [--dry-run] [--force]
       [--verbose] --db=<dbPath> --index=<highestTransactionIndex>
       --term=<highestTransactionTerm>
CLI to update the highest index in transaction info table. The corresponding OM
or SCM role should be stopped for this tool.
      --db=<dbPath>   Database File Path
      --index=<highestTransactionIndex>
                      Highest index to set. The input should be non-zero long
                        integer.
      --term=<highestTransactionTerm>
                      Highest term to set. The input should be non-zero long
                        integer.
```
