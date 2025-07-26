---
title: "Ozone Repair"
date: 2025-07-22
summary: Advanced tool to repair Ozone. The nodes being repaired must be stopped before the tool is run.
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

Ozone Repair (`ozone repair`) is an advanced tool to repair Ozone. The nodes being repaired must be stopped before the tool is run.
Note: All repair commands support a `--dry-run` option which allows a user to see what repair the command will be performing without actually making any changes to the cluster.

```bash
Usage: ozone repair [-hV] [--verbose] [-conf=<configurationPath>]
                    [-D=<String=String>]... [COMMAND]
Advanced tool to repair Ozone. The nodes being repaired must be stopped before
the tool is run.
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

## ozone debug datanode
Tools to repair Datanode
```bash
Usage: ozone repair datanode [--verbose] [COMMAND]
Tools to repair Datanode
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  upgrade-container-schema  Offline upgrade all schema V2 containers to schema
                              V3 for this datanode.
```

### upgrade-container-schema
Offline upgrade all schema V2 containers to schema V3 for this datanode.
```bash
Usage: ozone repair datanode upgrade-container-schema [-hV] [--dry-run]
       [--force] [--verbose] [--volume=<volume>]
Offline upgrade all schema V2 containers to schema V3 for this datanode.
      --dry-run           Simulate repair, but do not make any changes
      --force             Use this flag if you want to bypass the check in
                            false-positive cases.
  -h, --help              Show this help message and exit.
  -V, --version           Print version information and exit.
      --verbose           More verbose output. Show the stack trace of the
                            errors.
      --volume=<volume>   volume path
```

## ozone debug ldb
Operational tool to repair ldb.
```bash
Usage: ozone repair ldb [--verbose] [COMMAND]
Operational tool to repair ldb.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  compact  CLI to compact a column-family in the DB while the service is
             offline.
           Note: If om.db is compacted with this tool then it will negatively
             impact the Ozone Manager\'s efficient snapshot diff.
```

### compact
CLI to compact a column-family in the DB while the service is offline.
```bash
Usage: ozone repair ldb compact [-hV] [--dry-run] [--force] [--verbose]
                                --cf=<columnFamilyName> --db=<dbPath>
CLI to compact a column-family in the DB while the service is offline.
Note: If om.db is compacted with this tool then it will negatively impact the
Ozone Manager\'s efficient snapshot diff.
      --cf, --column-family, --column_family=<columnFamilyName>
                      Column family name
      --db=<dbPath>   Database File Path
      --dry-run       Simulate repair, but do not make any changes
      --force         Use this flag if you want to bypass the check in
                        false-positive cases.
  -h, --help          Show this help message and exit.
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```

## ozone debug om
Operational tool to repair OM.
```bash
Usage: ozone repair om [--verbose] [COMMAND]
Operational tool to repair OM.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  fso-tree                     Identify and repair a disconnected FSO tree by
                                 marking unreferenced entries for deletion. OM
                                 should be stopped while this tool is run.
  snapshot                     Subcommand for all snapshot related repairs.
  update-transaction           CLI to update the highest index in transaction
                                 info table.
  quota                        Operational tool to repair quota in OM DB.
  compact                      CLI to compact a column family in the om.db. The
                                 compaction happens asynchronously. Requires
                                 admin privileges.
  skip-ratis-transaction, srt  CLI to omit a raft log in a ratis segment file.
                                 The raft log at the index specified is
                                 replaced with an EchoOM command (which is a
                                 dummy command). It is an offline command i.e.,
                                 doesn\'t require OM to be running. The command
                                 should be run for the same transaction on all
                                 3 OMs only when all the OMs are crashing while
                                 applying the same transaction. If only one OM
                                 is crashing and the other OMs have executed
                                 the log successfully, then the DB should be
                                 manually copied from one of the good OMs to
                                 the crashing OM instead.
```

### fso-tree
Identify and repair a disconnected FSO tree by marking unreferenced entries for deletion. OM should be stopped while this tool is run.
```bash
Usage: ozone repair om fso-tree [-hV] [--dry-run] [--force] [--verbose]
                                [-b=<bucketFilter>] --db=<omDBPath>
                                [-v=<volumeFilter>]
Identify and repair a disconnected FSO tree by marking unreferenced entries for
deletion. OM should be stopped while this tool is run.
  -b, --bucket=<bucketFilter>
                        Filter by bucket name
      --db=<omDBPath>   Path to OM RocksDB
      --dry-run         Simulate repair, but do not make any changes
      --force           Use this flag if you want to bypass the check in
                          false-positive cases.
  -h, --help            Show this help message and exit.
  -v, --volume=<volumeFilter>
                        Filter by volume name. Add '/' before the volume name.
  -V, --version         Print version information and exit.
      --verbose         More verbose output. Show the stack trace of the errors.
```

### snapshot
Subcommand for all snapshot related repairs.
```bash
Usage: ozone repair om snapshot [--verbose] [COMMAND]
Subcommand for all snapshot related repairs.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  chain  CLI to update global and path previous snapshot for a snapshot in case
           snapshot chain is corrupted.
```

#### chain
CLI to update global and path previous snapshot for a snapshot in case snapshot chain is corrupted.
```bash
Usage: ozone repair om snapshot chain [-hV] [--dry-run] [--force] [--verbose]
                                      --db=<dbPath>
                                      --gp=<globalPreviousSnapshotId>
                                      --pp=<pathPreviousSnapshotId> <value>
                                      <snapshotName>
CLI to update global and path previous snapshot for a snapshot in case snapshot
chain is corrupted.
      <value>          URI of the bucket (format: volume/bucket).
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

      <snapshotName>   Snapshot name to update
      --db=<dbPath>    Database File Path
      --dry-run        Simulate repair, but do not make any changes
      --force          Use this flag if you want to bypass the check in
                         false-positive cases.
      --gp, --global-previous=<globalPreviousSnapshotId>
                       Global previous snapshotId to set for the given snapshot
  -h, --help           Show this help message and exit.
      --pp, --path-previous=<pathPreviousSnapshotId>
                       Path previous snapshotId to set for the given snapshot
  -V, --version        Print version information and exit.
      --verbose        More verbose output. Show the stack trace of the errors.
```

### update-transaction
CLI to update the highest index in transaction info table.
```bash
Usage: ozone repair om update-transaction [-hV] [--dry-run] [--force]
       [--verbose] --db=<dbPath> --index=<highestTransactionIndex>
       --term=<highestTransactionTerm>
CLI to update the highest index in transaction info table.
      --db=<dbPath>   Database File Path
      --dry-run       Simulate repair, but do not make any changes
      --force         Use this flag if you want to bypass the check in
                        false-positive cases.
  -h, --help          Show this help message and exit.
      --index=<highestTransactionIndex>
                      Highest index to set. The input should be non-zero long
                        integer.
      --term=<highestTransactionTerm>
                      Highest term to set. The input should be non-zero long
                        integer.
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```

### quota
Operational tool to repair quota in OM DB.
```bash
Usage: ozone repair om quota [-hV] [--verbose] [COMMAND]
Operational tool to repair quota in OM DB.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  status  CLI to get the status of last trigger quota repair if available.
  start   CLI to trigger quota repair.
```

#### status
CLI to get the status of last trigger quota repair if available.
```bash
Usage: ozone repair om quota status [-hV] [--verbose] [--service-host=<omHost>]
                                    [--service-id=<omServiceId>]
CLI to get the status of last trigger quota repair if available.
  -h, --help      Show this help message and exit.
      --service-host=<omHost>
                  Ozone Manager Host. If OM HA is enabled, use --service-id
                    instead. If you must use --service-host with OM HA, this
                    must point directly to the leader OM. This option is
                    required when --service-id is not provided or when HA is
                    not enabled.
      --service-id, --om-service-id=<omServiceId>
                  Ozone Manager Service ID
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
```

#### start
CLI to trigger quota repair.
```bash
Usage: ozone repair om quota start [-hV] [--dry-run] [--force] [--verbose]
                                   [--buckets=<buckets>]
                                   [--service-host=<omHost>]
                                   [--service-id=<omServiceId>]
CLI to trigger quota repair.
      --buckets=<buckets>   start quota repair for specific buckets. Input will
                              be list of uri separated by comma as
                              /<volume>/<bucket>[,...]
      --dry-run             Simulate repair, but do not make any changes
      --force               Use this flag if you want to bypass the check in
                              false-positive cases.
  -h, --help                Show this help message and exit.
      --service-host=<omHost>
                            Ozone Manager Host. If OM HA is enabled, use
                              --service-id instead. If you must use
                              --service-host with OM HA, this must point
                              directly to the leader OM. This option is
                              required when --service-id is not provided or
                              when HA is not enabled.
      --service-id, --om-service-id=<omServiceId>
                            Ozone Manager Service ID
  -V, --version             Print version information and exit.
      --verbose             More verbose output. Show the stack trace of the
                              errors.
```

### compact
CLI to compact a column family in the om.db. The compaction happens asynchronously. Requires admin privileges.
```bash
Usage: ozone repair om compact [-hV] [--dry-run] [--force] [--verbose]
                               --cf=<columnFamilyName> [--node-id=<nodeId>]
                               [--service-id=<omServiceId>]
CLI to compact a column family in the om.db. The compaction happens
asynchronously. Requires admin privileges.
      --cf, --column-family, --column_family=<columnFamilyName>
                           Column family name
      --dry-run            Simulate repair, but do not make any changes
      --force              Use this flag if you want to bypass the check in
                             false-positive cases.
  -h, --help               Show this help message and exit.
      --node-id=<nodeId>   NodeID of the OM for which db needs to be compacted.
      --service-id, --om-service-id=<omServiceId>
                           Ozone Manager Service ID
  -V, --version            Print version information and exit.
      --verbose            More verbose output. Show the stack trace of the
                             errors.
```

### skip-ratis-transaction, srt
CLI to omit a raft log in a ratis segment file. \
The raft log at the index specified is replaced with an EchoOM command (which is a dummy command). 
It is an offline command i.e., doesn't require OM to be running. 
The command should be run for the same transaction on all 3 OMs only when all the OMs are crashing while applying the same transaction. 
If only one OM is crashing and the other OMs have executed the log successfully, 
then the DB should be manually copied from one of the good OMs to the crashing OM instead.
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
from one of the good OMs to the crashing OM instead.
  -b, --backup=<backupDir>   Directory to put the backup of the original
                               repaired segment file before the repair.
  -d, --ratis-log-dir=<logDir>
                             Path of the ratis log directory
      --dry-run              Simulate repair, but do not make any changes
      --force                Use this flag if you want to bypass the check in
                               false-positive cases.
  -h, --help                 Show this help message and exit.
      --index=<index>        Index of the failing transaction that should be
                               removed
  -s, --segment-path=<segmentFile>
                             Path of the input segment file
  -V, --version              Print version information and exit.
      --verbose              More verbose output. Show the stack trace of the
                               errors.
```

## ozone debug scm
Operational tool to repair SCM.
```bash
Usage: ozone repair scm [--verbose] [COMMAND]
Operational tool to repair SCM.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  cert                Subcommand for all certificate related repairs on SCM
  update-transaction  CLI to update the highest index in transaction info table.

```

### cert
Subcommand for all certificate related repairs on SCM
```bash
Usage: ozone repair scm cert [--verbose] [COMMAND]
Subcommand for all certificate related repairs on SCM
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  recover  Recover Deleted SCM Certificate from RocksDB
```

#### recover
Recover Deleted SCM Certificate from RocksDB
```bash
Usage: ozone repair scm cert recover [-hV] [--dry-run] [--force] [--verbose]
                                     --db=<dbPath>
Recover Deleted SCM Certificate from RocksDB
      --db=<dbPath>   SCM DB Path
      --dry-run       Simulate repair, but do not make any changes
      --force         Use this flag if you want to bypass the check in
                        false-positive cases.
  -h, --help          Show this help message and exit.
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```

### update-transaction
CLI to update the highest index in transaction info table.
```bash
Usage: ozone repair scm update-transaction [-hV] [--dry-run] [--force]
       [--verbose] --db=<dbPath> --index=<highestTransactionIndex>
       --term=<highestTransactionTerm>
CLI to update the highest index in transaction info table.
      --db=<dbPath>   Database File Path
      --dry-run       Simulate repair, but do not make any changes
      --force         Use this flag if you want to bypass the check in
                        false-positive cases.
  -h, --help          Show this help message and exit.
      --index=<highestTransactionIndex>
                      Highest index to set. The input should be non-zero long
                        integer.
      --term=<highestTransactionTerm>
                      Highest term to set. The input should be non-zero long
                        integer.
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```
