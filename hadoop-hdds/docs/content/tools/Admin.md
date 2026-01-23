---
title: "Ozone Admin"
date: 2020-03-25
summary: Ozone Admin command can be used for all the admin related tasks.
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

Ozone Admin command (`ozone admin`) is a collection of tools intended to be used only by admins.

And quick overview about the available functionalities:

 * `ozone admin safemode`: You can check the safe mode status and force to leave/enter from/to safemode,  `--verbose` option will print validation status of all rules that evaluate safemode status.
 * `ozone admin container`: Containers are the unit of the replication. The subcommands can help to debug the current state of the containers (list/get/create/...)
 * `ozone admin pipeline`: Can help to check the available pipelines (datanode sets)
 * `ozone admin datanode`: Provides information about the datanode
 * `ozone admin printTopology`: display the rack-awareness related information
 * `ozone admin replicationmanager`: Can be used to check the status of the replications (and start / stop replication in case of emergency).
 * `ozone admin om`: Ozone Manager HA related tool to get information about the current cluster.

For more detailed usage see the output of `--help`.

```bash
$ ozone admin --help
Usage: ozone admin [-hV] [--verbose] [-conf=<configurationPath>]
                   [-D=<String=String>]... [COMMAND]
Developer tools for Ozone Admin operations
      -conf=<configurationPath>

  -D, --set=<String=String>

  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  containerbalancer   ContainerBalancer specific operations
  replicationmanager  ReplicationManager specific operations
  safemode            Safe mode specific operations
  printTopology       Print a tree of the network topology as reported by SCM
  cert                Certificate related operations
  container           Container specific operations
  datanode            Datanode specific operations
  pipeline            Pipeline specific operations
  namespace           Namespace Summary specific admin operations
  om                  Ozone Manager specific admin operations
  reconfig            Dynamically reconfigure server without restarting it
  scm                 Ozone Storage Container Manager specific admin operations
```

Some of those subcommand usages has been detailed in their dedicated feature documentation pages. For instance, [Decommissioning]({{<ref "feature/Decommission.md">}}), [Non-Rolling Upgrades and Downgrades]({{<ref "feature/Nonrolling-Upgrade.md">}}).


## List open files

List open files admin command lists open keys in Ozone Manager's `OpenKeyTable`.
Works for all bucket types.
Argument `--prefix` could be root (`/`), path to a bucket (`/vol1/buck`) or a key prefix (for FSO buckets the key prefix could contain parent object ID). But it can't be a volume.

```bash
$ ozone admin om lof --help
Usage: ozone admin om list-open-files [-hV] [--json] [-l=<limit>]
                                      [-p=<pathPrefix>] [-s=<startItem>]
                                      [--service-host=<omHost>]
                                      [--service-id=<omServiceId>]
Lists open files (keys) in Ozone Manager.
  -h, --help                Show this help message and exit.
      --json                Format output as JSON
  -l, --length=<limit>      Maximum number of items to list
  -p, --prefix=<pathPrefix> Filter results by the specified path on the server
                              side.
  -s, --start=<startItem>   The item to start the listing from.
                            i.e. continuation token. This will be excluded from
                              the result.
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
```

### Example usages

- In human-readable format, list open files (keys) under bucket `/volumelof/buck1` with a batch size of 3:

```bash
$ ozone admin om lof --service-id=om-service-test1 --length=3 --prefix=/volumelof/buck1
```

```bash
5 total open files (est.). Showing 3 open files (limit 3) under path prefix:
  /volume-lof/buck1

Client ID		Creation time	Hsync'ed	Open File Path
111726338148007937	1704808626523	No		/volume-lof/buck1/-9223372036854774527/key0
111726338151415810	1704808626578	No		/volume-lof/buck1/-9223372036854774527/key1
111726338152071171	1704808626588	No		/volume-lof/buck1/-9223372036854774527/key2

To get the next batch of open keys, run:
  ozone admin om lof --service-id=om-service-test1 --length=3 --prefix=/volume-lof/buck1 --start=/-9223372036854775552/-9223372036854775040/-9223372036854774527/key2/111726338152071171
```

- In JSON, list open files (keys) under bucket `/volumelof/buck1` with a batch size of 3:

```bash
$ ozone admin om lof --service-id=om-service-test1 --length=3 --prefix=/volumelof/buck1 --json
```

```json
{
  "openKeys" : [ {
    "keyInfo" : {
      "metadata" : { },
      "objectID" : -9223372036854774015,
      "updateID" : 7,
      "parentObjectID" : -9223372036854774527,
      "volumeName" : "volume-lof",
      "bucketName" : "buck1",
      "keyName" : "key0",
      "dataSize" : 4194304,
      "keyLocationVersions" : [ ... ],
      "creationTime" : 1704808722487,
      "modificationTime" : 1704808722487,
      "replicationConfig" : {
        "replicationFactor" : "THREE",
        "requiredNodes" : 3,
        "replicationType" : "RATIS"
      },
      "fileName" : "key0",
      "acls" : [ ... ],
      "path" : "-9223372036854774527/key0",
      "file" : true,
      "replicatedSize" : 12582912,
      "objectInfo" : "OMKeyInfo{volume='volume-lof', bucket='buck1', key='key0', dataSize='4194304', creationTime='1704808722487', objectID='-9223372036854774015', parentID='-9223372036854774527', replication='RATIS/THREE', fileChecksum='null}",
      "hsync" : false,
      "latestVersionLocations" : { ... },
      "updateIDset" : true
    },
    "openVersion" : 0,
    "clientId" : 111726344437039105
  }, {
    "keyInfo" : { ... },
    "openVersion" : 0,
    "clientId" : 111726344440578050
  }, {
    "keyInfo" : { ... },
    "openVersion" : 0,
    "clientId" : 111726344441233411
  } ],
  "totalOpenKeyCount" : 5,
  "hasMore" : true,
  "contToken" : "/-9223372036854775552/-9223372036854775040/-9223372036854774527/key2/111726344441233411"
}
```

Note in JSON output mode, field `contToken` won't show up at all in the result if there are no more entries after the batch (i.e. when `hasMore` is `false`).


## Snapshot Defragmentation Trigger

The snapshot defrag command triggers the Snapshot Defragmentation Service to run immediately on a specific Ozone Manager node.
This command manually initiates the snapshot defragmentation process which compacts snapshot data and removes fragmentation to improve storage efficiency.

This command only works on Ozone Manager HA clusters.

```bash
$ ozone admin om snapshot defrag --help
Usage: ozone admin om snapshot defrag [-hV] [--no-wait] [--verbose]
                                      [--node-id=<nodeId>]
                                      [--service-id=<serviceID>]
Triggers the Snapshot Defragmentation Service to run immediately. This command
manually initiates the snapshot defragmentation process which compacts snapshot
data and removes fragmentation to improve storage efficiency. This command
works only on OzoneManager HA cluster.
  -h, --help               Show this help message and exit.
      --no-wait            Do not wait for the defragmentation task to
                             complete. The command will return immediately
                             after triggering the task.
      --node-id=<nodeId>   NodeID of the OM to trigger snapshot defragmentation
                             on.
      --service-id, --om-service-id=<serviceID>
                           Ozone Manager Service ID.
  -V, --version            Print version information and exit.
      --verbose            More verbose output. Show the stack trace of the
                             errors.
```

### Example usages

- Trigger snapshot defragmentation on OM node `om3` in service `omservice` and wait for completion:

```bash
$ ozone admin om snapshot defrag --service-id=omservice --node-id=om3
Triggering Snapshot Defrag Service ...
Snapshot defragmentation completed successfully.
```

- Trigger snapshot defragmentation without waiting for completion:

```bash
$ ozone admin om snapshot defrag --service-id=omservice --node-id=om3 --no-wait
Triggering Snapshot Defrag Service ...
Snapshot defragmentation task has been triggered successfully and is running in the background.
```

