---
title: Improved Layout of Ozone Tools
summary: Proposal for an improved layout of Ozone tools CLI
date: 2024-12-18
author: Ethan Rose
---
<!--
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

# Improved Layout of Ozone Tools

The `ozone-tools` module currently contains utilities that can be used to troubleshoot and fix low level issues in an Ozone cluster through the `ozone debug` and `ozone repair` subcommands. These tools have traditionally been updated in an ad-hoc fashion and not well documented. As we continue to add more tools and their complexity increases, we should standardize the layout and function of these tools to make sure they are safe and easy to use.

This document proposes a migration of the Ozone tools layout, and documents the commands available at the time of writing. It may be kept as a reference for the motivation behind the layout of the tools commands, but the CLI may change in the future beyond what this doc initially covers.

## Current Layout

This is the current layout of the first two levels of the `ozone-tools` command line:
- ozone debug
    - print-log-dag
    - find-missing-padding
    - read-replicas
    - recover
    - prefix
    - version
    - chunkinfo
    - container
    - ldb
    - ratislogparser
- ozone repair
    - cert-recover
    - ldb
    - quota

## New Layout

Since debug and repair tools are not meant to be scripted, we should be able to change the CLI layout without impact to compatibility. The following is a proposal to logically regroup all existing commands while leaving room for new additions.

### Debug

Invoked as `ozone debug`, these commands make no modifications to Ozone and can be run while the Ozone cluster is live. They can be broadly grouped into three categories:
- Commands that run locally on all components.
    - These include generic things like `ldb` and ratis log parsing.
- Commands that run locally on a specific component.
    - This include things like repairing quota or snapshot chain on the OM.
- Commands that run over the network.
    - This include things like reading all replicas and checking their integrity.

Commands from different categories should not exist under the same `ozone debug` subcommand. For example, `ozone debug ldb` should not have a mix of subcommands that work on all DB instances and some that only work on OM. All subcommands should clearly document where they are expected to run in their help message. We can use these guidelines to place subcommands based on their type:
- Commands that run locally or commands that run over the network may be top level subcommands of `ozone debug`.
- Commands that run locally on a specific component should be under a dedicated subcommand, like `ozone debug om`.

We can then apply these rules to group the existing commands:

- `ozone debug ldb`
    This command which provides RocksDB parsing for all components will remain in its original location.
- `ozone debug version`
    Implemented in [HDDS-11740](https://issues.apache.org/jira/browse/HDDS-11740), this command prints the internal component versions of the Ozone instance installed on the current host. It will remain in its original location
- `ozone debug om`
    A subcommand for all debug operations that run locally on an OM instance.
    - `ozone debug om generate-compaction-dag`
        This will be the new location of `ozone debug print-log-dag`. Since it outputs a file locally to the leader OM, it should be modified to run locally as well. The current version goes over the network but does not return the result over the network. The name has been updated to better reflect its output. The command only works on the OM since we do not track RocksDB compactions for other components.
    - `ozone debug om prefix`
        The new location of `ozone debug prefix`.
- `ozone debug datanode`
    A subcommand for all debug operations that run locally on a datanode instance.
    - `ozone debug datanode container`
        The new location of `ozone debug container` and all of its subcommands. These commands are used to inspect container replicas locally on datanodes. Currently `container` is the only debug subcommand on the datanodes.
- `ozone debug replicas`
    Currently all online debug tools fit under this category, where they get a listing from the OM and perform checks on the replica information retrieved. The help message for this subcommand should explicitly state that it runs over the network against a running cluster.
    - `ozone debug replicas chunk-info`
        This is the new location of `ozone debug chunkinfo`
    - `ozone debug replicas verify [--checksums] [--block-existence] [--container-state]`
        This is the combination of `ozone debug find-missing-padding`, `ozone debug read-replicas`, and the [proposed block metadata check tool](https://github.com/apache/ozone/pull/7548). The command takes  paths in the namespace and performs checks on the objects based on the flags given:
      - No flags: Error, at least one check must be specified.
      - `--blocks` checks block existence of all replicas as proposed in [HDDS-11891](https://issues.apache.org/jira/browse/HDDS-11891).
          - This also incorporates the `find-missing-padding` tool, which actually does a similar thing and is not EC specific.
          - This command may support filtering by things like replication type in the future if only a certain type of key needs to be checked.
      - `--checksums` Pulls data and verifies the checksums on the client side for all replicas. This is a thorough check but it will run very slowly.
- `ozone debug ratis`
    This subcommand is where we can put Ratis related commands that are specific to Ozone. For bringing the Ratis shell into Ozone, see the [`ozone ratis`](#ratis) subcommand.
    -  `ozone debug ratis parse <--role={om,scm,datanode}>`
        - Currently our only Ozone-specific subcommand, this can be used to parse any Ratis on-disk state and apply the Ozone specific schema to it if needed. 
        - The user will need to specify what type of Ozone role the Ratis information came from so it can apply the proper schema.
        - This is tracked as [HDDS-13518](https://issues.apache.org/jira/browse/HDDS-13518).

### Repair

Invoked as `ozone repair`, these commands make modifications to an Ozone instance. Some commands, like quota repair, may go over the network and run while the cluster is online. Proper usage should be specified in each command's help message. All locally running commands will support a `--dry-run` flag to show what they will do without making any modifications. Subcommand grouping follows similar guidelines to `ozone debug`.

- `ozone repair om`
    These commands make repairs to an individual OM instance.
    - `ozone repair om quota`
        The new location of `ozone repair quota`
    - `ozone repair om snapshot`
        A dedicated subcommand for all snapshot related repairs we may need to do now or in the future.
        - `ozone repair om snapshot chain`
            Currently our only snapshot related repair tool, this is the new location of `ozone repair snapshot`, which only handles chain corruption despite the generic name.
    - `ozone repair om update-transaction`
        The new location of `ozone repair ldb update-transaction`, which is currently only supported for OM.
    - `ozone repair om fso-tree`
        The FSO repair tool implemented in [HDDS-11724](https://issues.apache.org/jira/browse/HDDS-11724).
- `ozone repair scm`
    These commands make repairs to an individual SCM instance.
    - `ozone repair scm update-transaction`
        A port of the existing `ozone repair ldb update-transaction` for SCM.
    - `ozone repair scm cert`
        A dedicated subcommand for all certificate related repairs on SCM we may need now or in the future.
        - `ozone repair scm cert recover`
            The new location of `ozone repair cert-recover`
- `ozone repair datanode`
    These commands make repairs to an individual datanode instance.
    - `ozone repair datanode upgrade-container-schema`
        This is the new location of `ozone admin container upgrade`, which actually works offline on a single datanode instance so it does not fit with other `ozone admin container` commands.

### Ratis

Invoked as `ozone ratis` starting from [HDDS-10509](https://issues.apache.org/jira/browse/HDDS-10509), this subcommand applies Ozone's TLS configuration and forwards all commands to the Ratis shell, meaning `ozone ratis sh` will work the same as `ratis sh` but will also work out of the box in a secure cluster. To avoid conflicts with commands that may be added to Ratis in the future, we should not add any new `ozone ratis` subcommands in Ozone. Custom Ozone subcommands related to Ratis, like log parsing with Ozone's schemas, will be added to `ozone debug ratis`.

### Misc

#### Lease Recovery

The `ozone debug recover` command that is currently used for lease recovery has a few problems:
- It modifies the cluster, so it should not be under `debug`.
- The name does not specify that it deals with leases.
- It uses a standard API from the `FileSystem` interface unlike other low level tools in this package.

The command is unique because it goes through the `FileSystem` interface but is not exposed through the `fs` command line in Hadoop or Ozone. For these reasons, we propose moving this command to its own Ozone subcommand: `ozone admin om lease recover`. This also allows other lease related subcommands (like querying of existing leases) to be added in the future.

#### LDB Repair

For now there are no plans to support arbitrary RocksDB modifications to RocksDB data per [this discussion](https://github.com/apache/ozone/pull/7177). The current set of `ozone repair ldb` subcommands only repair OM, so they should be moved to `ozone repair om` instead. Currently the only use case for `ozone repair ldb` is a wrapper around compaction, giving us the `ozone repair ldb compact` command in [HDDS-12533](https://issues.apache.org/jira/browse/HDDS-12533).

