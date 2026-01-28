---
title: Event notification support in Ozone
summary: Event notifications for all bucket/event types in ozone
date: 2025-06-28
jira: HDDS-13513
status: design
author: Donal Magennis, Colm Dougan
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

# Abstract

Implement an event notification system for Apache Ozone, providing the ability for users to consume events occurring on the Ozone filesystem.
This is similar to https://issues.apache.org/jira/browse/HDDS-5984 but aims to encapsulate all events and not solely S3 buckets.  
This document proposes a potential solution and discusses some of the challenges/open questions.

## Introduction

Apache Ozone does not currently provide the ability to consume filesystem events, similar to how HDFS does with Inotify or S3 with bucket notifications.  
These events are an integral part of integration with external systems to support real-time, scalable, and programmatic monitoring of changes in the data or metadata stored in Ozone.  
These external systems can use notifications of objects created/deleted to trigger data processing workflows, replication and monitoring alerts.

### Goals

Durable event log within each OM containing relevant OMRequest information for notification purposes.
Plugin framework for publishers (e.g. Kafka/RabbitMQ, custom sinks) running in separate threads in the OM.
Provide support for all events across the Ozone filesystem for FSO and non FSO buckets, including renames and changes to acls.
Guarantee at-least-once delivery within a bounded retention period, with notification of "missed events" where applicable.
Read-only access for plugins to notification table.

### Non-Goals

Exactly-once end-to-end semantics to external systems.
Filtering of events or paths/buckets.
Cross-OM consensus about what has been notified; co-ordination to be defined in the plugin e.g. write last notified position to a file in Ozone.
Retrofitting historical events prior to feature enablement.

### Supported OMRequests

OMDirectoryCreateRequest
OMKeyCommitRequest
OMKeyDeleteRequest
OMKeyRenameRequest
OMKeyAddAclRequest
OMKeyRemoveAclRequest
OMKeySetAclRequest
OMKeySetTimesRequest

# Design

## Overview

Introduce an Event Notification Pipeline for Apache Ozone with two
logical pieces:

1. event data capture

* OM captures the required details of selected OMRequest write
  operations post metadata update and persists them to a dedicated RocksDB
  completed operations "ledger" table keyed by the Ratis Txn Id
* each OM independently produces items to its local ledger table.  The
  ledger table should be integrated into OM Snapshots so that all OM's
  converge on the full set of required notifications.
* a retention policy is to be implemented in order to clean up no longer required entries.  This policy is bounded to a table size(number of events) which can be configurable.
* event capture will only be enabled if enabled

2. event data publishing

* a plugin framework is exposed where plugins can consume the ledger
  items in read-only fashion and process them as desired
* Plugins will run inside the OM and should be cognisant of resource consumption i.e. memory/disk
* all OMs will run the plugins but only the current leader OM will be
  active
* a base plugin implementation will provide common behaviour, including
  read only iteration of new ledger items and flagging that events
  have been "missed" since the consumer last requested them
  leader OM will be active
* a concrete plugin implementation will deal with publishing
  notifications to external targets (Apache Kafka)

### Components

#### Ozone Manager

Changes are required in the OzoneManager:
1. Add a new RocksDB column family e.g. om_event_log.
2. Add a hook in the OMRequest execution workflow (post successful commit) to persist required events.
3. Implement a plugin framework to run notification publishers.
4. Use DeleteRange API for cleaning up events outside of the retention policy.

#### Plugin Framework

Plugin Manager - spawns and supervises plugin threads.

Base Plugin - Provides common functionality which can be re-used:
1. Leader check.
2. Read-only query on the RocksDb table.
3. Offset tracking and persistence.
4. MissedEvents notifications.

It should be possible to run multiple plugins at the same time.

Plugins should be configured such that the implementation can be loaded
if provided on the classpath, similarly to ranger plugins which
are configured as follows:

```
    xasecure.audit.destination.kafka=true
    xasecure.audit.destination.kafka.classname=org.apache.ranger.audit.provider.kafka.KafkaAuditProvider
    xasecure.audit.kafka.topic_name=ranger_audits
    xasecure.audit.kafka.other_config_key=abc123
```

#### RocksDB Table

The ledger will be stored as a RocksDb column family where the
transaction id of the successful write operation is the key and the
value is an object with the folliwng sample protobuf schema:

```
message CreateKeyOperationArgs {
}

message RenameKeyOperationArgs {
    required string toKeyName = 1;
}

message DeleteKeyOperationArgs {
}

message CommitKeyOperationArgs {
}

message CreateDirectoryOperationArgs {
}

message CreateFileOperationArgs {
    required bool isRecursive = 2;
    required bool isOverwrite = 3;
}

message OperationInfo {

  optional int64 trxLogIndex = 1;
  required Type cmdType = 2; // Type of the command
  optional string volumeName = 3;
  optional string bucketName = 4;
  optional string keyName = 5;
  optional uint64 creationTime = 6;

  optional CreateKeyOperationArgs       createKeyArgs = 7;
  optional RenameKeyOperationArgs       renameKeyArgs = 8;
  optional DeleteKeyOperationArgs       deleteKeyArgs = 9;
  optional CommitKeyOperationArgs       commitKeyArgs = 10;
  optional CreateDirectoryOperationArgs createDirectoryArgs = 11;
  optional CreateFileOperationArgs      createFileArgs = 12;
}
```

## Performance

Writes to the RocksDB table happen synchronously in the OM Commit path but are a single put operation.
Reads are done asynchronously by plugins in their own threads.

## Configuration

Configuration has two parts.  Firstly the configuration of the persistence events, secondly the configuration of the plugins.

e.g. to enable notification persistence
```xml
<configration>
    <property>
        <name>ozone.event.notification.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>ozone.event.notification.limit</name>
        <value>1000000</value>
    </property>
</configration>
```
Example configuration to provide a kafka plugin:
```xml

<configration>
    <property>
        <name>ozone.om.plugin.kafka</name>
        <value>true</value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.class</name>
        <value>org.apache.ozone.notify.KafkaPublisher</value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.bootstrap.server</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.topic</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.saslUsername</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.saslPassword</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.saslMechanism</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.clientTlsCert</name>
        <value></value>
    </property>
    <property>
        <name>ozone.om.plugin.kafka.clientTlsKey</name>
        <value></value>
    </property>
</configration>
```

## Metrics

Key metrics would include:

Total events written
Head TxnId
Tail TxnId
Plugin current txnId
Missed total

## Notification Schema

[event-notification-schema.md](event-notification-schema.md)

## Testing

Unit testing of individual components.

Integration tests with Kafka/RabbitMQ.
