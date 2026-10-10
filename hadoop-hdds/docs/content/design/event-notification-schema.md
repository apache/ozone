---
title: Event notification schema discussion
summary: Event notifications schema discussion
date: 2025-06-29
jira: HDDS-13513
status: design
author: Colm Dougan, Donal Magennis
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

## Overview

This document outlines the schema requirements for event notification
within Ozone and discusses the suitability of 2 widely used event
notification schemas (S3 and HDFS) as candidates to use as a basis for
the transmission format for notifications within Ozone.

# General schema requirements

## File/Directory creation/modification

event notifications should be raised to inform consumers of completed
operations which modify the filesystem and specifically the requests:

#### CreateRequest

we should emit some **create** event

required fields:
- path (volume + bucket + key)
- isfile

nice to have fields:
- overwrite
- recursive

#### CreateFileRequest

we should emit some **create** event

required fields:
- path (volume + bucket + key)
- isfile

nice to have fields:
- overwrite
- recursive

#### CreateDirectoryRequest

we should emit some **create** event

required fields:
- path (volume + bucket + key)
- isfile

#### CommitKeyRequest

we should emit some **commit/close** event

required fields:
- path (volume + bucket + key)

nice to have fields:
- data size
- hsync?

#### DeleteKeyRequest

we should emit some **delete** event

required fields:
- path (volume + bucket + key)

nice to have fields:
- recursive (if known)

### RenameKeyRequest

we should emit some **rename** event

required fields:
- fromPath (volume + bucket + key)
- toPath (volume + bucket + toKeyName)

nice to have fields:
- recursive (if known)
- is directory (if known)

NOTE: in the case of a FSO directory rename there is a dillema
(discussed later in this document) as to whether we should emit a single
event for a directory rename (specifying only the old/new directory names)
or whether we should emit granular events for all the child objects impacted by
the rename.

## ACLs

event notifications should be raised to inform consumers that ACL events
have happened. The relevant requests are:

* AddAclRequest
* SetAclRequest
* RemoveAclRequest

The fields provided could vary based on the implementation complexity.

Minimally we have a requirement that we be informed that "some ACL update
happened" to a certain key (or prefix).

Ideally the details would include the full context of the change made as
per the request. (perhaps by mirroring the full request details as a JSON
sub-object) e.g. :

```json
   ...

   "acls": [
    {
      type: "GROUP",
      name: "mygroup"
      rights: "\000\001",
      aclScope: "ACCESS",
    }
   ]
```

The precise details we would need to revisit with guidance from the
community but this is just to set broad brush expectations.

## SetTimes

event notifications should be raised to inform consumers that
mtime/atime has changed, as per **SetTimesRequest**

# Transmission format

This section discusses 2 widely used transmission formats for event
notifiations (S3 and HDFS) and their suitability as candidates for
adoption within Ozone.

It is not assumed that these are the only options available but they are
good examples to test against our requirements and discuss trade-offs.

## 1. S3 Event Notification schema

The S3 event notification schema:

[https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html#supported-notification-event-types](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html#supported-notification-event-types)

has become a standard for change notifications in S3 compatible storage services such as S3 itself, Ceph, MinIO etc

Notification events are produced as a list of JSON records.

To illustrate we can look at a sample "create" event from the Ceph docs
(https://docs.ceph.com/en/quincy/radosgw/notifications/#events):

```json

{"Records":[
    {
        "eventVersion":"2.1",
        "eventSource":"ceph:s3",
        "awsRegion":"us-east-1",
        "eventTime":"2019-11-22T13:47:35.124724Z",
        "eventName":"ObjectCreated:Put",
        "userIdentity":{
            "principalId":"tester"
        },
        "requestParameters":{
            "sourceIPAddress":""
        },
        "responseElements":{
            "x-amz-request-id":"503a4c37-85eb-47cd-8681-2817e80b4281.5330.903595",
            "x-amz-id-2":"14d2-zone1-zonegroup1"
        },
        "s3":{
            "s3SchemaVersion":"1.0",
            "configurationId":"mynotif1",
            "bucket":{
                "name":"mybucket1",
                "ownerIdentity":{
                    "principalId":"tester"
                },
                "arn":"arn:aws:s3:us-east-1::mybucket1",
                "id":"503a4c37-85eb-47cd-8681-2817e80b4281.5332.38"
            },
            "object":{
                "key":"myimage1.jpg",
                "size":"1024",
                "eTag":"37b51d194a7513e45b56f6524f2d51f2",
                "versionId":"",
                "sequencer": "F7E6D75DC742D108",
                "metadata":[],
                "tags":[]
            }
        },
        "eventId":"",
        "opaqueData":"me@example.com"
    }
]}
```

As we can see above: there are a number of boilerplate fields to inform us
of various aspects of the completed operation but there are a few fundamental
aspects to highlight;

1. the "key" informs us of the key that the operation was performed on.

2. the "eventName" informs us of the type of operation that was
   performed.  The 2 most notable eventNames are **ObjectCreated:Put** and
   **ObjectRemoved:Deleted** which pertain to key creation and deletion respectively.

3. operation specific fields can be included within the "object" sub-object (in
   the above example we can see that "size" and "eTag" of the created object are included)

## Applicability to Ozone

For non-FSO Ozone buckets / operations there is a clear mapping between
operations such as CreateKey / CommitKey / DeleteKey / RenameKey and the
standard S3 event notification semantics.

Examples:

1. CommitKey could be mapped to a ObjectCreated:Put "/path/to/keyToCreate" notification event

2. DeleteKey could be mapped to a ObjectRemoved:Deleted "/path/to/keyToDelete" notification event

3. RenameKey (assuming a file based key) in standard S3 event noification semantics would produce 2 events:

- a ObjectRemoved:Deleted event for the source path of the rename
- a ObjectCreated:Put event for the destination path of the rename

The challenge in adopting S3 Event notification semantics within Ozone
would be in at least 2 areas:

### 1. FSO hierarchical operations which impact multiple child keys

Example: directory renames

To illustrate with an example: lets say we have the following simple directory structure:

```
  /vol1/bucket1/myfiles/f1
  /vol1/bucket1/myfiles/f2
  /vol1/bucket1/myfiles/subdir/f1
```

If a user performs a directory rename such as:

```
  ozone fs -mv /vol1/bucket1/myfiles /vol1/bucket1/myfiles-RENAMED
```

Within standard S3 event notification semantics we would expect to see 6 notifications
emitted in that case:

```
  eventName=ObjectRemoved:Deleted, key=/vol1/bucket1/myfiles/f1
  eventName=ObjectRemoved:Deleted, key=/vol1/bucket1/myfiles/f2
  eventName=ObjectRemoved:Deleted, key=/vol1/bucket1/myfiles/subdir/f1
  eventName=ObjectCreated:Put, key=/vol1/bucket1/myfiles-RENAMED/f1
  eventName=ObjectCreated:Put, key=/vol1/bucket1/myfiles-RENAMED/f2
  eventName=ObjectCreated:Put, key=/vol1/bucket1/myfiles-RENAMED/subdir/f1
```

However, with an approach of simply producing notifications based on Ratis
state machine events then all we would have to go on from the
RenameKeyRequest would be the fromKeyName and the toKeyName of the
*parent* of the directory being renamed (and not the impacted child
objects).

Therefore to produce notifications using the standard S3 event
notification semantics for FSO directory renames we would need to
consider the trade-offs between compatibility with the normal S3
semantics for renames vs a custom event type for directory renames.

### most compatible approach

We could introduce some additional processing before emitting notification
events in the case of a directory rename which "gathers together" (prior
to the change being committed to the DB) the child objects impacted by
the directory rename and emits pairs of delete/create events for each
key (as described above)

Pros:
- standard S3 event notification rename semantics

Cons:
- additional processing to pull together the events.  This could mean an
  unknown amount of additional processing for large directory renames.
- could be a performance drag if performed on the leader

### custom event type

Conversely - we could opt to not try to be fully compliant with existing S3 event notification
semantics since the schema was designed for non-hierarchical filesystems and
instead create some custom event extension (e.g. ObjectRenamed:) and
emit just a single event for directory renames which specifies only the parent
paths impacted by the rename:

e.g.
```
  eventName=ObjectReanmed:Reanmed, fromKey=myfiles, toKey=myfiles-RENAMED
```

.. it would then be up to the notification consumer to deal with the
different rename event semantics (i.e. that only the parent names were
notified and not the impacted child objects).

This is the same semantics used in the HDFS inotify directory rename
event (see below).

Pros:
- no additional processing when emitting events

Cons:
- non-standard S3 event notification semantics

NOTE: directory rename is just one example of a hierarchical FSO
operation which impacts child objects.  There may be other Ozone
hierarchical FSO operations which will need be catered for in a similar
way (recursive delete?)

### 2. Metadata changes

The standard S3 event notification schema does not have provision for
notifying about metadata changes.

Therefore to support notifying about metadata changes one option would
be to add a custom event type. e.g. ObjectMetadataUpdated:*

It is worth noting here that Ceph has some custom extensions,
so there is some precedent for that:
https://docs.ceph.com/en/latest/radosgw/s3-notification-compatibility/#event-types


## 2. HDFS event schema

The HDFS inotify event notification schema

[https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/hdfs/inotify/package-summary.html](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/hdfs/inotify/package-summary.html)

allows a HDFS client with suitable privileges to poll the HDFS namenode
for notifications pertaining to changes on the filesystem across the entire cluster
(i.e. there is no granular per-directory subscription).

The notifications use a binary protocol (protobuf).  The protobuf specs
for the notification events can be found here:

https://github.com/apache/hadoop/blob/3d905f9cd07d118f5ea0c8485170f5ebefb84089/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/inotify.proto#L62


## Applicability to Ozone

Since HDFS is a hierarchical filesystem there is a natural mapping to
the FSO operations within Ozone.

For example:

* a directory rename is emitted as a RenameEvent
  (https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/hdfs/inotify/Event.RenameEvent.html) with
  srcPath=/path/to/old-dir, dstPath=/path/to/new-dir (i.e. there is no
  expectation that the impact on child objects will be notified)

* a recursive delete is emitted as a UnlinkEvent
  (https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/hdfs/inotify/Event.UnlinkEvent.html) on the parent

* metadata changes (such as changes to permissions, replication,
  owner/group, acls, xattr etc.

are sent via a MetadataUpdateEvent
(https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/hdfs/inotify/Event.MetadataUpdateEvent.html)

This would be a good starting point for Ozone but would require some
bespoke changes as acls, for example, do not have a one-to-one mapping
to HDFS concepts.

Pros:
- clear mapping for FSO and non-FSO operations such as directory renames
- caters for metadata operations by design (although would require some
  customization)

Cons:
- not ubiquitous across many storage solutions in the way that the S3 Event Notification schema is
