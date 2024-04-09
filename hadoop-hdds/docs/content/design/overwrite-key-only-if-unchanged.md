---
title: Overwriting an Ozone Key only if it has not changed.
summary: A minimal design illustrating how to replace a key in Ozone only if it has not changes since it was read.
date: 2024-04-05
jira: HDDS-10657
status: accepted
author: Stephen ODonnell
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


Ozone offers write semantics where the last writer to commit a key wins. Therefore multiple writers can concurrently write the same key, and which ever commits last will effectively overwrite all data that came before it.

As an extension of this, there is no "locking" on a key which is being replaced.

For any key, but especially a large key, it can take significant time to read and write it. There are scenarios where it would be desirable to replace a key in Ozone, but only if the key has not changed since it was read. With the absence of a lock, such an operation is not possible today.

## As Things Stand

Internally, all Ozone keys have both an objectID and UpdateID which are stored in OM as part of the key metadata.

Each time something changes on the key, whether it is data or metadata, the updateID is changed. It comes from the ratis transactionID and is generally an increasing number.

When an existing key is over written, its existing metadata including the ObjectID and ACLs are mirrored onto the new key version. The only metadata which is replaced is any custom metadata stored on the key by the user. Upon commit, the updateID is also changed to the current Ratis transaction ID.

Writing a key in Ozone is a 3 step process:

1. The key is opened via an Open Key request from the client to OM
2. The client writes data to the data nodes
3. The client commits the key to OM via a Commit Key call.

Note, that as things stand, it is possible to lose metadata updates (eg ACL changes) when a key is overwritten.

1. If the key exists, then a new copy of the key is open for writing.
2. While the new copy is open, another process updates the ACLs for the key
3. On commit, the new ACLs are not copied to the new key as the new key made a copy of the existing metadata at the time the key was opened.

With the technique described in the next section, that problem is removed in this design, as the ACL update will change the updateID, and the key will not be committed.

## Atomic Key Replacement

In relational database applications, records are often assigned an update counter similar to the updateID for a key in Ozone. The data record can be read and displayed on a UI to be edited, and then written back to the database. However another user could have made an edit to the same record in the mean time, and if the record is written back without any checks, those edits could be lost.

To combat this, "optimistic locking" is used. With Optimistic locking, no locks are actually involved. The client reads the data along with the update counter. When it attempts to write the data back, it validates the record has not change by including the updateID in the update statement, eg:

```
update customerDetails
set <columns = values>
where customerID = :b1
and updateCounter = :b2
```
If no records are updated, the application must display an error or reload the customer record to handle the problem.

In Ozone the same concept can be used to perform an atomic update of a key only if it has not changed since the key details were originally read.

To do this:

1. The client reads the key details as usual. The key details can be extended to include the existing updateID as it is currently not passed to the client.
2. The client opens a new key for writing with the same key name as the original, passing the previously read updateID in a new field. Call this new field overwriteExpectedUpdateID.
3. On OM, it receives the openKey request as usual and detects the presence of the overwriteExpectedUpdateID.
4. On OM, it first ensures that a key is present with the given key name and having a updateID == overwriteExpectedUpdateID. If so, it opens the key and stored the details including the overwriteExpectedUpdateID in the openKeyTable. As things stand, the other existing key metadata copied from the original key is stored in the openKeyTable too.
5. The client continues to write the data as usual.
6. On commit key, the client does not need to send the overwriteExpectedUpdateID again, as the open key contains it.
7. On OM, on commit key, it validates the key still exists with the given key name and its updateID is unchanged. If so the key is committed, otherwise an error is returned to the client.

Note that any change to a key will change the updateID. This is existing behaviour, and committing an rewritten key will also modify the updateID. Note this also offers protection against concurrent rewrites. 

## Changes Required

In order to enable the above steps on Ozone, several small changes are needed.

### Wire Protocol

1. The overwriteExpectedUpdateID needs to be added to the KeyInfo protobuf object so it can be stored in the openKey table.
2. The overwriteExpectedUpdateID needs to be added to the keyArgs protobuf object, which is passed from the client to OM when creating a key.

No new messages need to be defined.

### On OM

No new OM handlers are needed. The existing OpenKey and CommitKey handlers will receive the new overwriteExpectedUpdateID and perform the checked.

No new locks are needed on OM. As part of the openKey and commitKey, there are existing locks taken to ensure the key open / commit is atomic. The new checks are performed under those locks, and come down to a couple of long comparisons, so add negligible overhead.

### On The Client

 1. We need to allow the updateID of an existing key to be accessible when an existing details are read, by adding it to OzoneKey and OzoneKeyDetails. There are internal object changes and do no impact any APIs.
 2. To pass the overwriteExpectedUpdateID to OM on key open, it would be possible to overload the existing OzoneBucket.createKey() method, which already has several overloaded versions, or create a new explicit method on Ozone bucket called replaceKeyIfUnchanged, passing either the OzoneKeyDetails of the existing key (which includes the key name and existing updateID, or by passing the key name and updateID explicitly, eg:
 
 ```
 public OzoneOutputStream replaceKeyIfUnchanged(OzoneKeyDetails keyToOverwrite, ReplicationConfig replicationConfigOfNewKey)
      throws IOException 
	  
// Alternatively or additionally

 public OzoneOutputStream replaceKeyIfUnchanged(String volumeName, String bucketName, String keyName, long size, long expectedUpdateID, ReplicationConfig replicationConfigOfNewKey)
      throws IOException 

	  
 ```
This specification is roughly in line with the exiting createKey method:

```
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata)
```

An alternative, is to create a new overloaded createKey:

```
  public OzoneOutputStream createKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, long expectedUpdateID)
```

Note the omission of the metaData map, as the intention of this API is to copy that from what already exisits on the server.

The intended usage of this API, is that the existing key details are read, then used to open the new key, and then data is written, eg:

```
OzoneKeyDetails exisitingKey = bucket.getKey(keyName);
try (OutputStream os = bucket.replaceKeyIfUnchanged(existingKey, newRepConfig) {
  os.write(bucket.readKey(keyName))
}
```

## Other Storage Systems

Amazon S3 does not offer a facility like this.

Google Cloud has a concept of a generationID which is used in various [API calls](https://cloud.google.com/storage/docs/json_api/v1/objects/update).

## Further Ideas

The intention of this initial design is to make as few changes to Ozone as possible to enable overwriting a key if it has not changed.

It would be possible to have separate UpdateIDs for metadata changes and data changes to give a more fine grained approach.

It would also be possible to expose these IDs over the S3 interface as well as the Java interface.

However both these options required more changes to Ozone and more API surface to test and support.

The changes suggested here are small, and carry little risk to existing operations if the new field is not passed. They also do not rule out extending the idea to cover a separate metadata UpdateID if such a thing is desired by enough users.
