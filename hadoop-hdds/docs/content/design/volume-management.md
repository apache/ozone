---
title: Ozone Volume Management
summary: A simplified version of mapping between S3 buckets and Ozone volume/buckets
date: 2020-04-02
jira: HDDS-3331
status: accepted
author: Marton Elek, Arpit Agarwal, Sanjay Radia
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


## Introduction

This document explores how we can improve the Ozone volume semantics especially with respect to the S3 compatibility layer.

## The Problems

 1. Unprivileged users cannot enumerate volumes.
 2. The mapping of S3 buckets to Ozone volumes is confusing. Based on external feedback it's hard to understand the exact Ozone URL to be used.
 3. The volume name is not friendly and cannot be remembered by humans.
 4. Ozone buckets created via the native object store interface are not visible via the S3 gateway.
 5. We don't support the revocation of access keys.

We explore some of these in more detail in subsequent sections.

### Volume enumeration problem

Currently when a user enumerates volumes, they see the list of volumes that they own. This means that when an unprivileged user enumerates volumes, it always gets any empty list. Instead users should be able to see all volumes that they have been granted read or write access to.

This also has an impact on [ofs](https://issues.apache.org/jira/browse/HDDS-2665) which makes volumes appear as top-level directories.

### S3 to HCFS path mapping problem

Ozone has the semantics of volume *and* buckets while S3 has only buckets. To make it possible to use the same bucket both from Hadoop world and via S3 we need a mapping between them.

Currently we maintain a map between the S3 buckets and Ozone volumes + buckets in `OmMetadataManagerImpl`

```
s3_bucket --> ozone_volume/ozone_bucket
```
 
The current implementation uses the `"s3" + s3UserName` string as the volume name and the `s3BucketName` as the bucket name. Where `s3UserName` is is the `DigestUtils.md5Hex(kerberosUsername.toLowerCase())`

To create an S3 bucket and use it from o3fs, you should:

1. Get your personal secret based on your kerberos keytab

```
> kinit -kt /etc/security/keytabs/testuser.keytab testuser/scm
> ozone s3 getsecret
awsAccessKey=testuser/scm@EXAMPLE.COM
awsSecret=7a6d81dbae019085585513757b1e5332289bdbffa849126bcb7c20f2d9852092
```

2. Create the bucket with S3 cli

```
> export AWS_ACCESS_KEY_ID=testuser/scm@EXAMPLE.COM
> export AWS_SECRET_ACCESS_KEY=7a6d81dbae019085585513757b1e5332289bdbffa849126bcb7c20f2d9852092
> aws s3api --endpoint http://localhost:9878 create-bucket --bucket=bucket1
```

3. And identify the ozone path

```
> ozone s3 path bucket1
Volume name for S3Bucket is : s3c89e813c80ffcea9543004d57b2a1239
Ozone FileSystem Uri is : o3fs://bucket1.s3c89e813c80ffcea9543004d57b2a1239
```

## Proposed solution[1]

### Supporting multiple access keys (#5 from the problem listing)

Problem #5 can be easily supported with improving the `ozone s3` CLI. Ozone has a separated table for the S3 secrets and the API can be improved to handle multiple secrets for one specific kerberos user.

### Solving the mapping problem (#2-4 from the problem listing)

 1. Let's always use `s3v` volume for all the s3 buckets **if the bucket is created from the s3 interface**.

This is an easy an fast method, but with this approach not all the volumes are available via the S3 interface. We need to provide a method to publish any of the ozone volumes / buckets.

 2. Let's improve the existing toolset to expose **any** Ozone volume/bucket as an s3 bucket. (Eg. expose `o3:/vol1/bucketx` as an S3 bucket `s3://foobar` )

**Implementation**:

 The first part is easy compared to the current implementation. We don't need any mapping table any more.

 To implement the second (expose ozone buckets as s3 buckets) we have multiple options:

   1. Store some metadata (** s3 bucket name **) on each of the buckets
   2. Implement a **symbolic link** mechanism which makes it possible to *link* to any volume/buckets from the "s3" volume.

The first approach required a secondary cache table and it violates the naming hierarchy. The s3 bucket name is a global unique name, therefore it's more than just a single attribute on a specific object. It's more like an element in the hierarchy. For this reason the second option is proposed:

For example if the default s3 volume is `s3v`

 1. Every new buckets created via s3 interface will be placed under the `/s3v` volume
 2. Any existing **Ozone** buckets can be exposed by linking to it from s3: `ozone sh bucket link /vol1/bucket1 /s3v/s3bucketname`

**Lock contention problem**

One possible problem with using just one volume is using the locks of the same volume for all the S3 buckets (thanks Xiaoyu). But this shouldn't be a big problem.

 1. We hold only a READ lock. Most of the time it can acquired without any contention (writing lock is required only to change owner / set quota)
 2. For symbolic link the read lock is only required for the first read. After that the lock of the referenced volume will be used. In case of any performance problem multiple volumes and links can be used.

Note: Sanjay is added to the authors as the original proposal of this approach.

#### Implementation details

 * `bucket link` operation creates a link bucket.  Links are like regular buckets, stored in DB the same way, but with two new, optional pieces of information: source volume and bucket.  (The bucket being referenced by the link is called "source", not "target", to follow symlink terminology.)
 * Link buckets share the namespace with regular buckets.  If a bucket or link with the same name already exists, a `BUCKET_ALREADY_EXISTS` result is returned.
 * Link buckets are not inherently specific to a user, access is restricted only by ACL.
 * Links are persistent, ie. they can be used until they are deleted.
 * Existing bucket operations (info, delete, ACL) work on the link object in the same way as they do on regular buckets.  No new link-specific RPC is required.
 * Links are followed for key operations (list, get, put, etc.).  Read permission on the link is required for this.
 * Checks for existence of the source bucket, as well as ACL, are performed only when following the link (similar to symlinks).  Source bucket is not checked when operating on the link bucket itself (eg. deleting it).  This avoids the need for reverse checks for each bucket delete or ACL change.
 * Bucket links are generic, not restricted to the `s3v` volume.

## Alternative approaches and reasons to reject

To solve the the _s3 bucket name to ozone bucket name mapping_ problem some other approaches are also considered. They are rejected but keeping them in this section together with the reasons to reject.


### 1. Predefined volume mapping

 1. Let's support multiple `ACCESS_KEY_ID` for the same user.
 2. For each `ACCESS_KEY_ID` a volume name MUST be defined.
 3. Instead of using a specific mapping table, the `ACCESS_KEY_ID` would provide a **view** of the buckets in the specified volume.
 
With this approach the used volume will be more visible and -- hopefully -- understandable.

Instead of using `ozone s3 getsecret`, following commands would be used:

 1. `ozone s3 secret create --volume=myvolume`: To create a secret and use myvolume for all of these buckets
 2. `ozone s3 secret list`: To list all of the existing S3 secrets (available for the current user)
 3. `ozone s3 secret delete <ACCESS_KEY_ID`: To delete any secret

The `AWS_ACCESS_KEY_ID` should be a random identifier instead of using a kerberos principal.

 * __pro__: Easier to understand
 * __con__: We should either have global unique bucket names or it will be possible to see two different buckets with
 * __con__: It can be hard to remember which volumes are assigned to a specific ACCESS_KEY_ID
 
### 3. String Magic

We can try to make volume name visible for the S3 world by using some structured bucket names. Unfortunately the available separator characters are very limited:

For example we can't use `/`

```
aws s3api create-bucket --bucket=vol1/bucket1

Parameter validation failed:
Invalid bucket name "vol1/bucket1": Bucket name must match the regex "^[a-zA-Z0-9.\-_]{1,255}$" or be an ARN matching the regex "^arn:(aws).*:s3:[a-z\-0-9]+:[0-9]{12}:accesspoint[/:][a-zA-Z0-9\-]{1,63}$"
```

But it's possible to use `volume-bucket` notion:

```
aws s3api create-bucket --bucket=vol1-bucket1
```
 * __pro__: Volume mapping is visible all the time.
 * __con__: Harder to use any external tool with defaults (all the buckets should have at least one `-`)
 * __con__: Hierarchy is not visble. The uniform way to separated elements in fs hierarchy is `/`. It can be confusing.
 * 
### 4. Remove volume From OzoneFs Paths

We can also make volumes a lightweight *bucket group* object by removing it from the ozonefs path. With this approach we can use all the benefits of the volumes as an administration object but it would be removed from the `o3fs` path.

 * __pro__: can be the most simple solution. Easy to understand as there are no more volumes in the path.
 * __con__: Bigger change (all the API can't be modified to make volumes optional)
 * __con__: Harder to dis-joint namespaces based on volumes. (With the current scheme, it's easier to delegate the responsibilities for one volumes to a different OM).
 * __con__: We lose volumes as the top-level directories in `ofs` scheme.
 * __con__: One level of hierarchy might not be enough in case of multi-tenancy.
 * __con__: One level of hierarchy is not enough if we would like to provide separated level for users and admins 
 * __con__: Hierarchical abstraction can be easier to manage and understand

