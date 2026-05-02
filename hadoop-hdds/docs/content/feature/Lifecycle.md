---
title: "Object Lifecycle Management"
weight: 1
menu:
   main:
      parent: Features
summary: S3-compatible object lifecycle management with automatic object expiration
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

## Background

In object storage scenarios, large amounts of data become obsolete over time and no longer need to be accessed or retained. Manually cleaning up expired data is both time-consuming and error-prone. Object lifecycle management provides an automated approach that allows administrators to configure policies at the bucket level so the system can automatically handle the cleanup of expired objects.

Ozone's object lifecycle management is designed after [AWS S3 Lifecycle Configuration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) and provides compatible API interfaces through the S3 Gateway. The current version implements the Expiration action, which automatically deletes or moves objects to trash based on the object's last modification time.

## Compatibility with AWS S3 Lifecycle

Ozone's lifecycle management is designed with AWS S3 Lifecycle as a reference. The current version does not implement all S3 Lifecycle features. Below is a detailed compatibility comparison.

### API Compatibility

| S3 API | Supported | Description |
|--------|-----------|-------------|
| `PutBucketLifecycleConfiguration` | Yes | Set via S3 Gateway `PUT /{bucket}?lifecycle` |
| `GetBucketLifecycleConfiguration` | Yes | Get via S3 Gateway `GET /{bucket}?lifecycle` |
| `DeleteBucketLifecycle` | Yes | Delete via S3 Gateway `DELETE /{bucket}?lifecycle` |

### Lifecycle Actions

| S3 Lifecycle Action | Supported | Description |
|---------------------|-----------|-------------|
| Expiration | Yes | Supports both `Days` and `Date` modes |
| Transition | No | Ozone does not currently support tiered storage class transitions similar to S3 |
| NoncurrentVersionExpiration | No | Ozone's bucket versioning mechanism differs from S3 |
| NoncurrentVersionTransition | No | Same as above |
| AbortIncompleteMultipartUpload [1] | No | Automatic cleanup of incomplete multipart uploads is not implemented |
| ExpiredObjectDeleteMarker | No | Ozone does not use S3-style delete markers |

[1] Ozone has a separate cleanup service for incomplete multipart uploads (MultipartUploadCleanupService)

### Filter Conditions

| S3 Filter Element | Supported | Description |
|--------------------|-----------|-------------|
| Prefix | Yes | Supports both top-level Prefix and Prefix within Filter |
| Tag | Yes | Supports filtering by a single tag |
| And (Prefix + Tags) | Yes | Supports combining Prefix with multiple Tag conditions |
| ObjectSizeGreaterThan | No | Filtering by minimum object size is not supported |
| ObjectSizeLessThan | No | Filtering by maximum object size is not supported |

### Other Differences

- Ozone-specific feature: Ozone supports moving expired objects to trash (`.Trash`) instead of deleting them directly.
- Bucket Layout: Ozone's FSO (FILE_SYSTEM_OPTIMIZED) buckets support recursive directory-tree-based evaluation and can automatically expire empty directories.
- Administrative operations: Ozone provides `suspend` / `resume` commands to dynamically control the lifecycle service (S3 achieves similar effects through disabling rule `Status`, which Ozone also supports), allowing you to stop all lifecycle processing directly.

## Lifecycle Configuration

The overall configuration rules of Ozone lifecycle are essentially the same as AWS S3 Lifecycle semantics. Refer to AWS S3 Lifecycle documentation for more details on the rules.

### Overall Structure

A Lifecycle Configuration is bound to a bucket. Each bucket can have at most one lifecycle configuration, and each configuration can contain up to 1000 rules.

Each rule contains the following elements:

| Element | Description |
|---------|-------------|
| ID | Unique identifier for the rule, up to 255 characters. Auto-generated if not specified. |
| Status | `Enabled` or `Disabled`. Only enabled rules are executed. |
| Filter / Prefix | Specifies the scope of the rule. Can filter by object name prefix. |
| Expiration | Expiration action. Specifies when objects expire via `Days` or `Date`. |

### Expiration Action

The expiration action supports two modes:

- Days: Objects expire after the specified number of days since the last modification time. Must be a positive integer and cannot be 0.
- Date: Specifies a UTC point in time. All objects last modified before that time are considered expired.

Each rule can specify at most one Expiration action. `Days` and `Date` are mutually exclusive.

Expiration validation rules:

- Exactly one of `Days` or `Date` must be specified. They cannot be specified together, nor can both be omitted.
- `Days` must be a positive integer greater than zero.
- `Date` must conform to ISO 8601 format and must include both the time and timezone components (they cannot be omitted). Valid examples: `2042-04-02T00:00:00Z`, `2042-04-02T00:00:00+00:00`.
- `Date` must resolve to midnight UTC (`00:00:00`) after timezone conversion. Non-zero hours, minutes, or seconds are not allowed.
- `Date` must be a future time relative to when the lifecycle configuration is created. Past dates are not accepted.

### Filter and Prefix

Rules can specify their scope in the following ways:

- Prefix (top-level): Set the Prefix field directly on the Rule. Applies to all objects matching the prefix.
- Filter: Specified via the Filter element, supporting:
  - `Prefix`: Filter by prefix.
  - `Tag`: Filter by a single tag (Key/Value pair).
  - `And`: Combine Prefix with multiple Tag conditions.

General validation rules:

- Prefix and Filter cannot be used simultaneously, nor can both be omitted.
- Setting Prefix to an empty string `""` means the rule applies to all objects in the bucket.
- Prefix length cannot exceed 1024 bytes.
- Prefix cannot point to trash directories (paths starting with `.Trash` or `.Trash/`).
- Only one of Prefix, Tag, or And can be specified inside a Filter.
- Tag Key length must be between 1 and 128 bytes. Tag Value length must be between 0 and 256 bytes.
- Tag Keys within an And operator must be unique.
- The And operator must contain at least one Tag. Specifying only a Prefix without Tags is not allowed. If there is no Prefix, the number of Tags must be greater than 1.

Additional validation rules for FSO buckets:

For FILE_SYSTEM_OPTIMIZED (FSO) buckets, the Prefix must be a normalized and valid path. The requirements are:

- Cannot start with `/`. FSO bucket prefixes are relative to the bucket root and do not need a leading slash.
- Cannot contain consecutive slashes `//`.
- Path components cannot contain `.` (current directory), `..` (parent directory), or `:`.

The following table shows examples of valid and invalid prefixes:

| Prefix | Valid for FSO Bucket | Reason |
|--------|----------------------|--------|
| `logs/` | Valid | Normalized directory prefix |
| `data/2024/` | Valid | Multi-level directory prefix |
| `archive` | Valid | Simple prefix without slash |
| `/logs/` | Invalid | Cannot start with `/`, use `logs/` instead |
| `data//backup/` | Invalid | Contains consecutive slashes `//`, use `data/backup/` instead |
| `data/../secret/` | Invalid | Contains `..`, parent directory references are not allowed |
| `data/./logs/` | Invalid | Contains `.`, current directory references are not allowed |
| `.Trash/` | Invalid | Cannot point to trash directories |

## S3 Gateway API

Lifecycle configurations are managed through standard S3 API operations using the `?lifecycle` query parameter.

### Set Lifecycle Configuration

Example using the `Days` mode:

```json
{
  "Rules": [
    {
      "ID": "expire-logs-after-30-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
```

Example using the `Date` mode:

```json
{
  "Rules": [
    {
      "ID": "expire-temp-data",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "temp/"
      },
      "Expiration": {
        "Date": "2042-04-02T00:00:00Z"
      }
    }
  ]
}
```

Example using `And` to combine Prefix and Tag filtering:

```json
{
  "Rules": [
    {
      "ID": "expire-tagged-objects",
      "Status": "Enabled",
      "Filter": {
        "And": {
          "Prefix": "data/",
          "Tags": [
            {
              "Key": "environment",
              "Value": "dev"
            }
          ]
        }
      },
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

Set lifecycle configuration using AWS CLI:

```shell
aws s3api put-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9878 \
  --lifecycle-configuration file://lifecycle.json
```

### Get Lifecycle Configuration

GET `/{bucket}?lifecycle`

```shell
aws s3api get-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9878
```

### Delete Lifecycle Configuration

DELETE `/{bucket}?lifecycle`

```shell
aws s3api delete-bucket-lifecycle \
  --bucket mybucket \
  --endpoint-url http://localhost:9878
```

## Bucket Layout Support

Ozone supports three bucket layouts: OBJECT_STORE (OBS), LEGACY, and FILE_SYSTEM_OPTIMIZED (FSO). The lifecycle management behavior varies across different layouts.

### OBS and LEGACY Buckets

For OBS and LEGACY buckets, the lifecycle service directly iterates through the Key Table and performs prefix matching on key names. Objects that match and meet the expiration criteria are either deleted directly (OBS buckets do not support trash) or moved to trash (LEGACY buckets).

### FSO Buckets

For FSO buckets, the lifecycle service performs recursive evaluation based on the directory tree:

1. Parses the directory path from the prefix and locates the corresponding directory in the directory table.
2. Traverses the directory tree in depth-first order, evaluating files and subdirectories level by level.
3. If all files and subdirectories under a directory have expired, the directory itself is also marked as expired.

Prefix semantic differences:

| Prefix | OBS/LEGACY Behavior | FSO Behavior |
|--------|---------------------|--------------|
| `""` (empty) | Matches all objects | Matches all objects and directories |
| `key` | Matches all keys starting with `key` | Matches files and directories starting with `key` |
| `dir/` | Matches all keys starting with `dir/` | Matches files and subdirectories under `dir`, excluding `dir` itself |
| `dir1/dir2` | Matches all keys starting with `dir1/dir2` | Matches files and directories under `dir1` starting with `dir2` |

<div class="alert alert-warning" role="alert">

For FSO buckets, a directory's ModificationTime is not updated when its child files or subdirectories change. Therefore, when configuring prefixes, if you only want to expire the contents under a directory without accidentally deleting the directory itself, append `/` to the end of the prefix. For example, use `data/` instead of `data`.

</div>

## Trash Integration

By default, the lifecycle service moves expired objects to trash (the `.Trash` directory) instead of deleting them directly. This provides a layer of protection against accidental operations.

- When `ozone.lifecycle.service.move.to.trash.enabled` is set to `true` (the default), expired objects are moved to the `.Trash/<owner>/Current/` path.
- OBS buckets do not support trash; expired objects are deleted directly.
- When set to `false`, all expired objects are deleted directly.

Objects moved to trash still follow Ozone's trash cleanup policy and will be permanently deleted after the retention period.

## Configuration

The lifecycle service is disabled by default and must be explicitly enabled in `ozone-site.xml`.

```XML
<property>
   <name>ozone.lifecycle.service.enabled</name>
   <value>true</value>
   <description>Enable the object lifecycle management service.</description>
</property>
```

The following table lists all related configuration properties:

| Property | Default | Description |
|----------|---------|-------------|
| `ozone.lifecycle.service.enabled` | `false` | Whether to enable the lifecycle management service. |
| `ozone.lifecycle.service.interval` | `24h` | The scan interval of the lifecycle management service. |
| `ozone.lifecycle.service.timeout` | `2h` | The timeout threshold for lifecycle evaluation tasks. This setting does not interrupt a running task. It only prints a WARN-level log after a task completes if the actual execution time of a single bucket's evaluation exceeds this value. |
| `ozone.lifecycle.service.workers` | `5` | The number of worker threads for the lifecycle management service. Must be greater than 0. Each bucket is handled by one thread. The maximum number of buckets processed concurrently equals this value; remaining buckets are queued. Setting this too high increases concurrent RocksDB reads/writes and Ratis request pressure on the OM, potentially affecting cluster performance. |
| `ozone.lifecycle.service.delete.batch-size` | `1000` | The maximum number of objects included in a single batch delete request. Each batch of keys is packaged into a single Ratis delete request submitted to the OM. Excessively large batches increase the size of individual Ratis log entries and consume more memory. It is not recommended to exceed 1000. |
| `ozone.lifecycle.service.move.to.trash.enabled` | `true` | When enabled, expired objects are moved to trash; when disabled, they are deleted directly. Not applicable to OBS buckets. |
| `ozone.lifecycle.service.delete.cached.directory.max-count` | `1000000` | The maximum number of directories cached in memory during recursive evaluation of FSO buckets. The current evaluation will be aborted if this limit is exceeded. |

## Administrative Operations

Ozone provides administrative commands to view and control the lifecycle service runtime status.

### Check Service Status

```shell
ozone admin om lifecycle status [-id=<omServiceId>] [-host=<omHost>]
```

### Suspend Service

```shell
ozone admin om lifecycle suspend [-id=<omServiceId>] [-host=<omHost>]
```

After suspension, the service will not start new evaluation tasks. Tasks already in progress will stop after detecting the suspended state.

<div class="alert alert-warning" role="alert">

The suspend operation is only effective for the current OM runtime. After an OM restart, the service will resume according to the value of `ozone.lifecycle.service.enabled`.

</div>

### Resume Service

```shell
ozone admin om lifecycle resume [-id=<omServiceId>] [-host=<omHost>]
```

## Considerations

- Lifecycle configuration can currently only be set through the S3 API. To configure lifecycle rules for a bucket, you must have the S3 Gateway (S3G) service running and access the bucket via the S3 interface.
- The lifecycle service is disabled by default. Set `ozone.lifecycle.service.enabled=true` to enable it.
- In OM HA mode, only the leader OM executes lifecycle evaluation tasks.
- For FSO buckets, a directory is only marked as expired and deleted if all its child files and subdirectories have expired.
- For FSO buckets using Prefix, if the Prefix does not end with `/`, it will match both the directory with the exact name and sibling directories starting with the same prefix (e.g., `dir` matches both `dir` and `dir1`).

### Impact of OM Leader Transfer on the Lifecycle Service

In an OM HA deployment, the lifecycle service only runs on the leader OM. When a Transfer Leader operation is performed:

1. The lifecycle evaluation tasks running on the old leader will be interrupted.
2. After the new leader is elected, the lifecycle service restarts from the beginning. Previously evaluated buckets are not skipped, and the task starts over from the first bucket.

Therefore, in scenarios with frequent leader transfers, it is recommended to monitor the actual execution of the lifecycle service to ensure expired objects are cleaned up in a timely manner.

### Impact of Mass Key Expiration on Metadata Performance

If a large number of keys in a bucket expire and are deleted within a short period (e.g., over 100 million keys within 24 hours), it may generate a large number of tombstone records in RocksDB, leading to the following issues:

- Significantly increased metadata operation latency on the affected bucket, especially for operations like `list` that require iterating through RocksDB.
- Degraded read performance, as RocksDB needs to skip over large numbers of deleted tombstone records during queries.

Mitigation measures:

1. Enable the automatic compaction service (recommended): Set `ozone.om.compaction.service.enabled=true` and ensure that `ozone.om.compaction.service.columnfamilies` includes `keyTable,fileTable,directoryTable` (included by default). This service runs compaction every 6 hours by default, adjustable via `ozone.om.compaction.service.run.interval`.

2. Manual compaction: Use the `ozone repair om compact` command to manually compact the affected column families (requires admin privileges, OM must be running):

```bash
ozone repair om compact --cf keyTable
ozone repair om compact --cf fileTable
ozone repair om compact --cf directoryTable
```

In an OM HA environment, you can specify the target OM node via `--service-id` and `--node-id`:

```bash
ozone repair om compact --cf keyTable --service-id omServiceId --node-id om1
```

The compaction operation is executed asynchronously. Check the corresponding OM node's logs for completion status.

## References

 * [Design Document]({{< ref path="design/s3-object-lifecycle-management.md" lang="en">}})
 * [AWS S3 Lifecycle Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
 * [AWS S3 Object Expiration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)
 * [AWS S3 Setting Lifecycle Configuration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html)
 * [AWS S3 Lifecycle Configuration Examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)
