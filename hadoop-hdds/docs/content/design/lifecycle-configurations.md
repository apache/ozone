---
title: AWS S3 Lifecycle Configurations
summary: Enables users to manage lifecycle configurations for buckets, allowing automated deletion of keys based on predefined rules.
date: 2024-04-25
jira: HDDS-8342
status: draft
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

# Lifecycle Management

## Introduction
I encountered the need for a retention solution within my cluster, specifically the ability to delete keys in specific paths after a certain time period.   
This requirement closely resembled the functionality provided by AWS S3 Lifecycle configurations, particularly the Expiration part ([AWS S3 Lifecycle Configuration Examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)).  


## term
- Lifecycle:
  - S3 Lifecycle is a set of rules that automatically manage object storage.
  - Each Bucket can have 0 or 1 Lifecycle, and each Lifecycle can have multiple Rules.
- Rule:
  - A Lifecycle Rule specifies which objects are affected and how they should be handled.
    Each Rule includes:
    - Filters (Prefix or Tag, etc)
    - Lifecycle actions (deleting objects or transitioning objects)
- Filter: Filters determine which objects a Lifecycle Rule applies to, they can be based on Prefix, Tag, etc.
- Lifecycle actions: Actions define what a Lifecycle Rule does, including: Transition, Expiration, etc.
- Lifecycle configurations: A definition of a Lifecycle. Users can define a Lifecycle through json or xml.


## Overview

### Functionality
- User should be able to create/remove/fetch lifecycle configurations for a specific S3 bucket.
- The lifecycle configurations will be executed periodically.
  - Depending on the rules of the lifecycle configuration there could be different actions or even multiple actions. 
  - At the moment only expiration is supported (keys get deleted).
- The lifecycle configurations supports all buckets not only S3 buckets.
- Compatible with AWS S3 lifecycle management commands.

### Components
#### S3G
- Responsible for implementing the relevant protocols defined by AWS S3, including Lifecycle addition, deletion, fetch, etc.
#### OM
- Responsible for saving the configuration of Lifecycle.
- Responsible for the execution of Lifecycle.


## Lifecycle configurations
- Lifecycle configurations are persisted in the DB by OM, and each Bucket can have at most 1 Lifecycle configurations.
- Lifecycle configurations and Buckets have a 1-to-1 relationship, and Lifecycle configurations will be saved in a separate DB instead of being a field of Bucket.
- According to Bucket, the Lifecycle configurations of the Bucket can be obtained from the DB.
- Lifecycle configurations reference:

```xml
<LifecycleConfiguration>
    <Rule>
         <Element>
    </Rule>
    <Rule>
         <Element>
         <Element>
    </Rule>
</LifecycleConfiguration>
```


## Retention Manager
RetentionManager is responsible for managing the Lifecycle of Bucket, regularly checking and performing corresponding operations according to the rules. RetentionManager will start when OM is initialized (if allowed) and run in the background.
1. Initialization and Start:
- The retention manager is initialized with required parameters (rate limit, max iterators, and running interval).
- A retention service is started in the OzoneManager, running periodically based on a configured interval.

2. Periodic Execution:

   ![RetentionManager](https://issues.apache.org/jira/secure/attachment/13074943/RetentionManager.png)

- After starting, RetentionManager obtains all Lifecycles from DB and starts a thread for each Lifecycle
- Each thread traverses all keys of the corresponding Bucket to check whether they meet the Lifecycle rules
  - A Lifecycle can contain multiple Rules, and RetentionManager checks whether the rules match one by one.
  - First, filter out candidate keys based on Prefix and Tag, and then check the modification time of these keys to determine whether they are expired.
  - If the key is expired, execute actions.
  - The executed actions include deleting the key, converting the key, etc. The current implementation only supports deleting the key.
- After all threads are executed, RetentionManager ends this check and goes into sleep, waiting for the next execution time.

## Limitations
- The current solution lacks certain features:
  - Only expiration actions are supported.
  - Lack of CLI support for managing lifecycle configurations across all buckets (S3G is the only supported entry point).
  
All these kind of features can be added in the future.

## Permission
- Creating or deleting a Lifecycle on a Bucket requires write permission on the Bucket.
- Fetching a Bucket's Lifecycle requires read permission on the Bucket.
- Note that when a Lifecycle executes, it does not run under the identity of the user who created it. 
    Instead, it runs under an administrator account with the necessary permissions. Therefore, once a Lifecycle is created, it will not fail due to permission issues during execution.

## Conflicts in lifecycle configurations
- For expiration deletion policies, each Lifecycle Rule independently checks whether the deletion conditions are met. Once an object meets the deletion conditions of any rule, 
    it will be deleted according to that rule's configuration.
- In this example:
  - If an OBS bucket is configured to expire objects under the prefix `"a"` in 7 days and under the prefix `"a/b"` in 14 days,
  - The deletion process will proceed as follows:
    - On the 7th day, objects under prefix `"a"` will be deleted. Since `"a/b"` is a subdirectory of `"a"`, objects under `"a/b"` will also be deleted.
    - On the 14th day, since the objects under `"a/b"` have already been removed, this operation will effectively do nothing.
- As seen in this case, if the expiration action only involves deletion, conflicts are not complex. Simply executing each deletion rule in order of time is sufficient. 
    However, if future expiration actions involve other operations (such as transition), further considerations will be needed to handle conflicts between different actions.

## Bucket Layout
Lifecycle supports all types of Bucket Layouts, but different types of Buckets support different Lifecycle Rules.

- OBS/Legacy
  - Lifecycle Rules support both prefix and tag.
  - A rule can specify prefix and tag either separately or together. When both are specified, they are treated as "AND" conditions, consistent with AWS S3 rules. 
      For detailed specifications, refer to [intro-lifecycle-rules-filter](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rules-filter).

- FSO
  - Lifecycle Rules only support prefix.
  - Since FSO-type buckets have file system semantics, they can only delete empty directories. If a directory contains subdirectories or files, it cannot be deleted.
  - If tag support were added, conflicts might arise. For example, if a rule attempts to delete all objects with a specific tag, but a directory A with that tag contains some files without that tag, 
      those files will not be deleted, causing directory A to fail deletion. However, prefix-based filtering does not have this issue, so FSO buckets only support prefix.
  - In FSO buckets, the prefix must start with "/", where "/" represents the root directory in the file system.
  - If an FSO prefix ends with "/", it represents a directory. If the prefix does not end with "/", it represents a file with that prefix.

## Protobuf Definitions (Initial Version)
```protobuf
/**
S3 lifecycles (filter, expiration, rule and configuration).
 */
message LifecycleFilter {
  optional string prefix = 1;
  optional LifecycleFilterTag tag = 2;
  optional LifecycleRuleAndOperator andOperator = 3;
}

message LifecycleFilterTag {
  required string key = 1;
  required string value = 2;
}

message LifecycleRuleAndOperator {
  optional string prefix = 1;
  repeated LifecycleFilterTag tags = 2;
}

message LifecycleAction {
  optional LifecycleExpiration expiration = 1;
}

message LifecycleExpiration {
  optional uint32 days = 1;
  optional string date = 2;
}

message LifecycleRule {
  optional string id = 1;
  optional string prefix = 2;
  required bool enabled = 3;
  required LifecycleAction action = 4;
  optional LifecycleFilter filter = 5;
}

message LifecycleConfiguration {
  required string volume = 1;
  required string bucket = 2;
  required string owner = 3;
  optional uint64 creationTime = 4;
  repeated LifecycleRule rules = 5;
  optional uint64 objectID = 6;
  optional uint64 updateID = 7;
}

message CreateLifecycleConfigurationRequest {
  required LifecycleConfiguration lifecycleConfiguration = 1;
}

message CreateLifecycleConfigurationResponse {

}

message InfoLifecycleConfigurationRequest {
  required string volumeName = 1;
  required string bucketName = 2;
}

message InfoLifecycleConfigurationResponse {
  required LifecycleConfiguration lifecycleConfiguration = 1;
}

message DeleteLifecycleConfigurationRequest {
  required string volumeName = 1;
  required string bucketName = 2;
}

message DeleteLifecycleConfigurationResponse {

}

message ListLifecycleConfigurationsRequest {
  optional string userName = 1;
  optional string prevKey = 2;
  optional uint32 maxKeys = 3;
}

message ListLifecycleConfigurationsResponse {
  repeated LifecycleConfiguration lifecycleConfiguration = 1;
}
```

# Table format
## OmLifecycleConfiguration Table

The `OmLifecycleConfiguration` table in RocksDB is used to store lifecycle configurations of buckets. Below is a summary of the table structure.

### Table Structure

| Column Name  | Data Type             | Description                                              |
|--------------|-----------------------|----------------------------------------------------------|
| volume       | String                | The name of the volume.                                  |
| bucket       | String                | The name of the bucket.                                  |
| owner        | String                | The owner of the volume/bucket.                          |
| creationTime | long                  | The creation time of the configuration.                  |
| rules        | List<OmLCRule>        | A list of lifecycle rules associated with the configuration. |
| objectID     | long                  | Unique identifier for the object.                        |
| updateID     | long                  | Identifier for updates to the object.                    |


### Additional Information

- **Maximum Rules**: The table can store up to 1000 rules per lifecycle configuration according to https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rule-id
- **Validation**: The configuration is considered valid if:
  - The `volume`, `bucket`, and `owner` are not blank.
  - The number of rules is between 1 and 1000.
  - Each rule has a unique ID.
  - All rules are valid according to their individual validation criteria.
- **Conflict Resolution**: In case of overlapping lifecycle configurations the implementation follows AWS cost optimizing strategy to solve the conflicts https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html#lifecycle-config-conceptual-ex5

## Concurrency and Rate Limiting

1. **Thread Pool:** the thread pool is configurable to allow concurrent processing of lifecycle configurations.
2. **Rate Limiter:** the RateLimiter controls the rate of key deletions, ensuring system stability.


# Proposal

## 1. New Table for Lifecycle Configurations

- Introduce a new table
- Efficient query.
- Requires a new manager (lifecycle manager) and codec.
- No need to alter existing design.
- Update Bucket Deletion to delete linked lifecycle configurations when the bucket is deleted.

## 2. New Field in OmBucketInfo

- Utilize an existing table
- Less efficient query.
- No need for a new manager or codec.
- Update existing design to support lifecycle configurations in OmBucketInfo.
- Updates required for create, get, list, and delete operations in the BucketManager.

## Design Decisions
I made some decisions regarding the design, which require discussion before contribution:
- Lifecycle configurations are stored in their own table in the database, rather than as a field in OmBucketInfo.
  - Reasons for this decision:
    - Avoid modifying OmBucketInfo table.
    - Improve query efficiency for RetentionManager.
- If the alternative approach (storing lifecycle configurations in OmBucketInfo) is preferred, I will eliminate LifecycleConfigurationsManager & the new codec.

## Plan for Contribution
The implementation is substantial and should be split into several merge requests for better review:
1. Basic building blocks (lifecycle configuration, rule, expiration, etc.) and related table creation.
2. ClientProtocol & OzoneManager new operations for managing lifecycle configurations (including protobuf messages).
3. Updates to S3G endpoints.
4. Implementation of the RetentionManager.
5. Merge all changes into a new branch (e.g., 'X'), then merge that branch into master.

## Performance Impact

### List
RetentionManager runs periodically in OM and scans Buckets during the Lifecycle check process. 
Each Bucket is checked by a single thread, but multiple threads can run in parallel to process multiple Buckets.  
Therefore, the number of RetentionManager execution threads should be controlled to prevent excessive parallel List operations from impacting cluster performance.  
In the minimal case, RetentionManager can be configured to run with only one thread, meaning only one Bucket is being listed at any given time.

### Action
After RetentionManager finds expired Keys, it will delete them. 
If a large number of Keys expire at the same time, more deletion operations may be generated. 
Batch deletion can be used to reduce the number of deletion requests, thereby reducing the impact on cluster performance.

## Ozone Command
Currently, all Lifecycle operations are supported only through S3 commands.  
Future improvements may consider adding support for managing Lifecycle directly through Ozone's own commands.

## Metrics
Monitoring is mainly focused on RetentionManager’s execution status, including:
- Number of List requests issued by RetentionManager.
- Number of Keys scanned.
- Number of expired Keys detected.
- Number of delete requests sent.

Additional metrics could include the total number of Lifecycles in the cluster, as well as execution time and frequency for each Lifecycle.