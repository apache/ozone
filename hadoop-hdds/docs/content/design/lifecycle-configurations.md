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

## Overview

### Functionality
- User should be able to create/remove/fetch lifecycle configurations for a specific S3 bucket.
- The lifecycle configurations will be executed periodically.
- Depending on the rules of the lifecycle configuration there could be different actions or even multiple actions. 
- At the moment only expiration is supported (keys get deleted).
- The lifecycle configurations supports all buckets not only S3 buckets.


### Components

- Lifecycle configurations (will be stored in DB) consists of volumeName, bucketName and a list of rules
    - A rule contains prefix (string), Expiration and an optional Filter.
    - Object tagging integrations for bucket lifecycle configuration.
    - Expiration contains either days (integer) or Date (long)
    - Filter contains prefix (string).
- S3G bucket endpoint needs few updates to accept ?/lifecycle 
- ClientProtocol and all implementers provides (get, list, delete and create) lifecycle configuration
- RetentionManager:
   - Upon startup, the OzoneManager initializes the Retention Manager based on configuration parameters such as retention interval.
   - A background retention service is responsible for scheduling and executing tasks at specified intervals.
   - The Retention Manager retrieves lifecycle configurations associated with buckets.
   - Then assigns each lifecycle configuration (attached to a bucket) to a threadpool (Configurable) for further processing. 
   - Each task will iterate through keys of a specific bucket and issue deletion request for eligible keys.
     


### Flow
1. Users interact with lifecycle configurations via S3Gateway.
2. Configuration details are processed by a handler.
3. Configurations are saved/fetched from the database.
4. RetentionManager, running periodically in the Leader OM, executes lifecycle configurations and issues deletions for eligible keys.

## Limitations
- The current solution lacks certain features:
  - Only expiration actions are supported.
  - Lack of CLI support for managing lifecycle configurations across all buckets (S3G is the only supported entry point).
  
All these kind of features can be added in the future.

## Protobuf Definitions
```protobuf
/**
S3 lifecycles (filter, expiration, rule and configuration).
 */
message LifecycleFilter {
  optional string prefix = 1;
}

message LifecycleExpiration {
  optional uint32 days = 1;
  optional string date = 2;
}

message LifecycleRule {
  optional string id = 1;
  optional string prefix = 2;
  required bool enabled = 3;
  optional LifecycleExpiration expiration = 4;
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

- **Maximum Rules**: The table can store up to 1000 rules per lifecycle configuration.
- **Validation**: The configuration is considered valid if:
  - The `volume`, `bucket`, and `owner` are not blank.
  - The number of rules is between 1 and 1000.
  - Each rule has a unique ID.
  - All rules are valid according to their individual validation criteria.

# Retention Manager
## High-Level Flow

1. **Initialization and Start:**
   - The retention manager is initialized with required parameters (rate limit, max iterators, and running interval).
   - A retention service is started in the OzoneManager, running periodically based on a configured interval.

2. **Periodic Execution:**
   - Each time the service runs, it checks if the current node is the leader and sleeps if it is not the leader.
   - If it is the leader, it proceeds with the following operations:
     * Retrieve the lifecycle configurations list.
     * Each lifecycle configuration represents a bucket and contains a list of lifecycle rules to be applied.
     * Lifecycle configurations are handled simultaneously by a configurable threadpool executor.
     * The operation involves scanning the bucket's entries, and if they are eligible, performing an action (currently, deletion).

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

   
# Files Affected

## Implemented Proposal: New Table for Lifecycle Configurations

### hdds-common

- OzoneConfigKeys.java
- OzoneConsts.java

### ozone-client

- ClientProtocol.java
- RpcClient.java
- OzoneLifecycleConfiguration.java

### ozone-common

- OmLCExpiration.java
- OmLCFilter.java
- OmLCRule.java
- OmLifecycleConfiguration.java
- OzoneManagerProtocol.java
- OzoneManagerProtocolClientSideTranslatorPB.java
- OMConfigKeys.java
- OmUtils.java
- TestOmLifeCycleConfiguration.java

### ozone-integration-test

- TestOzoneRpcClientAbstract.java
- TestSecureOzoneRpcClient.java
- TestRetentionManager.java
- TestDataUtil.java

### ozone-interface-client

- OmClientProtocol.proto

### ozone-interface-storage

- OmLifecycleConfigurationCodec.java
- OMMetadataManager.java
- OMDBDefinition.java
- OzoneManagerRatisUtils.java
- OMBucketDeleteRequest.java
- OMLifecycleConfigurationCreateRequest.java
- OMLifecycleConfigurationDeleteRequest.java
- OMBucketDeleteResponse.java
- OMLifecycleConfigurationCreateResponse.java
- OMLifecycleConfigurationDeleteResponse.java
- LCOpAction.java
- LCOpCurrentExpiration.java
- LCOpRule.java
- RetentionManager.java
- RetentionManagerImpl.java
- LifecycleConfigurationManager.java
- LifecycleConfigurationManagerImpl.java
- OmMetadataManagerImpl.java
- OzoneManager.java
- OzoneManagerRequestHandler.java
- TestOMLifecycleConfigurationCreateRequest.java
- TestOMLifecycleConfigurationDeleteRequest.java
- TestOMLifecycleConfigurationRequest.java
- TestOMLifecycleConfigurationCreateResponse.java
- TestOMLifecycleConfigurationDeleteResponse.java
- RetentionTestUtils.java
- TestLCOpCurrentExpiration.java
- TestLCOpRule.java
- TestLifecycleConfigurationManagerImpl.java

### ozone-s3gateway

- BucketEndpoint.java
- EndpointBase.java
- LifecycleConfiguration.java
- PutBucketLifecycleConfigurationUnmarshaller.java
- S3ErrorTable.java
- S3GatewayConfigKeys.java
- TestLifecycleConfigurationDelete.java
- TestLifecycleConfigurationGet.java
- TestLifecycleConfigurationPut.java


