---
title: "Quota in Ozone"
date: "2020-10-22"
weight: 4
summary: Quota in Ozone
icon: user
menu:
   main:
      parent: Features
summary: Introduction to Ozone Quota
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

So far, we know that Ozone allows users to create volumes, buckets, and keys. A Volume usually contains several buckets, and each Bucket also contains a certain number of keys. Obviously, it should allow the user to define quotas (for example, how many buckets can be created under a Volume or how much space can be used by a Bucket), which is a common requirement for storage systems.

## Currently supported
1. Storage Space level quota

Administrators should be able to define how much storage space a Volume or Bucket can use. The following Settings for Storage space quota are currently supported：

a. By default, the quota for volume and bucket is not enabled.

b. When volume quota is enabled, the total quota of the buckets, cannot exceed the volume quota.

c. Bucket quota can be set separately without enabling Volume quota. The size of bucket quota is unrestricted at this point.

d. Volume quota is not currently supported separately, and volume quota takes effect only if bucket quota is set. Because ozone only check the usedBytes of the bucket when we write the key.

e. If the cluster is upgraded from old version less than 1.1.0, use of quota on older volumes and buckets(We can confirm by looking at the info for the volume or bucket, and if the quota value is -2 the volume or bucket is old) is not recommended. Since the old key is not counted to the bucket's usedBytes, the quota setting is inaccurate at this point.

f. If volume's quota is enabled then bucket's quota cannot be cleared. 

g. Volume having linked bucket do not consume space quota for keys within linked bucket. Linked bucket keys will consume space quota of source volume and source bucket.

2. Namespace quota

Administrators should be able to define how many namespace a Volume or Bucket can use. The following settings for namespace quota are supported: 

a. By default, the namespace quota for volume and bucket is not enabled (thus unlimited quota).

b. When volume namespace quota is enabled, the total number of buckets under the volume, cannot exceed the volume namespace quota.

c. When bucket namespace quota is enabled, the total number of keys under the bucket, cannot exceed the bucket namespace quota.

d. Linked buckets do not define separate namespace quota, it is referred by namespace quota of source bucket for keys inside linked bucket.

e. Linked bucket will consume namespace quota of volume.

f. If the cluster is upgraded from old version less than 1.1.0, use of quota on older volumes and buckets(We can confirm by looking at the info for the volume or bucket, and if the quota value is -2 then volume or bucket is old) is not recommended. Since the old key is not counted to the bucket's namespace quota, the quota setting is inaccurate at this point.

g. For FSO bucket, while files and directory moving to trash, trash will consume extra namespace for below cases:
- for internal directory path of trash in the bucket, i.e. /.trash/<user>/<current or timestamp>
- for extra path created while moving file / directory to trash present at some hierarchy.
  eg: 

```
- source: /<vol>/<bucket>/dir1/dir2/file.txt
Scenario 1:
- move file.txt to trash (while delete operation)
- trash created with "dir1 and dir2" as extra namespace to have same path as source in trash:
  /<vol>/<bucket>/.trash/<user>/current/dir1/dir2/file.txt
  So this will consume extra name space of "2"
  
Scenaro 2:
- move dir2 to trash (while delete operation)
- trash created with "dir1" as extra namespace
  /<vol>/<bucket>/.trash/<user>/current/dir1/dir2/file.txt
  So this will consume extra namespace of "1" for dir1
  
Scenario 3:
- move dir1 to trash (while delete operation), in this case, no extra namespace is required
  /<vol>/<bucket>/.trash/<user>/current/dir1/dir2/file.txt

```

### Note:
- For FSO bucket with recursive deletion of directory, release of quota happens asynchronously after sub directories and files are removed (when directory is removed, recursive deletion can be in-progress in background).
- When quota is about to reach the limit, and ozone clients (in parallel) commit the files, then file commit will be success for those files meeting the quota and verification will be in order of first come basis at backend.

## Client usage
### Storage Space level quota
Storage space level quotas allow the use of units B, KB, MB, GB and TB. Represents how much storage Spaces will be used.

#### Note:

- Decimals are not supported while setting quota for volume and bucket. For example, 1.5 TB.

- Ensure that the minimum storage quota is default block size * replication factor. If you set the value lesser than the default block size * replication factor, while writing the data (key put) operation, an operation error is displayed.

#### Volume Storage Space level quota
```shell
bin/ozone sh volume create --space-quota 5MB /volume1
```
This means setting the storage space of Volume1 to 5MB

```shell
bin/ozone sh volume setquota --space-quota 10GB /volume1
```
This behavior changes the quota of Volume1 to 10GB.

#### Bucket Storage Space level quota
```shell
bin/ozone sh bucket create --space-quota 5MB /volume1/bucket1
```
That means bucket1 allows us to use 5MB of storage.

```shell
bin/ozone sh bucket setquota  --space-quota 10GB /volume1/bucket1 
```
This behavior changes the quota for Bucket1 to 10GB

Total bucket quota should not be greater than its Volume quota. If we have a 10MB Volume, The sum of the sizes of all buckets under this volume cannot exceed 10MB, otherwise the bucket set quota fails.

#### Clear the quota for volume and bucket
```shell
bin/ozone sh volume clrquota --space-quota /volume1
bin/ozone sh bucket clrquota --space-quota /volume1/bucket1
```

#### Check quota and usedBytes for volume and bucket
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
We can get the quota value and usedBytes in the info of volume and bucket.

### Namespace quota
Namespace quota is a number that represents how many unique names can be used. This number cannot be greater than LONG.MAX_VALUE in Java.

#### Volume Namespace quota
```shell
bin/ozone sh volume create --namespace-quota 100 /volume1
```
This means setting the namespace quota of Volume1 to 100.

```shell
bin/ozone sh volume setquota --namespace-quota 1000 /volume1
```
This behavior changes the namespace quota of Volume1 to 1000.

#### Bucket Namespace quota
```shell
bin/ozone sh bucket create --namespace-quota 100 /volume1/bucket1
```
That means bucket1 allows us to use 100 of namespace.

```shell
bin/ozone sh bucket setquota --namespace-quota 1000 /volume1/bucket1 
```
This behavior changes the quota for Bucket1 to 1000.

#### Clear the quota for volume and bucket
```shell
bin/ozone sh volume clrquota --namespace-quota /volume1
bin/ozone sh bucket clrquota --namespace-quota /volume1/bucket1
```

#### Check quota and usedNamespace for volume and bucket
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
We can get the quota value and usedNamespace in the info of volume and bucket.