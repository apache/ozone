---
title: "Quota in Ozone"
date: "2020-October-22"
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

Administrators should be able to define how much storage space a Volume or Bucket can use.

## Client usage
### Storage Space level quota
Storage space level quotas allow the use of units such as KB (k), MB (m), GB (g), TB (t), PB (p), etc. Represents how much storage Spaces will be used.

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

A bucket quota should not be greater than its Volume quota. Let's look at an example. If we have a 10MB Volume and create five buckets under that Volume with a quota of 5MB, the total quota is 25MB. In this case, the bucket creation will always succeed, and we check the quota for bucket and volume when the data is actually written. Each write needs to check whether the current bucket is exceeding the limit and the current total volume usage is exceeding the limit.

#### Clear the quota for Volume1. The Bucket cleanup command is similar.
```shell
bin/ozone sh volume clrquota --space-quota /volume1
```

#### Check quota and usedBytes for volume and bucket
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
We can get the quota value and usedBytes in the info of volume and bucket.