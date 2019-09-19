---
title: Bucket Commands
summary: Bucket commands help you to manage the life cycle of a volume.
weight: 3
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

Ozone shell supports the following bucket commands.

  * [create](#create)
  * [delete](#delete)
  * [info](#info)
  * [list](#list)

### Create

The `bucket create` command allows users to create a bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -g, \-\-enforcegdpr            | Optional, if set to true it creates a GDPR compliant bucket, if not specified or set to false, it creates an ordinary bucket.
|  Uri                           | The name of the bucket in **/volume/bucket** format.


{{< highlight bash >}}
ozone sh bucket create /hive/jan
{{< /highlight >}}

The above command will create a bucket called _jan_ in the _hive_ volume.
Since no scheme was specified this command defaults to O3 (RPC) protocol.

### Delete

The `bucket delete` command allows users to delete a bucket. If the
bucket is not empty then this command will fail.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket

{{< highlight bash >}}
ozone sh bucket delete /hive/jan
{{< /highlight >}}

The above command will delete _jan_ bucket if it is empty.

### Info

The `bucket info` commands returns the information about the bucket.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | The name of the bucket.

{{< highlight bash >}}
ozone sh bucket info /hive/jan
{{< /highlight >}}

The above command will print out the information about _jan_ bucket.

### List

The `bucket list` command allows users to list the buckets in a volume.

***Params:***

| Arguments                      |  Comment                                |
|--------------------------------|-----------------------------------------|
| -l, \-\-length                   | Maximum number of results to return. Default: 100
| -p, \-\-prefix                   | Optional, Only buckets that match this prefix will be returned.
| -s, \-\-start                    | The listing will start from key after the start key.
|  Uri                           | The name of the _volume_.

{{< highlight bash >}}
ozone sh bucket list /hive
{{< /highlight >}}

This command will list all buckets on the volume _hive_.
