---
title: Command Shell
menu: main
---
<!---
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
Ozone Command Shell
===================

Ozone command shell gives a command shell interface to work against ozone.
Please note that this  document assumes that cluster is deployed
with simple authentication.

The Ozone commands take the following format.

* `ozone oz --command_ http://hostname:port/volume/bucket/key -user
<name> -root`

The *port* specified in command should match the port mentioned in the config
property `hdds.rest.http-address`. This property can be set in `ozone-site.xml`.
The default value for the port is `9880` and is used in below commands.

The *-root* option is a command line short cut that allows *ozone oz*
commands to be run as the user that started the cluster. This is useful to
indicate that you want the commands to be run as some admin user. The only
reason for this option is that it makes the life of a lazy developer more
easier.

Ozone Volume Commands
--------------------

The volume commands allow users to create, delete and list the volumes in the
ozone cluster.

### Create Volume

Volumes can be created only by Admins. Here is an example of creating a volume.

* `ozone oz -createVolume http://localhost:9880/hive -user bilbo -quota
100TB -root`

The above command creates a volume called `hive` owned by user `bilbo`. The
`-root` option allows the command to be executed as user `hdfs` which is an
admin in the cluster.

### Update Volume

Updates information like ownership and quota on an existing volume.

* `ozone oz  -updateVolume  http://localhost:9880/hive -quota 500TB -root`

The above command changes the volume quota of hive from 100TB to 500TB.

### Delete Volume
Deletes a Volume if it is empty.

* `ozone oz -deleteVolume http://localhost:9880/hive -root`


### Info Volume
Info volume command allows the owner or the administrator of the cluster to read meta-data about a specific volume.

* `ozone oz -infoVolume http://localhost:9880/hive -root`

### List Volumes

List volume command can be used by administrator to list volumes of any user. It can also be used by a user to list volumes owned by him.

* `ozone oz -listVolume http://localhost:9880/ -user bilbo -root`

The above command lists all volumes owned by user bilbo.

Ozone Bucket Commands
--------------------

Bucket commands follow a similar pattern as volume commands. However bucket commands are designed to be run by the owner of the volume.
Following examples assume that these commands are run by the owner of the volume or bucket.


### Create Bucket

Create bucket call allows the owner of a volume to create a bucket.

* `ozone oz -createBucket http://localhost:9880/hive/january`

This call creates a bucket called `january` in the volume called `hive`. If
the volume does not exist, then this call will fail.


### Update Bucket
Updates bucket meta-data, like ACLs.

* `ozone oz -updateBucket http://localhost:9880/hive/january  -addAcl
user:spark:rw`

### Delete Bucket
Deletes a bucket if it is empty.

* `ozone oz -deleteBucket http://localhost:9880/hive/january`

### Info Bucket
Returns information about a given bucket.

* `ozone oz -infoBucket http://localhost:9880/hive/january`

### List Buckets
List buckets on a given volume.

* `ozone oz -listBucket http://localhost:9880/hive`

Ozone Key Commands
------------------

Ozone key commands allows users to put, delete and get keys from ozone buckets.

### Put Key
Creates or overwrites a key in ozone store, -file points to the file you want
to upload.

* `ozone oz -putKey  http://localhost:9880/hive/january/processed.orc  -file
processed.orc`

### Get Key
Downloads a file from the ozone bucket.

* `ozone oz -getKey  http://localhost:9880/hive/january/processed.orc  -file
  processed.orc.copy`

### Delete Key
Deletes a key  from the ozone store.

* `ozone oz -deleteKey http://localhost:9880/hive/january/processed.orc`

### Info Key
Reads  key metadata from the ozone store.

* `ozone oz -infoKey http://localhost:9880/hive/january/processed.orc`

### List Keys
List all keys in an ozone bucket.

* `ozone oz -listKey  http://localhost:9880/hive/january`
