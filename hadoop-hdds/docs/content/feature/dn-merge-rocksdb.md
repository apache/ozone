---
title: "Merge Container RocksDB in DN"
weight: 2
menu:
   main:
      parent: Features
summary: Introduction to Ozone Datanode Container Schema V3
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

In Ozone, user data are separated into blocks and stored in HDDS Containers. Containers are the fundamental replication unit of Ozone/HDDS. Each Container has its metadata and data. Data are saved as files on disk. Metadata is saved in RocksDB.

Earlier, there was one RocksDB for each Container on datanode. With user data continously growing, there will be hundreds of thousands of RocksDB instances on one datanode. It's a big challenge to manage this amount of RocksDB instances in one JVM. 

Unlike the previous approach, this "Merge Container RocksDB in DN" feature will use only one RocksDB for each data volume, holding all metadata of Containers in this RocksDB. 
  
## Configuration

This is mainly a DN feature, which doesn't require much configuration. By default, it is enabled.

Here is a configuration which disables this feature if the "one RocksDB for each container" mode is more preferred. Please be noted that once the feature is enabled, it's strongly suggested not to disable it in later. 
  
```XML
<property>
   <name>hdds.datanode.container.schema.v3.enabled</name>
   <value>false</value>
   <description>Disable or enable this feature.</description>
</property>
```
 
Without any specific configuration, the single RocksDB will be created under the data volume configured in "hdds.datanode.dir". 

For some advanced cluster admins who have the high performance requirement, he/she can leverage quick storages to save RocksDB. In this case, configure these two properties.  

```XML
<property>
   <name>hdds.datanode.container.db.dir</name>
   <value/>
   <description>This setting is optional. Specify where the per-disk rocksdb instances will be stored.</description>
</property>
<property>
   <name>hdds.datanode.failed.db.volumes.tolerated</name>
   <value>-1</value>
   <description>The number of db volumes that are allowed to fail before a datanode stops offering service.
   Default -1 means unlimited, but we should have at least one good volume left.</description>
</property>
```

### Backward compatibility 

Existing containers each has one RocksDB for them will be still accessible after this feature is enabled. All container data will co-exist in an existing Ozone cluster.

## References

 * [Design doc]({{< ref "design/dn-merge-rocksdb.md">}})