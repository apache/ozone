---
title: "Short Circuit Local Read in Datanode"
weight: 2
menu:
   main:
      parent: Features
summary: Introduction to Ozone Datanode Short Circuit Local Read Feature
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

By default, client reads data over GRPC from the Datanode. When the client asks the Datanode to read a file, the Datanode reads that file off of the disk and sends the data to the client over a GRPC connection.

This short-circuit local read feature will bypass the Datanode, allowing the client to read the file from local disk directly when the client is co-located with the data on the same server.

Short-circuit local read can provide a substantial performance boost to many applications by removing the overhead of network communication. 
  
## Prerequisite

Short-circuit local reads make use of a UNIX domain socket. This is a special path in the filesystem that allows the client and the DataNodes to communicate.

The Hadoop native library `libhadoop.so` provides support to for Unix domain sockets. Please refer to Hadoop's [Native Libraries Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/NativeLibraries.html) for details.

The Hadoop version used in Ozone is defined by `hadoop.version` in pom.xml. Before enabling short-circuit local reads, find the `libhadoop.so` from the release package of the corresponding Hadoop version, put it under one of the directories specified by Java `java.library.path` property. The default value of `java.library.path` depends on the OS and Java version. For example, on Linux with OpenJDK 8 it is `/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib`.

The `ozone checknative` command can be used to detect whether `libhadoop.so` can be found and loaded successfully by Ozone service.


## Configuration

Short-circuit local reads need to be configured on both the Datanode and the client. By default, it is disabled.

```XML
<property>
   <name>ozone.client.read.short-circuit</name>
   <value>false</value>
   <description>Disable or enable the short-circuit local read feature.</description>
</property>
```

It makes use of a UNIX domain socket, a special path in the filesystem. You will need to set a path to this socket. 

```XML
<property>
   <name>ozone.domain.socket.path</name>
   <value></value>
   <description> This is a path to a UNIX domain socket that will be used for 
      communication between the Datanode and local Ozone clients. 
      If the string "_PORT" is present in this path, it will be replaced by the TCP port of the Datanode.
   </description>
</property>
```

The Datanode needs to be able to create this path. On the other hand, it should not be possible for any user except the user who launches Ozone service or root to create this path. For this reason, paths under `/var/run` or `/var/lib` are often used.

If you configure the `ozone.domain.socket.path` to some value, for example `/dir1/dir2/ozone_dn_socket`, please make sure that both `dir1` and `dir2` are existing directories, but the file `ozone_dn_socket` does not exist under `dir2`. `ozone_dn_socket` will be created by Ozone Datanode later during its startup.

### Example Configuration
To enable short-circuit read, here is an example configuration.

```XML
<property>
   <name>ozone.client.read.short-circuit</name>
   <value>false</value>
</property>
<property>
   <name>ozone.domain.socket.path</name>
   <value>/var/run/ozone_dn_socket</value>
</property>
```

### Security Consideration

To ensure data security and integrity, Ozone will follow the same rules as Hadoop to check permission on the `ozone.domain.socket.path` path as documented in [Socket Path Security](https://wiki.apache.org/hadoop/SocketPathSecurity). It will fail the `ozone.domain.socket.path` verification and disable the feature if the filesystem permissions of the specified path are inadequate. The verification failure message carries detail instruction about how to fix the problem. Following is an example, 

`The path component: '/etc/hadoop' in '/etc/hadoop/ozone_dn_socket' has permissions 0777 uid 0 and gid 0. It is not protected because it is world-writable. This might help: 'chmod o-w /etc/hadoop'. For more information: https://wiki.apache.org/hadoop/SocketPathSecurity`