---
title: "Streaming Write Pipeline"
weight: 1
menu:
   main:
      parent: Features
summary: A new write pipeline using Ratis Streaming.
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

This document discusses the new Streaming Write Pipeline feature in Ozone.
It is implemented with the Ratis Streaming API.
Note that the existing Ozone Write Pipeline is implemented with the Ratis Async API.
We refer the new Streaming Write Pipeline as Write Pipeline V2
and the existing Async Write Pipeline as Write Pipeline V1.

The Streaming Write Pipeline V2 increases the performance
by providing better network topology awareness
and removing the performance bottlenecks in V1.
The V2 implementation also avoids unnecessary buffer copying
(by Netty zero copy)
and has a better utilization of the CPUs and the disks in each datanode.

## Configuration Properties

Set the following properties to the Ozone configuration file `ozone-site.xml`.

- To enable the Streaming Write Pipeline feature, set the following property to true.
```XML
  <property>
    <name>hdds.container.ratis.datastream.enabled</name>
    <value>false</value>
    <tag>OZONE, CONTAINER, RATIS, DATASTREAM</tag>
    <description>It specifies whether to enable data stream of container.</description>
  </property>
```
- Datanodes listen to the following port for the streaming traffic.
```XML
  <property>
    <name>hdds.container.ratis.datastream.port</name>
    <value>9855</value>
    <tag>OZONE, CONTAINER, RATIS, DATASTREAM</tag>
    <description>The datastream port number of container.</description>
  </property>
```
- To use Streaming in FileSystem API, set the following property to true.
```XML
  <property>
    <name>ozone.fs.datastream.enabled</name>
    <value>false</value>
    <tag>OZONE, DATANODE</tag>
    <description>
      To enable/disable filesystem write via ratis streaming.
    </description>
  </property>
```

## Client APIs

### OzoneDataStreamOutput

The new `OzoneDataStreamOutput` class is very similar to the existing `OzoneOutputStream` class,
except that `OzoneDataStreamOutput` uses `ByteBuffer` as a parameter in the `write` methods
while `OzoneOutputStream` uses `byte[]`.
The reason of using a `ByteBuffer`, instead of a `byte[]`,
is to support zero buffer copying.
A typical `write` method is shown below:

- OzoneDataStreamOutput
```java
  public void write(ByteBuffer b, int off, int len) throws IOException;
```

- OzoneOutputStream
```java
  public void write(byte[] b, int off, int len) throws IOException;
```
### OzoneBucket

The following new methods are added to `OzoneBucket`
for creating keys using the Streaming Write Pipeline.

- createStreamKey
```java
  public OzoneDataStreamOutput createStreamKey(String key, long size)
      throws IOException;
```

```java
  public OzoneDataStreamOutput createStreamKey(String key, long size,
      ReplicationConfig replicationConfig, Map<String, String> keyMetadata)
      throws IOException;
```
- createMultipartStreamKey
```java
  public OzoneDataStreamOutput createMultipartStreamKey(String key, long size,
      int partNumber, String uploadID) throws IOException;
```

Note that the methods above have the same parameter list
as the existing `createKey` and `createMultipartKey` methods.

Below is an example to create a key from a local file using a memory-mapped buffer.
```java
  // Create a memory-mapped buffer from a local file:
  final FileChannel channel = ...  // local file channel
  final long length = ...          // length of the data
  final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);

  // Create an OzoneDataStreamOutput
  final OzoneBucket bucket = ...   // an Ozone bucket
  final String key = ...           // the key name
  final OzoneDataStreamOutput out = bucket.createStreamKey(key, length);

  // Write the memory-mapped buffer to the key output
  out.write(mapped);
  
  // close
  out.close();      // In practice, use try-with-resource to close it.
  channel.close();  // In practice, use try-with-resource to close it.
```