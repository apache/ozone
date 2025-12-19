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

FairCallQueue
===

This document contains information for setting up the `FairCallQueue` feature with Ozone. 
In order for `FairCallQueue` to be enabled and used, 
Hadoop RPC must be used as transport protocol for OM - S3G communication. 
There is no implementation for gRPC yet.

There is a custom `IdentityProvider` implementation for Ozone that must be specified in the configuration, otherwise
there is no S3G impersonation which makes the `FairCallQueue` ineffective since it's only reading one user, 
the S3G special user instead of the S3G client user.

## Configuration

There must be a port specified to which the OM will forward any activity 
and the `FailCallQueue` and `DecayRpcScheduler` will listen to. 
Furthermore, this port will have to be part of every configuration name.

Port used for below examples : 9862

```XML
<property>
   <name>ozone.om.address</name>
   <value>OMDomain:9862</value>
</property>

<property>
    <name>ozone.om.s3.grpc.server_enabled</name>
    <value>false</value>
</property>
<property>
    <name>ozone.om.transport.class</name>
    <value>org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory</value>
</property>

<property>
   <name>ipc.9862.callqueue.impl</name>
   <value>org.apache.hadoop.ipc_.FairCallQueue</value>
</property>
<property>
   <name>ipc.9862.scheduler.impl</name>
   <value>org.apache.hadoop.ipc_.DecayRpcScheduler</value>
</property>
<property>
   <name>ipc.9862.identity-provider.impl</name>
   <value>org.apache.hadoop.ozone.om.helpers.OzoneIdentityProvider</value>
</property>
<property>
   <name>ipc.9862.scheduler.priority.levels</name>
   <value>2</value>
</property>
<property>
   <name>ipc.9862.backoff.enable</name>
   <value>true</value>
</property>
<property>
   <name>ipc.9862.faircallqueue.multiplexer.weights</name>
   <value>2,1</value>
</property>
<property>
    <name>ipc.9862.decay-scheduler.thresholds</name>
    <value>50</value>
</property>
```
