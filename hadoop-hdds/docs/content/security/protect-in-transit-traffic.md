---
title: Protect In-Transit Traffic
name: Protect In-Transit Traffic
parent: security
menu: main
weight: 6
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

# Protecting In-Transit Traffic

This document describes how to protect data in transit within Apache Ozone.

## Hadoop RPC Encryption

To encrypt client-OM (Ozone Manager) communication, configure `hadoop.rpc.protection` to `privacy` in your `core-site.xml`. This ensures that all data exchanged over Hadoop RPC is encrypted.

```xml
<property>
  <name>hadoop.rpc.protection</name>
  <value>privacy</value>
</property>
```

## gRPC TLS Encryption

To enable TLS for gRPC traffic, set `hdds.grpc.tls.enabled` to `true`. This encrypts communication between Ozone services that use gRPC.

```xml
<property>
  <name>hdds.grpc.tls.enabled</name>
  <value>true</value>
</property>
```

## Ozone HTTP Web Console

For information on securing the Ozone HTTP web console, please refer to the [Securing HTTP](https://ozone.apache.org/docs/latest/security/securing-http.html) documentation.
