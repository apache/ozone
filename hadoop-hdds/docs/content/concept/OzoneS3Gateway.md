---
title: "Ozone S3 Gateway"
date: "2025-03-28"
weight: 8
menu: 
  main:
     parent: Architecture
summary: Apache Ozone’s S3 Gateway (often called S3G) is a component that provides an Amazon S3-compatible REST interface for Ozone’s object store.
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

Apache Ozone’s S3 Gateway (often called **S3G**) is a component that provides an Amazon S3-compatible REST interface for Ozone’s object store. In essence, it allows any S3 client or tool to read and write data in Ozone as if it were talking to AWS S3. The S3 Gateway runs as a **separate, stateless service** on top of Ozone, translating HTTP S3 API calls into Ozone’s native storage operations.

This design lets users leverage the rich ecosystem of S3-compatible applications with Ozone, without modifying those applications.

## High-Level Overview of the S3 Gateway

Ozone’s S3 Gateway acts as a **REST front-end** that speaks the S3 protocol and bridges it to the Ozone backend. It is **stateless**, meaning it does not persist data or metadata locally between requests – all state is stored in the Ozone cluster itself.

Being stateless allows multiple S3 Gateway instances to run in parallel behind a load balancer for scaling and high availability. The idea of stateless also allows S3G to be deployed in Kubernetes quite easily. The gateway supports a broad set of S3 operations – create/delete buckets, PUT/GET objects, list objects, perform multipart uploads – using standard S3 SDKs or CLI tools.

Under the hood, each operation is translated into the appropriate Ozone calls:
- A PUT object request becomes a `putKey` write operation.
- A GET object becomes a `getKey` read operation.

### Mapping S3 to Ozone Concepts

In Ozone, data is organized into **volumes**, **buckets**, and **keys**. By default, the S3 Gateway uses a special
volume named `/s3v` to store all S3 buckets (this is configurable via the `ozone.s3g.volume.name` property).

Each S3 bucket created is represented as a bucket under the `/s3v` volume in Ozone. Objects (keys) are stored within those buckets. This mapping is transparent to the end user.

The default bucket layout for S3 buckets (`OBJECT_STORE`) can be configured using the `ozone.s3g.default.bucket.layout` property. This property defines the storage layout for buckets created via the S3 gateway.

## S3 Gateway in the Ozone Architecture

The S3 Gateway does the following:
- It receives S3 REST API calls from clients.
- It translates them and forwards metadata operations to OM.
- It streams data directly to and from DataNodes.

This stateless gateway can be scaled horizontally.

## Internal Components and Design

### HTTP Server and REST API Layer
Runs an embedded HTTP server exposing REST endpoints like:
- `PUT /<bucket>/<key>`
- `GET /<bucket>/<key>`

Handles authentication headers, content length, and routing.

### Request Handlers / Protocol Translation
Each S3 operation is mapped to Ozone APIs:
- Create Bucket → create in `/s3v` volume
- List Buckets → query all user-owned buckets under `/s3v`
- Put/Get Object → `putKey` / `getKey`

It also assembles proper S3-compliant response formats.

### Ozone Client Integration
Uses Ozone client libraries to:
- Call OM for metadata operations
- Stream data to/from DataNodes

Bulk data does not flow through OM – it goes directly between gateway and DataNodes.

### Statelessness and Caching
The gateway:
- Does not persist state
- Uses lightweight in-memory caches
- Depends on OM for coordination and user/token validation

This simplifies scaling and failure recovery.

### Authentication and Security
Supports AWS Signature-style authentication (if enabled):
- Only AWS Signature V4 is supported (the older AWS Signature V2 is not).
- Validates access/secret keys via OM
- Uses OM’s stored credentials
- Relies on OM for final authorization

Supports Kerberos authentication for secure clusters;  In unsecured mode, allows anonymous or dummy access.

## Summary

The Ozone S3 Gateway is a **protocol adapter**:
- It translates REST S3 operations into native Ozone calls.
- It doesn’t store or manage data – it delegates to OM and DataNodes.
- Its stateless, scalable design enables multiple instances behind load balancers.
- It enables seamless use of existing S3-compatible clients and tools.

For more details, refer to the other documentation:
- [Ozone S3 Gateway Interface]({{< ref "S3" >}})
- [S3 Multi-Tenancy]({{< ref "S3-Multi-Tenancy" >}})
- [Securing S3]({{< ref "SecuringS3" >}})
- [Network Ports]({{< ref "NetworkPorts" >}})
