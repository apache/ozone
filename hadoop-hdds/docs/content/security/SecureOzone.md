---
title: "Securing Ozone"
date: "2019-04-03"
summary: Overview of Ozone security concepts and steps to secure Ozone Manager and SCM.
weight: 1
menu:
   main:
      parent: Security
icon: tower
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

Apache Ozone relies on Kerberos for robust security. Enabling Kerberos is the standard and recommended practice for all production environments.

## Enabling Security

To activate security, set the following in `ozone-site.xml`:

| Property                         | Value    | Description                                           |
| :------------------------------- | :------- | :----------------------------------------------------- |
| `ozone.security.enabled`         | `true`   | Enables Ozone-specific security features.              |
| `hadoop.security.authentication` | `kerberos` | Specifies Kerberos as the authentication mechanism.    |

Both properties must be correctly configured for a secure cluster.

## Network Communication Security Overview

Securing Ozone involves protecting inter-service (e.g. OM to SCM) RPC, client-to-service RPC, DataNode gRPC, and HTTP/HTTPS interfaces. Mechanisms include Kerberos, TLS/SSL, and SPNEGO. For a comprehensive list of default ports, refer to the [Network Ports documentation]({{< ref "concept/NetworkPorts.md" >}}).

---

## Authentication Mechanisms

### Kerberos

With security enabled, all Ozone daemons (OM, SCM, DataNodes, S3G, Recon, HttpFS) and users require Kerberos credentials, which means each client must always first obtain a Kerberos ticket.

### Tokens

Ozone uses a notion of tokens to avoid overburdening the Kerberos server. When you serve thousands of requests per second, involving Kerberos for every request is not scalable. Hence, once authentication is done, Ozone issues delegation tokens and block tokens to the clients. These tokens allow applications to perform specified operations against the cluster as if they have Kerberos tickets with them. Ozone supports the following kinds of tokens:

**Delegation Token**

Delegation tokens allow an application to impersonate a user's Kerberos credentials. This token is based on verification of Kerberos identity and is issued by the Ozone Manager. Delegation tokens are enabled by default when security is enabled.

**Block Token**

Block tokens allow a client to read or write a block. This is needed so that DataNodes know that the user/client has permission to read or make modifications to the block.

**S3AuthInfo**

S3 uses a very different shared secret security scheme. Ozone supports the AWS Signature Version 4 protocol, and from the end user's perspective Ozone’s S3 feels exactly like AWS S3.

The S3 credential tokens are called S3 auth info in the code. These tokens are also enabled by default when security is enabled.

For more details on delegation tokens, see the [Introducing Hadoop Tokens](https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/sections/hadoop_tokens.html).

Each of the service daemons that make up Ozone needs a Kerberos service principal name and a corresponding Kerberos keytab file. All these settings should be made in `ozone-site.xml`.

---

## Component-Specific Security Configurations

All Kerberos service principals and keytab file paths are configured in `ozone-site.xml`.

### Ozone Manager (OM)

#### Kerberos-based Authentication

**Service Identity**

| Property                          | Example Value           | Description                             |
| :------------------------------- | :---------------------- | :-------------------------------------- |
| `ozone.om.kerberos.principal`     | `om/_HOST@REALM.COM`    | OM service principal.                   |
| `ozone.om.kerberos.keytab.file`   | `/path/to/om.keytab`    | Keytab for OM service principal.        |

**HTTP SPNEGO Authentication**

| Property                                | Example Value            | Description                              |
| :-------------------------------------- | :----------------------- | :--------------------------------------- |
| `ozone.om.http.auth.kerberos.principal` | `HTTP/_HOST@REALM.COM`   | OM HTTP interface principal (SPNEGO).    |
| `ozone.om.http.auth.kerberos.keytab`    | `/path/to/http.keytab`   | Keytab for OM HTTP principal.            |

#### Hadoop RPC Encryption

OM RPC communication can be encrypted using `hadoop.rpc.protection`.

| Property                | Value                                | Description                                                                                                                               |
| :---------------------- | :----------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------- |
| `hadoop.rpc.protection` | `authentication`/`integrity`/`privacy` | Controls RPC Quality of Protection. `privacy` enables full encryption. |

Ensure compatibility with other Hadoop services if `privacy` is enabled.

#### Ratis Security for OM High Availability (HA)

OM HA uses Apache Ratis for state replication via gRPC. This channel should be secured with TLS. Refer to [Apache Ratis Security documentation](https://github.com/apache/ratis/blob/master/ratis-docs/src/site/markdown/security.md) for Ratis-specific TLS configurations.

### Storage Container Manager (SCM)

#### Kerberos-based Authentication

**Service Identity**

| Property                          | Example Value           | Description                             |
| :------------------------------- | :---------------------- | :-------------------------------------- |
| `hdds.scm.kerberos.principal`     | `scm/_HOST@REALM.COM`   | SCM service principal.                  |
| `hdds.scm.kerberos.keytab.file`   | `/path/to/scm.keytab`   | Keytab for SCM service principal.       |

**HTTP SPNEGO Authentication**

| Property                                | Example Value            | Description                              |
| :-------------------------------------- | :----------------------- | :--------------------------------------- |
| `hdds.scm.http.auth.kerberos.principal` | `HTTP/_HOST@REALM.COM`   | SCM HTTP interface principal (SPNEGO).   |
| `hdds.scm.http.auth.kerberos.keytab`    | `/path/to/http.keytab`   | Keytab for SCM HTTP principal.           |

#### Ratis Security for SCM High Availability (HA)

SCM HA (`ozone.scm.ratis.enable=true`) uses Ratis for state replication via gRPC, secured by TLS. The primordial SCM acts as a root CA, issuing certificates to other SCMs, which then issue certificates to OMs and DataNodes. Consult [Apache Ratis source code and documentation](https://github.com/apache/ratis) for Ratis TLS configurations.

### DataNodes

DataNodes store data blocks. For comprehensive details, see [Securing Datanodes documentation]({{< ref "security/securingdatanodes.md" >}}).

#### Kerberos-based Authentication

**Service Identity**

| Property                             | Example Value              | Description                             |
| :----------------------------------- | :------------------------- | :-------------------------------------- |
| `hdds.datanode.kerberos.principal`   | `dn/_HOST@REALM.COM`       | DataNode service principal.             |
| `hdds.datanode.kerberos.keytab.file` | `/path/to/datanode.keytab` | Keytab for DataNode service principal.  |

**HTTP SPNEGO Authentication**

| Property                                   | Example Value            | Description                              |
| :----------------------------------------- | :----------------------- | :--------------------------------------- |
| `hdds.datanode.http.auth.kerberos.principal`| `HTTP/_HOST@REALM.COM`   | DataNode HTTP interface principal.       |
| `hdds.datanode.http.auth.kerberos.keytab`  | `/path/to/http.keytab`   | Keytab for DataNode HTTP principal.      |

#### Certificate-based Security and gRPC TLS/mTLS Encryption

Modern Ozone DataNodes use SCM-issued certificates. DataNode gRPC communication (including Ratis replication) is secured using TLS/mTLS enabled by these certificates.

| Property                | Value        | Description                                                                                                |
| :---------------------- | :----------- | :--------------------------------------------------------------------------------------------------------- |
| `hdds.grpc.tls.enabled` | `true`/`false` | Enables TLS for DataNode gRPC channels (requires `ozone.security.enabled=true`).                             |

Ratis replication between DataNodes uses gRPC TLS for data-in-transit protection, which is also controlled by `hdds.grpc.tls.enabled`. Refer to Apache Ratis documentation for specific gRPC TLS settings.
Note: this property is also used by OM and SCM gRPC and Ratis transport.

### S3 Gateway (S3G)

Provides an S3-compatible REST interface.

#### Kerberos Configuration

**Service Identity**

| Property                         | Example Value           | Description                              |
| :------------------------------ | :---------------------- | :--------------------------------------- |
| `ozone.s3g.kerberos.principal`   | `s3g/_HOST@REALM.COM`   | S3G service principal for Kerberos auth. |
| `ozone.s3g.kerberos.keytab.file` | `/path/to/s3g.keytab`   | Keytab for S3G service principal.        |

**HTTP SPNEGO Authentication**

| Property                                 | Example Value            | Description                               |
| :--------------------------------------- | :----------------------- | :---------------------------------------- |
| `ozone.s3g.http.auth.kerberos.principal` | `HTTP/_HOST@REALM.COM`   | S3G HTTP server principal (SPNEGO).       |
| `ozone.s3g.http.auth.kerberos.keytab`    | `/path/to/http.keytab`   | Keytab for S3G HTTP principal.            |

S3G uses Kerberos for its service identity and S3 credentials (Access Key ID/Secret Key) for client S3 operations via AWS Signature Version 4.

### HttpFS Gateway

Offers an HDFS-compatible REST API (`webhdfs`).

#### Introduction to HttpFS Security

HttpFS supports Hadoop pseudo authentication (`simple`) and Kerberos SPNEGO.

#### Kerberos-based Authentication

**Service Identity**

| Property                                          | Default Value                                      | Description                                 |
| :------------------------------------------------ | :------------------------------------------------- | :------------------------------------------ |
| `httpfs.hadoop.authentication.kerberos.principal` | `${user.name}/${httpfs.hostname}@${kerberos.realm}` | HttpFS principal for OM connection.         |
| `httpfs.hadoop.authentication.kerberos.keytab`    | `${user.home}/httpfs.keytab`                       | Keytab for OM connection principal.         |

**HTTP SPNEGO Authentication**

| Property                                   | Default Value                                 | Description                                 |
| :----------------------------------------- | :-------------------------------------------- | :------------------------------------------ |
| `httpfs.authentication.kerberos.principal` | `HTTP/${httpfs.hostname}@${kerberos.realm}`   | HttpFS client-facing HTTP principal.        |
| `httpfs.authentication.kerberos.keytab`    | `${user.home}/httpfs.keytab`                  | Keytab for client-facing principal.         |
| `httpfs.authentication.type`               | `simple`                                      | Client HTTP authentication. Set to `kerberos` for SPNEGO. |
| `httpfs.hadoop.authentication.type`        | `simple`                                      | HttpFS to Ozone Manager authentication. Set to `kerberos`.|

HttpFS requires Kerberos for client-facing authentication and for its connection to Ozone Manager.

HttpFS acts as a secure proxy, authenticating clients and then using its own identity to connect to OM.

### Recon Server

Provides a web UI and REST APIs for cluster monitoring.

#### Introduction to Recon Security

Recon's HTTP/HTTPS endpoints can be secured using Kerberos SPNEGO.

#### Kerberos-based Authentication

| Property                                   | Value                                                        | Description                                 |
| :----------------------------------------- | :----------------------------------------------------------- | :------------------------------------------ |
| `ozone.security.http.kerberos.enabled`     | `true`                                                       | Global switch for Kerberos on Ozone HTTP endpoints. |
| `ozone.http.filter.initializers`           | `org.apache.hadoop.security.AuthenticationFilterInitializer` | Filter initializer for SPNEGO.              |
| `ozone.recon.http.auth.type`               | `kerberos`                                                   | Sets Recon HTTP authentication to `kerberos`.|
| `ozone.recon.http.auth.kerberos.principal` | `HTTP/_HOST@REALM`                                           | Recon HTTP service principal.               |
| `ozone.recon.http.auth.kerberos.keytab`    | `/path/to/HTTP.keytab`                                       | Keytab for Recon HTTP principal.            |
| `ozone.recon.administrators`               | `comma,separated,kerberos,users,or,groups`                   | Users/groups with admin privileges in Recon.|

Access to Recon's admin-only APIs is controlled by `ozone.administrators` or `ozone.recon.administrators` lists.

---
## Further Reading

* [Securing Datanodes]({{< ref "security/securingdatanodes.md" >}})
* [Network Ports]({{< ref "concept/networkports.md" >}})
* [Apache Ratis](https://github.com/apache/ratis)
