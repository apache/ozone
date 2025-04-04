---
title: "Network Ports used by Apache Ozone"
date: "2025-04-03"
weight: 9
menu: 
  main:
     parent: Architecture
summary: Understanding and correctly configuring the network ports used by Apache Ozone is essential for the successful deployment, operation, and maintenance of Apache Ozone clusters.

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

This document provides a comprehensive overview of the network ports utilized by Apache Ozone. Due to its distributed nature and the requirement for high performance in handling data-intensive tasks, understanding and correctly configuring these network ports is essential for the successful deployment, operation, and maintenance of Apache Ozone clusters.

# **Ozone Manager (OM)**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 9862 | ozone.om.address | Hadooop RPC | Primary RPC endpoint for Ozone clients. |
| 9874 | ozone.om.http-address | HTTP | Web UI for monitoring OM status and metadata. |
| 9875 | ozone.om.https-address | HTTPS | Secure Web UI for monitoring OM status and metadata. |
| 9872 | ozone.om.ratis.port  | HTTP/2 | RPC endpoint for OM HA instances to form a RAFT consensus ring |
| N/A | ozone.om.grpc.port | HTTP/2 | gRPC endpoint for Ozone Manager clients |

# **Storage Container Manager (SCM)**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 9860 | ozone.scm.client.port | Hadoop RPC | Communication with Ozone clients for namespace and container management. If ozone.scm.client.address is defined (default is empty), ozone.scm.client.address overrides it. |
| 9863 | ozone.scm.block.client.port | Hadoop RPC | Communication with Datanodes for block-level operations. |
| 9876 | ozone.scm.http-address | HTTP | Web UI for monitoring SCM status. |
| 9877 | ozone.scm.https-address | HTTPS | Secure Web UI for monitoring SCM status. |
| 9861 | ozone.scm.datanode.port | Hadoop RPC | Port used by DataNodes to communicate with the SCM. If ozone.scm.datanode.address is defined (default is empty), ozone.scm.datanode.address overrides it. |
| 9961 | ozone.scm.security.service.port  | Hadoop RPC | SCM security server port |
| 9894 | ozone.scm.ratis.port  | HTTP/2 | SCM Ratis HA |
| 9895 | ozone.scm.grpc.port  | HTTP/2 | SCM GRPC server port |

# **Recon**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 9891 | ozone.recon.address | Hadoop RPC | RPC address for Recon to collect metadata from other Ozone services. |
| 9888 | ozone.recon.http-address | HTTP | Web-based management and monitoring console for the entire Ozone cluster. |
| 9889 | ozone.recon.https-address | HTTPS | Web-based management and monitoring console for the entire Ozone cluster. |

# **S3 Gateway (S3G)**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 9878 | ozone.s3g.http-address | HTTP | S3-compatible RESTful API endpoint. |
| 9879 | ozone.s3g.https-address | HTTPS | Secure S3-compatible RESTful API endpoint. |
| 19878 | ozone.s3g.webadmin.http-address | HTTP | Ozone S3Gateway serves web content |
| 19879 | ozone.s3g.webadmin.https-address | HTTPS | Ozone S3Gateway serves web content |

# **HttpFS Server**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 14000 | httpfs.http.port | HTTP or HTTPS | The HTTP port for HttpFS REST API. TLS is enabled if httpfs.ssl.enabled is true. |

# **Datanode**

| Default Port Number | Configuration Key | Endpoint Protocol | Purpose |
| :---- | :---- | :---- | :---- |
| 9859 | hdds.container.ipc.port | HTTP/2 | Inter-process communication related to container operations. |
| 9855 | hdds.container.ratis.datastream.port | TCP | Ratis data streaming for container replication (if enabled). |
| 9858 | hdds.container.ratis.ipc.port | HTTP/2 | Communication with embedded Ratis server for replication coordination. |
| 9857 | hdds.container.ratis.admin.port | HTTP/2 | Administrative requests to the Ratis server. |
| 9856 | hdds.container.ratis.server.port | HTTP/2 | Communication between Ratis peers in a replication pipeline. |
| 9882 | hdds.datanode.http-address  | HTTP | Web UI for monitoring Datanode status and resource utilization. |
| 9883 | hdds.datanode.https-address  | HTTPS | Secure Web UI for monitoring Datanode status and resource utilization. |
| 19864 | hdds.datanode.client.port  | Hadoop RPC | The port number of the Ozone Datanode client service.  |

Note:

* The default port values can be overridden within the ozone-site.xml configuration file, with the exception of Httpfs ports, which are configurable via the httpfs-site.xml configuration file.
* Hadoop RPC, a binary protocol operating over TCP, may be authenticated and encrypted using the Java SASL mechanism.
* Authentication of the Web UI and HttpFS is achievable through Kerberos/SPNEGO, with encryption facilitated by HTTPS.
* S3 client connections to the S3 Gateway undergo authentication utilizing S3 secrets and encryption via TLS.
* A series of Ozone service ports are established by gRPC and Ratis, the latter employing gRPC. gRPC, a protocol based on HTTP/2, is capable of being encrypted with TLS.
* Ratis streaming ports, initiated by Netty, can be secured through TLS encryption.
