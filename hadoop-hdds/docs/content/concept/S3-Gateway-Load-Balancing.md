---
title: "S3 Gateway Load Balancing"
date: 2025-12-15
weight: 1
menu:
  main:
    parent: "Ozone S3 Gateway"
summary: This document describes how to set up multiple S3 gateways with a proxy for load balancing.

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

## Using Multiple S3 Gateways for Load Balancing

The Ozone S3 Gateway is designed to be stateless, which means it does not store any data on the node where it is running. This stateless architecture allows you to run multiple S3 Gateway nodes and load balance traffic between them.

If you find that a single S3 Gateway is becoming a bottleneck for your S3 traffic, you can improve the throughput by adding more S3 Gateway nodes to your cluster. You can then use a load balancer, such as HAProxy, to distribute the traffic among the S3 Gateway nodes in a round-robin fashion.

### Docker Compose Example

The Ozone source code includes a Docker Compose example for running multiple S3 Gateways with HAProxy. You can find it in the `hadoop-ozone/dist/src/main/compose/common` directory.

*   [s3-haproxy.cfg](https://github.com/apache/ozone/blob/master/hadoop-ozone/dist/src/main/compose/common/s3-haproxy.cfg)
*   [s3-haproxy.yaml](https://github.com/apache/ozone/blob/master/hadoop-ozone/dist/src/main/compose/common/s3-haproxy.yaml)

These examples can help you get started with running a load-balanced S3 Gateway setup.
