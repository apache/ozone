---
name: Ozone
title: An Introduction to Apache Ozone
menu: main
weight: -10
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

# An Introduction to Apache Ozone

**Apache Ozone is a highly scalable, distributed object store built for big data applications.** It can store billions of objects, both large and small, and is designed to run effectively in on-premise and containerized environments like Kubernetes.

Think of it as a private, on-premise storage platform that speaks the S3 protocol, while also offering native support for the Hadoop ecosystem.

{{<figure class="ozone-usage" src="/ozone-usage.png" width="60%">}}

---

### Why Use Ozone?

*   **Massive Scalability:** Ozone's architecture separates namespace management from block management, allowing it to scale to billions of objects without the limitations found in traditional filesystems.
*   **S3-Compatible:** Use the vast ecosystem of S3 tools, SDKs, and applications you already know. Ozone's S3 Gateway provides a compatible REST interface.
*   **Hadoop Ecosystem Native:** Applications like Apache Spark, Hive, and YARN can use Ozone as their storage backend without any modifications, making it a powerful replacement or complement to HDFS.
*   **Cloud-Native Ready:** Ozone is designed to be deployed and managed in containerized environments like Kubernetes, supporting modern, cloud-native data architectures.

---

### How It Compares

| Feature | Apache Ozone | HDFS (Hadoop Distributed File System)                                                            | Amazon S3 |
| :--- | :--- |:-------------------------------------------------------------------------------------------------| :--- |
| **Type** | Distributed Object Store | Distributed File System                                                                          | Cloud Object Store |
| **Best For** | Billions of mixed-size files, cloud-native apps, data lakes. | Very large files (hundreds of megabytes and above), streaming data access. | Fully managed cloud storage, web applications, backups. |
| **API** | S3-compatible, Hadoop FS | Hadoop FS                                                                                        | S3 API |
| **Deployment**| On-premise, private cloud, Kubernetes | On-premise, private cloud                                                                        | Public Cloud (AWS) |
| **Namespace** | Multiple volumes | Single rooted filesystem (`/`)                                                                   | Global bucket namespace |

---

### Getting Started: Your First Cluster in 5 Minutes

The fastest way to experience Ozone is with Docker. This single command will set up a complete, multi-node pseudo-cluster on your local machine.

**[➡️ Quick Start with Docker]({{< ref "start/StartFromDockerHub.md" >}})**

This is the recommended path for first-time users. For other deployment options, including Kubernetes and bare-metal, see the full [Getting Started Guide]({{< ref "start/_index.md" >}}).

---

### Common Use Cases

Ozone is versatile. Here are a few ways it's commonly used:

*   **Analytics & Data Lakes:** Store vast amounts of structured and unstructured data and run queries directly with Spark, Hive, or Presto.
*   **Machine Learning Backend:** Use Ozone as a central repository for training datasets, models, and experiment logs, accessed via S3 APIs from frameworks like TensorFlow or PyTorch.
*   **Cloud-Native Application Storage:** Provide persistent, scalable storage for stateful applications running on Kubernetes using the Ozone CSI driver.

---

### Core Concepts: Volumes, Buckets, and Keys

Ozone organizes data in a simple three-level hierarchy:

*   **Volumes:** The top-level organizational unit, similar to a user account or a top-level project folder. Volumes are created by administrators.
*   **Buckets:** Reside within volumes and are similar to directories. A bucket can contain any number of keys.
*   **Keys:** The actual objects you store, analogous to files.

This structure provides a flexible way to manage data for multiple tenants and use cases within a single cluster.
