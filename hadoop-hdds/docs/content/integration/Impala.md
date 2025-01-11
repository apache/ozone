---
title: Impala
weight: 4
menu:
   main:
      parent: "Application Integrations"
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

Starting with version 4.2.0, Apache Impala provides full support for querying data stored in Apache Ozone. To utilize this functionality, ensure that your Ozone version is 1.4.0 or later.

## Supported Access Protocols

Impala supports the following protocols for accessing Ozone data:

* ofs
* s3a

Note: The o3fs protocol is **NOT** supported by Impala.

## Supported Replication Types

Impala is compatible with Ozone buckets configured with either:

* RATIS (Replication)
* Erasure Coding

## Querying Ozone Data with Impala

Impala provides two approaches to interact with Ozone:

* Managed Tables
* External Tables

### Managed Tables

If the Hive Warehouse Directory is located in Ozone, you can execute Impala queries without any changes, treating the Ozone file system like HDFS. For example:

```sql
CREATE DATABASE d1;
```

```sql
CREATE TABLE t1 (x INT, s STRING);
```

The data will be stored under the Hive Warehouse Directory path in Ozone.

#### Specifying a Custom Ozone Path

You can create managed databases, tables, or partitions at a specific Ozone path using the `LOCATION` clause. Example:

```sql
CREATE DATABASE d1 LOCATION 'ofs://ozone1/vol1/bucket1/d1.db';
```

```sql
CREATE TABLE t1 LOCATION 'ofs://ozone1/vol1/bucket1/table1';
```

### External Tables

You can create an external table in Impala to query Ozone data. For example:

```sql
CREATE EXTERNAL TABLE external_table (
  id INT,
  name STRING
)
LOCATION 'ofs://ozone1/vol1/bucket1/table1';
```

* With external tables, the data is expected to be created and managed by another tool.
* Impala queries the data as-is.
* The metadata is stored under the external warehouse directory.
* Note: Dropping an external table in Impala does not delete the associated data.


## Using the S3A Protocol

In addition to ofs, Impala can access Ozone via the S3 Gateway using the S3A file system. For more details, refer to 
* The [S3 Protocol]({{< ref "interface/S3.md">}})
* The [Hadoop S3A](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) documentation.

For additional information, consult the Apache Impala User Documentation
[Using Impala with Apache Ozone Storage](https://impala.apache.org/docs/build/html/topics/impala_ozone.html).
