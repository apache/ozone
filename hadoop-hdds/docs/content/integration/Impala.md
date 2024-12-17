---
title: Impala
weight: 4
menu:
   main:
      parent: "Application integrations"
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

Starting with version **4.2.0**, Apache Impala provides full support for
querying data stored in **Apache Ozone**. To utilize this functionality,
ensure that your Ozone version is **1.4.0** or later.

## Supported Access Protocols

Impala supports the following protocols for accessing Ozone data:

* ofs
* s3a

Note: The o3fs protocol is **NOT** supported by Impala.

## Supported Bucket Types

Impala is compatible with Ozone buckets configured with either:

* RATIS (Replication)
* Erasure Coding

## Querying Ozone Data with Impala

**If Ozone is configured as the default file system**, you can run Impala
queries seamlessly without modifications, just as if the file system were
HDFS. For example:

```sql
  CREATE TABLE t1 (x INT, s STRING);
```

**If Ozone is not the default file system**, you must specify the Ozone path
explicitly using the LOCATION clause. For example:

```sql
  CREATE DATABASE d1 LOCATION 'ofs://ozone1/vol1/bucket1/d1.db';
```

For additional information, consult the Apache Impala User Documentation
[Using Impala with Apache Ozone Storage](https://impala.apache.org/docs/build/html/topics/impala_ozone.html).