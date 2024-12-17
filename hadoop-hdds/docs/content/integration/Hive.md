---
title: Hive
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

Hive has supported Ozone since Apache Hive 4.0 through [HIVE-26360](https://issues.apache.org/jira/browse/HIVE-26360).

To enable Hive to work with Ozone paths, ensure that the Ozone `ozone-filesystem-hadoop3` JAR file is added to the Hive classpath.

# Accessing Ozone Data in Hive

Hive provides two approaches to interact with Ozone:

* External Tables
* Managed Tables

## External Tables

You can create an external table in Hive using data stored in Ozone. For example:

```sql
CREATE EXTERNAL TABLE external_table (
    id INT,
    name STRING
)
LOCATION 'ofs://ozone1/vol1/bucket1/table1';
```

You can verify that Hive correctly references the Ozone path by running:

```sql
SHOW CREATE EXTERNAL TABLE external_table;
```

## Managed Tables

Hive allows you to configure the Warehouse Directory to store managed tables in Ozone.

To do this, set the following properties in the `hive-site.xml` configuration file:

```xml
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>ofs://ozone1/vol1/bucket1/warehouse/</value>
</property>
<property>
  <name>hive.metastore.warehouse.external.dir</name>
  <value>ofs://ozone1/vol1/bucket1/external/</value>
</property>
```

To create a managed table:
```sql
CREATE TABLE myTable (
    id INT,
    name STRING
)
```

You can also load data from an Ozone location into a Hive table. For example:

```sql
LOAD DATA INPATH 'ofs://ozone1/vol1/bucket1/table.csv' INTO TABLE myTable;
```

## Using the S3A Protocol
In addition to ofs, Hive can access Ozone via the S3 Gateway using the S3A file system. For more details, refer to the [S3 Protocol]({{< ref "interface/S3.md">}}) documentation.
