---
title: Hive
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

Apache Hive has supported Apache Ozone since Hive 4.0. To enable Hive to work with Ozone paths, ensure that the `ozone-filesystem-hadoop3` JAR is added to the Hive classpath.

## Supported Access Protocols

Hive supports the following protocols for accessing Ozone data:

* ofs
* o3fs
* s3a

## Supported Replication Types

Hive is compatible with Ozone buckets configured with either:

* RATIS (Replication)
* Erasure Coding

## Accessing Ozone Data in Hive

Hive provides two methods to interact with data in Ozone:

* Managed Tables
* External Tables

### Managed Tables
#### Configuring the Hive Warehouse Directory in Ozone
To store managed tables in Ozone, update the following properties in the `hive-site.xml` configuration file:

```xml
<property>
  <name>hive.metastore.warehouse.dir</name>
  <value>ofs://ozone1/vol1/bucket1/warehouse/</value>
</property>
```

#### Creating a Managed Table
You can create a managed table with a standard `CREATE TABLE` statement:

```sql
CREATE TABLE myTable (
    id INT,
    name STRING
);
```

#### Loading Data into a Managed Table
Data can be loaded into a Hive table from an Ozone location:

```sql
LOAD DATA INPATH 'ofs://ozone1/vol1/bucket1/table.csv' INTO TABLE myTable;
```

#### Specifying a Custom Ozone Path
You can define a custom Ozone path for a database using the `MANAGEDLOCATION` clause:

```sql
CREATE DATABASE d1 MANAGEDLOCATION 'ofs://ozone1/vol1/bucket1/data';
```

Tables created in the database d1 will be stored under the specified path:
`ofs://ozone1/vol1/bucket1/data`

#### Verifying the Ozone Path
You can confirm that Hive references the correct Ozone path using:

```sql
SHOW CREATE DATABASE d1;
```

Output Example:

```text
+----------------------------------------------------+
|                   createdb_stmt                    |
+----------------------------------------------------+
| CREATE DATABASE `d1`                               |
| LOCATION                                           |
|   'ofs://ozone1/vol1/bucket1/external/d1.db'       |
| MANAGEDLOCATION                                    |
|   'ofs://ozone1/vol1/bucket1/data'                 |
+----------------------------------------------------+
```

### External Tables

Hive allows the creation of external tables to query existing data stored in Ozone.

#### Creating an External Table
```sql
CREATE EXTERNAL TABLE external_table (
    id INT,
    name STRING
)
LOCATION 'ofs://ozone1/vol1/bucket1/table1';
```

* With external tables, the data is expected to be created and managed by another tool.
* Hive queries the data as-is.
* Note: Dropping an external table in Hive does not delete the associated data.

To set a default path for external tables, configure the following property in the `hive-site.xml` file:
```xml
<property>
  <name>hive.metastore.warehouse.external.dir</name>
  <value>ofs://ozone1/vol1/bucket1/external/</value>
</property>
```
This property specifies the base directory for external tables when no explicit `LOCATION` is provided.

#### Verifying the External Table Path
To confirm the table's metadata and location, use:

```sql
SHOW CREATE TABLE external_table;
```
Output Example:

```text
+----------------------------------------------------+
|                   createtab_stmt                   |
+----------------------------------------------------+
| CREATE EXTERNAL TABLE `external_table`(            |
|   `id` int,                                        |
|   `name` string)                                   |
| ROW FORMAT SERDE                                   |
|   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  |
| STORED AS INPUTFORMAT                              |
|   'org.apache.hadoop.mapred.TextInputFormat'       |
| OUTPUTFORMAT                                       |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' |
| LOCATION                                           |
|   'ofs://ozone1/vol1/bucket1/table1'               |
| TBLPROPERTIES (                                    |
|   'bucketing_version'='2',                         |
|   'transient_lastDdlTime'='1734725573')            |
+----------------------------------------------------+
```

## Using the S3A Protocol
In addition to ofs, Hive can access Ozone using the S3 Gateway via the S3A file system.

For more information, consult:

* The [S3 Protocol]({{< ref "interface/S3.md">}})
* The [Hadoop S3A](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) documentation.
