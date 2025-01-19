---
title: Hadoop DistCp
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

[Hadoop DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html) is a command line, MapReduce-based tool for bulk data copying.

The `hadoop distcp` command can be used to copy data to and from Ozone and any Hadoop compatible file systems, such as HDFS or S3A.

## Basic usage

To copy files from a source Ozone cluster directory to a destination Ozone cluster directory, use the following command:

```bash
hadoop distcp ofs://ozone1/vol1/bucket/dir1 ofs://ozone2/vol2/bucket2/dir2
```

You must define the service IDs for both `ozone1` and `ozone2` clusters in the `ozone-site.xml` configuration file. For example:
```bash
<property>
  <name>ozone.om.service.ids</name>
  <value>ozone1,ozone2</value>
</property>
```

Next, define their logical mappings. For more details, refer to [OM High Availability]({{< ref "OM-HA.md" >}}).

## Copy from HDFS to Ozone

DistCp performs a file checksum check to ensure file integrity. However, since the default checksum type of HDFS (`CRC32C`) differs from that of Ozone (`CRC32`), the file checksum check will cause the DistCp job to fail.

To prevent job failures, specify checksum options in the DistCp command to force Ozone to use the same checksum type as HDFS. For example:

```bash
hadoop distcp \
  -Ddfs.checksum.combine.mode=COMPOSITE_CRC \
  -Dozone.client.checksum.type=CRC32C \
  hdfs://ns1/tmp ofs://ozone1/vol1/bucket1/dst
```

> Note: The parameter `-Ddfs.checksum.combine.mode=COMPOSITE_CRC` is not required if the HDFS cluster is running Hadoop 3.1.1 or later.

Alternatively, you can skip the file checksum check entirely:

```bash
hadoop distcp \
  -skipcrccheck \
  hdfs://ns1/tmp ofs://ozone1/vol1/bucket1/dst
```

## Copy from Ozone to HDFS

When copying files from Ozone to HDFS, similar issues can occur due to differences in checksum types. In this case, you must configure the checksum type for HDFS, as it is the destination system.

Example:

```bash
hadoop distcp \
  -Ddfs.checksum.combine.mode=COMPOSITE_CRC \
  -Ddfs.checksum.type=CRC32 \
  ofs://ozone1/vol1/bucket1/src hdfs://ns1/tmp/dst 
```

By specifying the appropriate checksum configuration or skipping the validation, you can ensure that DistCp jobs complete successfully when transferring data between HDFS and Ozone.

## Encrypted data

When data resides in an HDFS encryption zone or Ozone encrypted buckets, the file checksum will not match. This is because the underlying block data differs due to the use of a new EDEK (Encryption Data Encryption Key) at the destination. In such cases, specify the `-skipcrccheck` parameter to avoid job failures.

For more information about using Hadoop DistCp, consult the [DistCp Guide](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html).
