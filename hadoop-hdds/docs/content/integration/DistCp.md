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

Note that `distcp` is supported between encrypted Ozone clusters starting with Ozone 2.0.

For more information about using Hadoop DistCp, consult the [DistCp Guide](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html).

## Troubleshooting Common Issues

### Delegation Token Issues
If a DistCp command fails and the error output contains "OzoneToken", indicating an issue with retrieving a delegation token from the destination (or source) Ozone cluster, ensure that Ozone's security is explicitly enabled in the client's configuration.

Add the following property to `ozone-site.xml` on the node where you run the DistCp command:

```xml
    <property>
      <name>ozone.security.enabled</name>
      <value>true</value>
    </property>
```

This helps the client correctly engage in secure communication protocols with Ozone.

### Cross-Realm Kerberos (Ozone 1.x)

**Affected Versions:** Ozone 1.x

When issuing DistCp commands (or other HDFS-compatible commands like `hdfs dfs -ls`) against an Ozone cluster in a different Kerberos realm than the client or source/destination cluster, Ozone 1.x versions may produce an error similar to:

    24/02/07 18:47:36 INFO retry.RetryInvocationHandler: com.google.protobuf.ServiceException: java.io.IOException: DestHost:destPort host1.dst.example.com:9862, LocalHost:localPort host1.src.example.com/10.140.99.144:0. Failed on local exception: java.io.IOException: Couldn't set up I/O streams: java.lang.IllegalArgumentException: Server has invalid Kerberos principal: om/host1.dst.example.com@DST.LOCAL, expecting: OM/host1.dst.example.com@REALM, while invoking $Proxy10.submitRequest over nodeId=om26,nodeAddress=host1.dst.example.com:9862 after 3 failover attempts. Trying to failover immediately.


**Cause:**
This typically occurs because the Ozone Manager's Kerberos principal (`ozone.om.kerberos.principal`) is not defined or interpreted in a way that accommodates cross-realm interactions. The client expects a principal from its own realm or a specifically trusted one, and a mismatch occurs. This issue is addressed in Ozone 2.0.

**Workaround (for Ozone 1.x):**
To resolve this in Ozone 1.x, add the following property to the `ozone-site.xml` on the client machine running DistCp (and potentially on the Ozone Managers if they also need to inter-communicate across realms):

    <property>
      <name>ozone.om.kerberos.principal.pattern</name>
      <value>*</value>
    </property>

This configuration relaxes the principal matching, allowing the client to accept Ozone Manager principals from different realms.

**Fix:**
This bug is tracked by [HDDS-10328](https://issues.apache.org/jira/browse/HDDS-10328) and is fixed in Ozone 2.0 and later versions. Upgrading to Ozone 2.0+ is the recommended long-term solution.

### Token Renewal Failures in Bidirectional Cross-Realm Trust Environments

In environments with bidirectional cross-realm Kerberos trust, DistCp jobs (which run as MapReduce jobs) may fail during execution due to errors renewing delegation tokens. An example error is:

    24/02/08 00:35:00 ERROR tools.DistCp: Exception encountered
    java.io.IOException: org.apache.hadoop.yarn.exceptions.YarnException: Failed to submit application_1707350431298_0001 to YARN: Failed to renew token: Kind: HDFS_DELEGATION_TOKEN, Service: 10.140.99.144:8020, Ident: (token for systest: HDFS_DELEGATION_TOKEN owner=user1@SRC.EXAMPLE.COM, renewer=yarn, realUser=, issueDate=1707352474394, maxDate=1707957274394, sequenceNumber=44, masterKeyId=14)

This can happen when the MapReduce job attempts to renew a delegation token for a remote HDFS or Ozone filesystem, and the renewal fails due to cross-realm complexities. The `Service` field in the error (e.g., `10.140.99.144:8020`) usually indicates the filesystem whose token renewal failed.

**Solution:**
You can prevent the DistCp MapReduce job from attempting to renew delegation tokens for specific HDFS-compatible filesystems by using the `-Dmapreduce.job.hdfs-servers.token-renewal.exclude` parameter. The value should be the authority (hostname or service ID) of the cluster whose tokens are causing renewal issues.

**Parameter:**

```shell
    -Dmapreduce.job.hdfs-servers.token-renewal.exclude=<authority_of_filesystem_to_exclude>
```

For an HDFS cluster, `<authority_of_filesystem_to_exclude>` would be its NameNode address (e.g., `namenode.example.com:8020` or just `namenode.example.com` if the port is standard). For an Ozone cluster, it would be its service ID (e.g., `ozoneprod`).

**Example:**
If you are running the DistCp command on a YARN cluster associated with the *destination* Ozone cluster (`ofs://ozone1707264383/...`) and copying data *from* a source HDFS cluster (`hdfs://host1.src.example.com:8020/...`), and the token renewal for the source HDFS cluster is failing:

```shell
    hadoop distcp \
      -Dmapreduce.job.hdfs-servers.token-renewal.exclude=host1.src.example.com \
      -Ddfs.checksum.combine.mode=COMPOSITE_CRC \
      -Dozone.client.checksum.type=CRC32C \
      hdfs://host1.src.example.com:8020/tmp/ \
      ofs://ozone1707264383/tmp/dest
```

In this example, `host1.src.example.com` is the authority of the source HDFS cluster, and its tokens will not be renewed by the DistCp MapReduce job. Adjust the value based on which cluster's (source or destination, HDFS or Ozone) token renewals are problematic.
