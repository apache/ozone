---
title: Running concurrently with HDFS
weight: 1
menu:
   main:
      parent: Starting
      weight: 4
---

Ozone is designed to work with HDFS. So it is easy to deploy ozone in an
existing HDFS cluster.

Ozone does *not* support security today. It is a work in progress and tracked
 in
[HDDS-4](https://issues.apache.org/jira/browse/HDDS-4). If you enable ozone
in a secure HDFS cluster, for your own protection Ozone will refuse to work.

In other words, till Ozone security work is done, Ozone will not work in any
secure clusters.

The container manager part of Ozone runs inside DataNodes as a pluggable module.
To activate ozone you should define the service plugin implementation class.

<div class="alert alert-warning" role="alert">
<b>Important</b>: It should be added to the <b>hdfs-site.xml</b> as the plugin should
be activated as part of the normal HDFS Datanode bootstrap.
</div>

{{< highlight xml >}}
<property>
   <name>dfs.datanode.plugins</name>
   <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
</property>
{{< /highlight >}}

You also need to add the ozone-datanode-plugin jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/hadoop/ozoneplugin/hadoop-ozone-datanode-plugin.jar
{{< /highlight >}}



To start ozone with HDFS you should start the the following components:

 1. HDFS Namenode (from Hadoop distribution)
 2. HDFS Datanode (from the Hadoop distribution with the plugin on the
 classpath from the Ozone distribution)
 3. Ozone Manager (from the Ozone distribution)
 4. Storage Container manager (from the Ozone distribution)

Please check the log of the datanode whether the HDDS/Ozone plugin is started or
not. Log of datanode should contain something like this:

```
2018-09-17 16:19:24 INFO  HddsDatanodeService:158 - Started plug-in org.apache.hadoop.ozone.web.OzoneHddsDatanodeService@6f94fb9d
```

<div class="alert alert-warning" role="alert">
<b>Note:</b> The current version of Ozone is tested with Hadoop 3.1.
</div>