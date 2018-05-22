---
title: Getting started
weight: -2
menu: main
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Ozone - Object store for Hadoop
==============================

Introduction
------------
Ozone is an object store for Hadoop. It  is a redundant, distributed object
store build by leveraging primitives present in HDFS. Ozone supports REST
API for accessing the store.

Getting Started
---------------
Ozone is a work in progress and currently lives in the hadoop source tree.
The subprojects (ozone/hdds) are part of the hadoop source tree but by default
not compiled and not part of the official releases. To
use it, you have to build a package by yourself and deploy a cluster.

### Building Ozone

To build Ozone, please checkout the hadoop sources from github. Then
checkout the trunk branch and build it.

`mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true -Pdist -Phdds -Dtar -DskipShade`

skipShade is just to make compilation faster and not really required.

This will give you a tarball in your distribution directory. This is the
tarball that can be used for deploying your hadoop cluster. Here is an
example of the tarball that will be generated.

* `~/apache/hadoop/hadoop-dist/target/${project.version}.tar.gz`

At this point we have an option to setup a physical cluster or run ozone via
docker.

Running Ozone via Docker
------------------------

This assumes that you have a running docker setup on the machine. Please run
these following commands to see ozone in action.

 Go to the directory where the docker compose files exist.


 - `cd hadoop-dist/target/compose/ozone`

Tell docker to start ozone, this will start a KSM, SCM and a single datanode in
the background.


 - `docker-compose up -d`

Now let us run some work load against ozone, to do that we will run freon.

This will log into the datanode and run bash.

 - `docker-compose exec datanode bash`

Now you can run the `ozone` command shell or freon, the ozone load generator.

This is the command to run freon.

 - `ozone freon -mode offline -validateWrites -numOfVolumes 1 -numOfBuckets 10 -numOfKeys 100`

You can checkout the KSM UI to see the requests information.

 - `http://localhost:9874/`

If you need more datanode you can scale up:

 - `docker-compose scale datanode=3`

Running Ozone using a real cluster
----------------------------------

Please proceed to setup a hadoop cluster by creating the hdfs-site.xml and
other configuration files that are needed for your cluster.


### Ozone Configuration

Ozone relies on its own configuration file called `ozone-site.xml`. It is
just for convenience and ease of management --  you can add these settings
to `hdfs-site.xml`, if you don't want to keep ozone settings separate.
This document refers to `ozone-site.xml` so that ozone settings are in one
place  and not mingled with HDFS settings.

 * _*ozone.enabled*_  This is the most important setting for ozone.
 Currently, Ozone is an opt-in subsystem of HDFS. By default, Ozone is
 disabled. Setting this flag to `true` enables ozone in the HDFS cluster.
 Here is an example,

```
    <property>
       <name>ozone.enabled</name>
       <value>True</value>
    </property>
```
 *  _*ozone.metadata.dirs*_ Ozone is designed with modern hardware
 in mind. It tries to use SSDs effectively. So users can specify where the
 metadata must reside. Usually you pick your fastest disk (SSD if
 you have them on your nodes). KSM, SCM and datanode will write the metadata
 to these disks. This is a required setting, if this is missing Ozone will
 fail to come up. Here is an example,

```
   <property>
      <name>ozone.metadata.dirs</name>
      <value>/data/disk1/meta</value>
   </property>
```

* _*ozone.scm.names*_ Ozone is build on top of container framework. Storage
 container manager(SCM) is a distributed block service which is used by ozone
 and other storage services.
 This property allows datanodes to discover where SCM is, so that
 datanodes can send heartbeat to SCM. SCM is designed to be highly available
 and datanodes assume there are multiple instances of SCM which form a highly
 available ring. The HA feature of SCM is a work in progress. So we
 configure ozone.scm.names to be a single machine. Here is an example,

```
    <property>
      <name>ozone.scm.names</name>
      <value>scm.hadoop.apache.org</value>
    </property>
```

* _*ozone.scm.datanode.id*_ Each datanode that speaks to SCM generates an ID
just like HDFS.  This is an optional setting. Please note:
This path will be created by datanodes if it doesn't exist already. Here is an
 example,

```
   <property>
      <name>ozone.scm.datanode.id</name>
      <value>/data/disk1/scm/meta/node/datanode.id</value>
   </property>
```

* _*ozone.scm.block.client.address*_ Storage Container Manager(SCM) offers a
 set of services that can be used to build a distributed storage system. One
 of the services offered is the block services. KSM and HDFS would use this
 service. This property describes where KSM can discover SCM's block service
 endpoint. There is corresponding ports etc, but assuming that we are using
 default ports, the server address is the only required field. Here is an
 example,

```
    <property>
      <name>ozone.scm.block.client.address</name>
      <value>scm.hadoop.apache.org</value>
    </property>
```

* _*ozone.ksm.address*_ KSM server address. This is used by Ozonehandler and
Ozone File System.

```
    <property>
       <name>ozone.ksm.address</name>
       <value>ksm.hadoop.apache.org</value>
    </property>
```

* _*dfs.datanode.plugin*_ Datanode service plugins: the container manager part
 of ozone is running inside the datanode as a service plugin. To activate ozone
 you should define the service plugin implementation class. **Important**
 It should be added to the **hdfs-site.xml** as the plugin should be activated
 as part of the normal HDFS Datanode bootstrap.

```
    <property>
       <name>dfs.datanode.plugins</name>
       <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
    </property>
```

Here is a quick summary of settings needed by Ozone.

| Setting                        | Value                        | Comment |
|--------------------------------|------------------------------|------------------------------------------------------------------|
| ozone.enabled                  | True                         | This enables SCM and  containers in HDFS cluster.                |
| ozone.metadata.dirs            | file path                    | The metadata will be stored here.                                |
| ozone.scm.names                | SCM server name              | Hostname:port or or IP:port address of SCM.                      |
| ozone.scm.block.client.address | SCM server name and port     | Used by services like KSM                                        |
| ozone.scm.client.address       | SCM server name and port     | Used by client side                                              |
| ozone.scm.datanode.address     | SCM server name and port     | Used by datanode to talk to SCM                                  |
| ozone.ksm.address              | KSM server name              | Used by Ozone handler and Ozone file system.                     |

 Here is a working example of`ozone-site.xml`.

```
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
          <name>ozone.enabled</name>
          <value>True</value>
        </property>

        <property>
          <name>ozone.metadata.dirs</name>
          <value>/data/disk1/ozone/meta</value>
        </property>

        <property>
          <name>ozone.scm.names</name>
          <value>127.0.0.1</value>
        </property>

        <property>
           <name>ozone.scm.client.address</name>
           <value>127.0.0.1:9860</value>
        </property>

         <property>
           <name>ozone.scm.block.client.address</name>
           <value>127.0.0.1:9863</value>
         </property>

         <property>
           <name>ozone.scm.datanode.address</name>
           <value>127.0.0.1:9861</value>
         </property>

         <property>
           <name>ozone.ksm.address</name>
           <value>127.0.0.1:9874</value>
         </property>
    </configuration>
```

And don't forget to enable the datanode component with adding the
following configuration to the hdfs-site.xml:

```
    <property>
       <name>dfs.datanode.plugins</name>
       <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
    </property>
```

### Starting Ozone

Ozone is designed to run concurrently with HDFS. The simplest way to [start
HDFS](../hadoop-common/ClusterSetup.html) is to run `start-dfs.sh` from the
`$HADOOP/sbin/start-dfs.sh`. Once HDFS
is running, please verify it is fully functional by running some commands like

   - *./hdfs dfs -mkdir /usr*
   - *./hdfs dfs -ls /*

 Once you are sure that HDFS is running, start Ozone. To start  ozone, you
 need to start SCM and KSM. Currently we assume that both KSM and SCM
  is running on the same node, this will change in future.

 The first time you bring up Ozone, SCM must be initialized.

   - `./ozone scm -init`

 Start SCM.

   - `./ozone --daemon start scm`

 Once SCM gets started, KSM must be initialized.

   - `./ozone ksm -createObjectStore`

 Start KSM.

   - `./ozone --daemon start ksm`

if you would like to start HDFS and Ozone together, you can do that by running
 a single command.
 - `$HADOOP/sbin/start-ozone.sh`

 This command will start HDFS and then start the ozone components.

 Once you have ozone running you can use these ozone [shell](./OzoneCommandShell.html)
 commands to  create a  volume, bucket and keys.

### Diagnosing issues

Ozone tries not to pollute the existing HDFS streams of configuration and
logging. So ozone logs are by default configured to be written to a file
called `ozone.log`. This is controlled by the settings in `log4j.properties`
file in the hadoop configuration directory.

Here is the log4j properties that are added by ozone.


```
   #
   # Add a logger for ozone that is separate from the Datanode.
   #
   #log4j.debug=true
   log4j.logger.org.apache.hadoop.ozone=DEBUG,OZONE,FILE

   # Do not log into datanode logs. Remove this line to have single log.
   log4j.additivity.org.apache.hadoop.ozone=false

   # For development purposes, log both to console and log file.
   log4j.appender.OZONE=org.apache.log4j.ConsoleAppender
   log4j.appender.OZONE.Threshold=info
   log4j.appender.OZONE.layout=org.apache.log4j.PatternLayout
   log4j.appender.OZONE.layout.ConversionPattern=%d{ISO8601} [%t] %-5p \
    %X{component} %X{function} %X{resource} %X{user} %X{request} - %m%n

   # Real ozone logger that writes to ozone.log
   log4j.appender.FILE=org.apache.log4j.DailyRollingFileAppender
   log4j.appender.FILE.File=${hadoop.log.dir}/ozone.log
   log4j.appender.FILE.Threshold=debug
   log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
   log4j.appender.FILE.layout.ConversionPattern=%d{ISO8601} [%t] %-5p \
     (%F:%L) %X{function} %X{resource} %X{user} %X{request} - \
      %m%n
```

If you would like to have a single datanode log instead of ozone stuff
getting written to ozone.log, please remove this line or set this to true.

 ` log4j.additivity.org.apache.hadoop.ozone=false`

On the SCM/KSM side, you will be able to see

  - `hadoop-hdfs-ksm-hostname.log`
  - `hadoop-hdfs-scm-hostname.log`

Please file any issues you see under the related issues:

 - [Object store in HDFS: HDFS-7240](https://issues.apache.org/jira/browse/HDFS-7240)
 - [Ozone File System: HDFS-13074](https://issues.apache.org/jira/browse/HDFS-13074)
 - [Building HDFS on top of new storage layer (HDDS): HDFS-10419](https://issues.apache.org/jira/browse/HDFS-10419)
