<!--
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

#Overview

libozfs is a JNI based C API for Ozone File System. It provides with read and write functionality on OzoneFileSystem. It also uses some functions from HDFS(Hadoop Distributed File System) for which it uses libhdfs, which is a JNI based C API for Hadoop’s Distributed File System (HDFS). It provides C APIs to a subset of the HDFS APIs to manipulate HDFS files and the filesystem. libhdfs is part of the Hadoop distribution and comes pre-compiled in $HADOOP_HDFS_HOME/lib/native/libhdfs.so .

#The APIs

The libozfs APIs are a subset of the Ozone FileSystem APIs. The header file for libozfs describes each API in detail and is available in $HADOOP_HDFS_HOME/include/ozfs.h.

#Requirements:

1.Hadoop with compiled libhdfs.so
2.Linux kernel > 2.6.9 
3.Compiled Ozone

#Compilation

      1.Compilation of .c files
            In libozfs directory there is one file ozfs.c.

                Execute the following command to compile it: 

           gcc -fPIC -pthread -I {OZONE_HOME}/ozone-native-client/libozone -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c ozfs.c
           In the libozfs-examples directory there are two .c files: libozfs_read.c and libozfs_write.c.

               Execute the following command to compile libozfs_read.c and libozfs_write.c:

           gcc -fPIC -pthread -I {OZONE_HOME}/ozone-native-client/libozone -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libozfs_read.c               
           gcc -fPIC -pthread -I {OZONE_HOME}/ozone-native-client/libozone -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libozfs_write.c

      2. Generation of libozfs.so
           Execute the following command to generate a .so:

           gcc -shared ozfs.o hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/mybuild/hdfs.o -o libozfs.so.

      3. Generation of binary(executable)
           Two binaries have to be generated namely ozfs_read and ozfs_write.
           Execute the following command to generate ozfs_red:

           gcc -L {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib -o ozfs_read libozfs_read.o -lhdfs -pthread -L/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64/jre/lib/amd64/server -ljvm -L {OZONE_HOME}/ozone-native-client/libozone -lozfs
           
           Execute the following command to execute ozfs_write:

           gcc -L {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib -o ozfs_write libozfs_write.o -lhdfs -pthread -L/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08- 0.el7_7.x86_64/jre/lib/amd64/server -ljvm -L {OZONE_HOME}/ozone-native-client/libozone -lozfs

#Deploying

In root shell execute the following:

    ./ozfs_write filename file_size buffer_size host_name port_number bucket_name volume_name

For example

    ./ozfs_write file1 100 100 127.0.0.1 9862 bucket4 vol4 , where file1 is name of the file, 100 is file size and buffer size, "127.0.0.1 is host", 9862 is the port number, "bucket4" is bucket name and "vol4" is the volume name.

#Common Problems:

CLASSPATH is not set. CLASSPATH can be set using the following command:
 
           export CLASSPATH=$({OZONE_HOME}/hadoop-ozone/dist/target/ozone-0.5.0-SNAPSHOT/bin/ozone classpath hadoop-ozone-filesystem --glob)

           export CLASSPATH=$CLASSPATH:{OZONE_HOME}/hadoop-ozone/dist/target/ozone-0.5.0-SNAPSHOT/share/ozone/lib/hadoop-ozone-filesystem-0.5.0-SNAPSHOT.jar

LD_LIBRARY_PATH is not set. LD_LIBRARY_PATH can be set using the following command:  

           export LD_LIBRARY_PATH={HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib:
                  /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64/jre/lib/amd64/server:
                  {OZONE_HOME}/ozone-native-client/libozone

Ozone-site.xml is not configured. Save the minimal snippet to hadoop-ozone/dist/target/ozone-*/etc/hadoop/ozone-site.xml in the compiled distribution.

            <configuration>
            <properties>
             <property><name>ozone.scm.datanode.id.dir</name><value>/tmp/ozone/data</value></property>
             <property><name>ozone.replication</name><value>1</value></property>
             <property><name>ozone.metadata.dirs</name><value>/tmp/ozone/data/metadata</value></property>
             <property><name>ozone.scm.names</name><value>localhost</value></property>
             <property><name>ozone.om.address</name><value>localhost</value></property>
            </properties>
            </configuration>
