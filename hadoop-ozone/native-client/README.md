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

libo3fs is a JNI based C API for Ozone File System. It provides with read and write functionality on OzoneFileSystem. It also uses some functions from HDFS(Hadoop Distributed File System) for which it uses libhdfs, which is a JNI based C API for Hadoop’s Distributed File System (HDFS). It provides C APIs to a subset of the HDFS APIs to manipulate HDFS files and the filesystem. libhdfs is part of the Hadoop distribution and comes pre-compiled in $HADOOP_HDFS_HOME/lib/native/libhdfs.so .

#The APIs

The libo3fs APIs are a subset of the Ozone FileSystem APIs. The header file for libo3fs describes each API in detail and is available in ${OZONE_HOME}/native-client/libo3fs/o3fs.h.

#Requirements:

1.Hadoop with compiled libhdfs.so
2.Linux kernel > 2.6.9 
3.Compiled Ozone

#Compilation

      1.Compilation of .c files
            In libo3fs directory there is one file o3fs.c.

                Execute the following command to compile it: 

           gcc -fPIC -pthread -I {OZONE_HOME}/native-client/libo3fs -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c o3fs.c
           In the libo3fs-examples directory there are two .c files: libo3fs_read.c and libo3fs_write.c.

               Execute the following command to compile libo3fs_read.c and libo3fs_write.c:

           gcc -fPIC -pthread -I {OZONE_HOME}/native-client/libo3fs -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libo3fs_read.c               
           gcc -fPIC -pthread -I {OZONE_HOME}/native-client/libo3fs -I {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libo3fs_write.c

      2. Generation of libo3fs.so
           Execute the following command to generate a .so:

           gcc -shared o3fs.o hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/mybuild/hdfs.o -o libo3fs.so.

      3. Generation of binary(executable)
           Two binaries have to be generated namely o3fs_read and o3fs_write.
           Execute the following command to generate o3fs_red:

           gcc -L {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib -o o3fs_read libo3fs_read.o -lhdfs -pthread -L/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64/jre/lib/amd64/server -ljvm -L {OZONE_HOME}/native-client/libo3fs -lo3fs
           
           Execute the following command to execute o3fs_write:

           gcc -L {HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib -o o3fs_write libo3fs_write.o -lhdfs -pthread -L/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08- 0.el7_7.x86_64/jre/lib/amd64/server -ljvm -L {OZONE_HOME}/native-client/libo3fs -lo3fs

#Deploying

In root shell execute the following:

    ./o3fs_write filename file_size buffer_size host_name port_number bucket_name volume_name

For example

    ./o3fs_write file1 100 100 127.0.0.1 9862 bucket4 vol4 , where file1 is name of the file, 100 is file size and buffer size, "127.0.0.1 is host", 9862 is the port number, "bucket4" is bucket name and "vol4" is the volume name.

#Common Problems:

CLASSPATH is not set. CLASSPATH can be set using the following command:
 
           export CLASSPATH=$({OZONE_HOME}/hadoop-ozone/dist/target/ozone-0.5.0-SNAPSHOT/bin/ozone classpath ozone-filesystem --glob)

           export CLASSPATH=$CLASSPATH:{OZONE_HOME}/hadoop-ozone/dist/target/ozone-0.5.0-SNAPSHOT/share/ozone/lib/ozone-filesystem-0.5.0-SNAPSHOT.jar

LD_LIBRARY_PATH is not set. LD_LIBRARY_PATH can be set using the following command:  

           export LD_LIBRARY_PATH={HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib:
                  /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64/jre/lib/amd64/server:
                  {OZONE_HOME}/native-client/libo3fs

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
