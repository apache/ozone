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

# History of Apache Hadoop Ozone project

Ozone development was started on a feature branch HDFS-7240 as part of the Apache Hadoop HDFS project. Based on the Jira information the first Ozone commit was the commit of [HDFS-8456 Ozone: Introduce STORAGE_CONTAINER_SERVICE as a new NodeType.](https://issues.apache.org/jira/browse/HDFS-8456) in May 2015.


Ozone is an Object Store for Hadoop which is based on a lower level storage replication layer. This layer was originally called HDSL (Hadoop Distributed Storage Layer) and later renamed to HDDS (Hadoop Distributed Data Storage).

Implementation of the generic storage layer began under [HDFS-11118](https://issues.apache.org/jira/browse/HDFS-11118) together with a iScsi/[jScsi](https://github.com/sebastiangraf/jSCSI) based block storage layer ("CBlock") introduced by [HDFS-11361](https://issues.apache.org/jira/browse/HDFS-11361).

As a summary:

 * HDDS (earlier HDSL): replicates huge binary _containers_ between datanodes
 * Ozone: provides Object Store semantics with the help of HDDS
 * CBlock: provides mountable volumes with the help of the HDDS layer (based on iScsi protocol)

In the beginning of the year 2017 a new podling project was started inside [Apache Incubator](http://incubator.apache.org/): [Apache Ratis](https://ratis.apache.org/). Ratis is an embeddable RAFT protocol implementation it is which became the corner stone of consensus inside both Ozone and HDDS projects. (Started to [be used](https://issues.apache.org/jira/browse/HDFS-11519) by Ozone in March of 2017) 

In the October of 2017 a [discussion](https://lists.apache.org/thread.html/3b5b65ce428f88299e6cb4c5d745ec65917490be9e417d361cc08d7e@%3Chdfs-dev.hadoop.apache.org%3E) has been started on hdfs-dev mailing list to merge the existing functionality to the Apache Hadoop trunk. After a long debate Owen O'Malley [suggested a consensus](https://lists.apache.org/thread.html/c85e5263dcc0ca1d13cbbe3bcfb53236784a39111b8c353f60582eb4@%3Chdfs-dev.hadoop.apache.org%3E) to merge it to the trunk but use separated release cycle:

 > * HDSL become a subproject of Hadoop.
 > * HDSL will release separately from Hadoop. Hadoop releases will not contain HDSL and vice versa.
 > * HDSL will get its own jira instance so that the release tags stay separate.
 > * On trunk (as opposed to release branches) HDSL will be a separate module in Hadoop's source tree. This will enable the HDSL to work on their trunk and the Hadoop trunk without making releases for every change.
 > * Hadoop's trunk will only build HDSL if a non-default profile is enabled. When Hadoop creates a release branch, the RM will delete the HDSL module from the branch.
 > * HDSL will have their own Yetus checks and won't cause failures in the Hadoop patch check.

This proposal was passed and after reorganizing the code (see HDFS-13258) and Ozone [has been voted](https://lists.apache.org/thread.html/ad0fe160ae84be97a0a87865059761ad7cd747be7b2fe060707d4f28@%3Chdfs-dev.hadoop.apache.org%3E) to be merged to the Hadoop trunk at the March of 2018.

As the CBlock feature was not stable enough it was not merged and archived on a separated feature branch which was not synced with the newer Ozone/HDDS features. (Somewhat similar functionality is provided later with S3 Fuse file system and an S3 compatible REST gateway.)

After the merge a new Jira project was created (HDDS) and the work was tracked under that project instead of child issues under HDFS-7240.

In the next year multiple Ozone releases has been published in separated release package. The Ozone source release was developed on the Hadoop trunk, but the Ozone sources are removed from the main Hadoop releases.

Originally, Ozone depended on the in-tree (SNAPSHOT) Hadoop artifacts. It was required to compile the core hadoop-hdfs/hadoop-common artifacts before compiling the Ozone subprojects. During the development this dependency was reduced more and more. With the 0.4.1 release this dependency has been totally removed and it became possible to compile Ozone with the help of the released Hadoop artifacts which made it possible to separate the development of Ozone from the main Hadoop trunk branch.

In October 2019, the Ozone sources were moved out to the [apache/hadoop-ozone](https://github.com/apache/hadoop-ozone) git repository. During this move the git history was transformed to remove old YARN/HDFS/MAPREDUCE tasks. 

 * The first git commit of the new repository is the commit which created the new maven subprojects for Ozone (before the trunk merge)
 * Some of the oldest Ozone commits are available only from the Hadoop repository.
 * Some newer HDDS commits have different commit hash in `hadoop` and `hadoop-ozone` repository.


In March 2020, [Ozone 0.5.0 was released](https://hadoop.apache.org/ozone/release/0.5.0-beta/), the first release marked as _beta_tag (earlier releases were alpha).
