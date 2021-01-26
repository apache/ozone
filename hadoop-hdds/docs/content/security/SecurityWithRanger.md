---
title: "Apache Ranger"
date: "2019-April-03"
weight: 7
menu:
   main:
      parent: Security
summary: Apache Ranger is a framework to enable, monitor and manage comprehensive data security across the Hadoop platform.
icon: user
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


Apache Ranger™ is a framework to enable, monitor and manage comprehensive data
security across the Hadoop platform. Apache Ranger has supported Ozone authentication 
since version 2.0. However, due to some bugs in 2.0, Apache Ranger 
2.1 and later versions are recommended.


To use Apache Ranger, you must have Apache Ranger installed in your Hadoop
Cluster. For installation instructions of Apache Ranger, Please take a look
at the [Apache Ranger website](https://ranger.apache.org/index.html).

If you have a working Apache Ranger installation that is aware of Ozone, then
configuring Ozone to work with Apache Ranger is trivial. You have to enable
the ACLs support and set the acl authorizer class inside Ozone to be Ranger
authorizer. Please add the following properties to the ozone-site.xml.

Property|Value
--------|------------------------------------------------------------
ozone.acl.enabled         | true
ozone.acl.authorizer.class| org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer

The Ranger permissions corresponding to the Ozone operations are as follows:

| operation&permission | Volume  permission | Bucket permission | Key permission |
| :--- | :--- | :--- | :--- |
| Create  volume | CREATE | | |
| List volume | LIST | | |
| Get volume Info | READ | | |
| Delete volume | DELETE | | |
| Create  bucket | READ | CREATE | |
| List bucket | LIST, READ | | |
| Get bucket info | READ | READ | |
| Delete bucket | READ | DELETE | |
| List key | READ | LIST, READ | |
| Write key | READ | READ | CREATE, WRITE |
| Read key | READ | READ | READ |
