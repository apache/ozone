---
title: "Apache Ranger"
date: "2019-April-03"
weight: 5
summary: Apache Ranger 是一个用于管理和监控 Hadoop 平台复杂数据权限的框架。
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


Apache Ranger™ 是一个用于管理和监控 Hadoop 平台复杂数据权限的框架。版本大于 1.20 的 Apache Ranger 都可以用于管理 Ozone 集群。

你需要先在你的 Hadoop 集群上安装 Apache Ranger，安装指南可以参考 [Apache Ranger 官网](https://ranger.apache.org/index.html).

如果你已经安装好了 Apache Ranger，那么 Ozone 的配置十分简单，你只需要启用 ACL 支持并且将 ACL 授权类设置为 Ranger 授权类，在 ozone-site.xml 中添加下面的参数：

参数名|参数值
--------|------------------------------------------------------------
ozone.acl.enabled         | true
ozone.acl.authorizer.class| org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer
