---
title: "Apache Ranger"
date: "2019-04-03"
weight: 7
menu:
   main:
      parent: 安全
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


Apache Ranger™ 是一个用于管理和监控 Hadoop 平台复杂数据权限的框架。Apache Ranger 从2.0版本开始支持Ozone鉴权。但由于在2.0中存在一些bug，因此我们更推荐使用Apache Ranger 2.1及以后版本。

你需要先在你的 Hadoop 集群上安装 Apache Ranger，安装指南可以参考 [Apache Ranger 官网](https://ranger.apache.org/index.html).

如果你已经安装好了 Apache Ranger，那么 Ozone 的配置十分简单，你只需要启用 ACL 支持并且将 ACL 授权类设置为 Ranger 授权类，在 ozone-site.xml 中添加下面的参数：

参数名|参数值
--------|------------------------------------------------------------
ozone.acl.enabled         | true
ozone.acl.authorizer.class| org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer

为了使用 RangerOzoneAuthorizer，还需要在 ozone-env.sh 中增加下面环境变量：
```bash
export OZONE_MANAGER_CLASSPATH="${OZONE_HOME}/share/ozone/lib/libext/*"
```
* ranger-ozone-plugin jars 具体路径取决于 Ranger Ozone plugin 安装配置。
* 如果 ranger-ozone-plugin jars 安装在其他节点，需要拷贝到 Ozone 安装目录。

Ozone各类操作对应Ranger权限如下：

| operation&permission | Volume  permission | Bucket permission | Key permission |
| :--- | :--- | :--- | :--- |
| Create volume | CREATE | | |
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
