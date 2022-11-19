---
title: "透明数据加密"
date: "2019-April-03"
summary: 透明数据加密（Transparent Data Encryption，TDE）以密文形式在磁盘上保存数据，但可以在用户访问的时候自动进行解密。
weight: 3
menu:
   main:
      parent: 安全
icon: lock
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

Ozone TDE 的配置和使用和 HDFS TDE 十分相似，主要的区别是，Ozone 中桶级别的 TDE 必须在创建桶时启用。

### 配置密钥管理服务器

使用 TDE 之前，管理员必须要提前配置密钥管理服务 KMS，并且把 KMS 的 URI 通过 core-site.xml 提供给 Ozone。

参数名 |  值
-----------------------------------|-----------------------------------------
hadoop.security.key.provider.path  | KMS uri. <br> 比如 kms://http@kms-host:9600/kms

### 使用 TDE
如果你的集群已经配置好了 TDE，那么你只需要创建加密密钥并启用桶加密即可。

创建加密密钥的方法为：
   * 使用 hadoop key 命令创建桶加密密钥，和 HDFS 加密区域的使用方法类似。

  ```bash
  hadoop key create enckey
  ```
  上面这个命令会创建一个用于保护桶数据的密钥。创建完成之后，你可以告诉 Ozone 在读写某个桶中的数据时使用这个密钥。

   * 将加密密钥分配给桶

  ```bash
  ozone sh bucket create -k enckey /vol/encryptedbucket
  ```

这条命令执行后，所以写往 _encryptedbucket_ 的数据都会用 enckey 进行加密，当读取里面的数据时，客户端通过 KMS 获取密钥进行解密。换句话说，Ozone 中存储的数据一直是加密的，但用户和客户端对此完全无感知。
