---
title: "Ozone 访问控制列表"
date: "2019-April-03"
weight: 6
menu:
   main:
      parent: 安全
summary: Ozone 原生的授权模块提供了不需要集成 Ranger 的访问控制列表（ACL）支持。
icon: transfer
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

Ozone 既支持原生的 ACL，也支持类似 Ranger 这样的 ACL 插件，如果启用了 Ranger 插件，则以 Ranger 中的 ACL 为准。

Ozone 的 ACL 是 Posix ACL 和 S3 ACL 的超集。

ACL 的通用格式为 _对象_:_角色_:_权限_.

_对象_ 可选的值包括：

1. **卷** - 一个 Ozone 卷，比如 _/volume_
2. **桶** - 一个 Ozone 桶，比如 _/volume/bucket_
3. **键** - 一个对象键，比如 _/volume/bucket/key_
4. **前缀** - 某个键的路径前缀，比如 _/volume/bucket/prefix1/prefix2_

_角色_ 可选的值包括:

1. **用户** - 一个 Kerberos 用户，和 Posix 用户一样，用户可以是已创建的也可以是未创建的。
2. **组** - 一个 Kerberos 组，和 Posix 组一样，组可以是已创建的也可以是未创建的。
3. **所有人** - 所有通过 Kerberos 认证的用户，这对应 Posix 标准中的其它用户。
4. **匿名** - 完全忽略用户字段，这是对 Posix 语义的扩展，使用 S3 协议时会用到，用于表达无法获取用户的身份或者不在乎用户的身份。

<div class="alert alert-success" role="alert">
  S3 用户通过 AWS v4 签名协议访问 Ozone 时，OM 会将其转化为对应的 Kerberos 用户。
</div>

_权限_ 可选的值包括：:

1. **创建** – 此 ACL 为用户赋予在卷中创建桶，或者在桶中创建键的权限。请注意：在 Ozone 中，只有管理员可以创建卷。
2. **列举** – 此 ACL 允许用户列举桶和键，因为列举的是子对象，所以这种 ACL 要绑定在卷和桶上。请注意：只有卷的属主和管理员可以对卷执行列举操作。
3. **删除** – 允许用户删除卷、桶或键。
4. **读取** – 允许用户读取卷和桶的元数据，以及读取键的数据流和元数据。
5. **写入** - 允许用户修改卷和桶的元数据，以及重写一个已存在的键。
6. **读 ACL** – 允许用户读取某个对象的 ACL。
7. **写 ACL** – 允许用户修改某个对象的 ACL。

<h3>Ozone 原生 ACL API</h3>

ACL 可以通过 Ozone 提供的一系列 API 进行操作，支持的 API 包括：

1. **SetAcl** – 此 API 的参数为用户主体、Ozone 对象名称、Ozone 对象的类型和 ACL 列表。
2. **GetAcl** – 此 API 的参数为 Ozone 对象名称和 Ozone 对象类型，返回值为 ACL 列表。
3. **AddAcl** - 此 API 的参数为 Ozone 对象名称、Ozone 对象类型和待添加的 ACL，新的 ACL 会被添加到该 Ozone 对象的 ACL 条目中。
4. **RemoveAcl** - 此 API 的参数为 Ozone 对象名称、Ozone 对象类型和待删除的 ACL。
