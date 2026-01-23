---
title: "Ozone 访问控制列表"
date: "2019-04-03"
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

Ozone 既支持类似 Ranger 这样的 ACL 插件，也支持原生的 ACL。如果需要启用原生的 ACL，在 ozone-site.xml 中添加下面的参数：

Property|Value
--------|------------------------------------------------------------
ozone.acl.enabled         | true
ozone.acl.authorizer.class| org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer

Ozone 的 ACL 是 Posix ACL 和 S3 ACL 的超集。

ACL 的通用格式为 _对象_:_角色_:_权限_:_范围_.

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

_范围_ 可选的值包括：:

1. **ACCESS** – 这类 ACL 仅作用于对象本身，不能被继承。它控制对对象本身的访问。
2. **DEFAULT** - 这类 ACL 不仅作用于对象本身，还会被对象的后代继承。不能在叶子对象上设置该类 ACL（因为叶子对象下不能再有其他对象）。 <br>
_注意_：从父级默认 ACL 继承的 ACL， 将根据不同的桶布局遵循以下规则：
   - **启用文件系统的 Legacy 或 FSO**：继承直接父目录的默认ACL。如果直接父目录没有默认ACL，则继承存储桶的默认ACL。
   - **禁用文件系统的 Legacy 或 OBS**：继承桶的默认ACL。

## Ozone 原生 ACL API

ACL 可以通过 Ozone 提供的一系列 API 进行操作，支持的 API 包括：

1. **SetAcl** – 此 API 的参数为用户主体、Ozone 对象名称、Ozone 对象的类型和 ACL 列表。
2. **GetAcl** – 此 API 的参数为 Ozone 对象名称和 Ozone 对象类型，返回值为 ACL 列表。
3. **AddAcl** - 此 API 的参数为 Ozone 对象名称、Ozone 对象类型和待添加的 ACL，新的 ACL 会被添加到该 Ozone 对象的 ACL 条目中。
4. **RemoveAcl** - 此 API 的参数为 Ozone 对象名称、Ozone 对象类型和待删除的 ACL。

## 使用 Ozone CLI 操作 ACL

还可以使用 `ozone sh` 命令来操作 ACL。<br>
用法 : `ozone sh <object> <action> [-a=<value>[,<value>...]] <object-uri>` <br>
`-a` 表示以逗号分隔的 ACL 列表。除了 `getacl` 之外的所有子命令都需要它。<br>
`<value>` 的格式为 **`type:name:rights[scope]`**。<br>
**_type_** 可以是 user, group, world 或 anonymous。<br>
**_name_** 是用户/组的名称。如果 type 为 world 和 anonymous，则 name 应留空或分别为 WORLD 或 ANONYMOUS。 <br>
**_rights_** 可以是 (读取=r, 写入=w, 删除=d, 列举=l, 全部=a, 毫无=n, 创建=c, 读 ACL=x, 写 ACL=y)。<br>
**_scope_** 可以是 **ACCESS** 或 **DEFAULT**. 如果不指定，默认 **ACCESS**。<br>

<div class="alert alert-warning" role="alert">
当对象是前缀时，对象路径必须包含从卷到密钥的目录或前缀的完整路径，例如，<br>
   /volume/bucket/some/key/prefix/ <br>
   注意：结尾的“/”是需要的。
</div>

<br>
以下是 CLI 支持的 ACL 具体操作。

<h3>setacl</h3>

```shell
$ ozone sh bucket setacl -a user:testuser2:a /vol1/bucket1
 ACLs set successfully.
$ ozone sh bucket setacl -a user:om:a,group:om:a /vol1/bucket2
 ACLs set successfully.
$ ozone sh bucket setacl -a=anonymous::lr /vol1/bucket3
 ACLs set successfully.
$ ozone sh bucket setacl -a world::a /vol1/bucket4
 ACLs set successfully.
```

<h3>getacl</h3>

```shell
$ ozone sh bucket getacl /vol1/bucket2 
[ {
  "type" : "USER",
  "name" : "om/om@EXAMPLE.COM",
  "aclScope" : "ACCESS",
  "aclList" : [ "ALL" ]
}, {
  "type" : "GROUP",
  "name" : "om",
  "aclScope" : "ACCESS",
  "aclList" : [ "ALL" ]
} ]
```

<h3>addacl</h3>

```shell
$ ozone sh bucket addacl -a user:testuser2:a /vol1/bucket2
ACL user:testuser2:a[ACCESS] added successfully.

$ ozone sh bucket addacl -a user:testuser:rxy[DEFAULT] /vol1/bucket2
ACL user:testuser:rxy[DEFAULT] added successfully.

$ ozone sh prefix addacl -a user:testuser2:a[DEFAULT] /vol1/buck3/dir1/
ACL user:testuser2:a[DEFAULT] added successfully.
```

<h3>removeacl</h3>

```shell
$ ozone sh bucket removeacl -a user:testuser:r[DEFAULT] /vol1/bucket2
ACL user:testuser:r[DEFAULT] removed successfully.
```

