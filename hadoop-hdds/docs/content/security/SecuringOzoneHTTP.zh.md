---
title: "安全化 HTTP"
date: "2020-06-17"
summary: 安全化 Ozone 服务的 HTTP 网络控制台
weight: 4
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

本文档介绍了如何配置 Ozone HTTP Web 控制台以要求用户身份验证。

### 默认身份验证

默认情况下 Ozone HTTP Web 控制台 (OM、SCM、S3G、Recon、Datanode) 根据以下默认配置允许无需身份验证的访问。 

参数 | 值
-----------------------------------|-----------------------------------------
ozone.security.http.kerberos.enabled | false
ozone.http.filter.initializers | <empty>

如果您有一个启用了 SPNEGO 的 Ozone 集群,并且想要为所有 Ozone 服务禁用它,只需确保按上述两个参数配置即可。

### 基于 Kerberos 的 SPNEGO 身份验证

身份验证也可以配置为要求使用 HTTP SPNEGO 协议（被 Firefox 和 Chrome 等浏览器所支持）。为了实现这一点，必须先配置以下参数。

参数 | 值
-----------------------------------|-----------------------------------------
hadoop.security.authentication | kerberos
ozone.security.http.kerberos.enabled | true
ozone.http.filter.initializers | org.apache.hadoop.security.AuthenticationFilterInitializer

之后，各个组件需要正确配置才能完全启用 SPNEGO 或 SIMPLE 身份验证。

### Filter initializer 兼容性

Ozone 的 HTTP 服务运行在 Jetty 12（EE10，`jakarta.servlet`）上。通过 `ozone.http.filter.initializers` 注册的过滤器必须使用 `jakarta.servlet.Filter`，或者属于以下已桥接的 `javax.servlet` 过滤器系列之一：

* `AuthenticationFilter`（hadoop-auth：Kerberos/SPNEGO 和简单身份验证）
* `StaticUserFilter`（未启用安全的控制台使用的静态用户过滤器）
* `CrossOriginFilter`（CORS 响应头）
* `RestCsrfPreventionFilter`（CSRF 防护）

其他任何 `javax.servlet.Filter` 实现——包括 Hadoop 的 `XFrameOptionsFilter` 及站点自定义的包装过滤器——均不可桥接。注册此类过滤器将导致守护进程（OM、SCM、Datanode、S3G、Recon）**启动中止**。

**升级注意事项：** 在迁移到 Jetty 12 之前，HTTP 服务启动失败仅会被记录日志，守护进程会在没有 Web UI 的情况下继续运行。迁移后，守护进程将拒绝启动，以确保配置错误的过滤器不会被静默跳过。如果您的集群将 `ozone.http.filter.initializers` 设置为自定义过滤器，请在升级前将其迁移至 `jakarta.servlet.Filter`。

### Jetty 12 URI 合规性

Jetty 12 连接器接受一些 Jetty 9 曾拒绝的、语义模糊但技术上合法的 URI 结构：空路径段（如 `bucket//key`）、模糊的百分号编码、编码的路径分隔符以及可疑路径字符。这些对于包含特殊但有效字符的 S3 和 WebHDFS 路径是必要的。

但是，RFC 3986 明确禁止以未编码形式出现在 URI 中的非法字符——例如 `[`、`]`、`{`、`}` 和 `|`——仍会被拒绝并返回 `400 Bad Request`。符合规范的 S3 和 WebHDFS 客户端已对这些字符进行百分号编码。如果您的客户端以未编码形式发送这些字符，则必须在升级前更新客户端以对其进行百分号编码。

### 为 OM HTTP 启用 SPNEGO 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.om.http.auth.type | kerberos
ozone.om.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.om.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### 为 S3G HTTP 启用 SPNEGO 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.s3g.http.auth.type | kerberos
ozone.s3g.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.s3g.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### 为 RECON HTTP 启用 SPNEGO 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.recon.http.auth.type | kerberos
ozone.recon.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.recon.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### 为 SCM HTTP 启用 SPNEGO 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
hdds.scm.http.auth.type | kerberos
hdds.scm.http.auth.kerberos.principal | HTTP/_HOST@REALM
hdds.scm.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### 为 DATANODE HTTP 启用 SPNEGO 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
hdds.datanode.http.auth.type | kerberos
hdds.datanode.http.auth.kerberos.principal | HTTP/_HOST@REALM
hdds.datanode.http.auth.kerberos.keytab| /path/to/HTTP.keytab

注意： Ozone datanode 没有默认网页，这会阻止您访问“/”或“/index.html”。但它通过 HTTP 提供了标准 Java Servlet，如 jmx/conf/jstack。

此外，Ozone HTTP Web 控制台支持相当于 Hadoop 的 Pseudo/Simple 身份验证。 如果启用此选项，则必须在第一次与浏览器交互中使用 user.name 指定用户名查询字符串参数。例如，http://scm:9876/?user.name=scmadmin。

### 为 OM HTTP 启用 SIMPLE 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.om.http.auth.type | simple
ozone.om.http.auth.simple.anonymous.allowed | false

如果您不想在查询字符串参数中指定 user.name，更改 ozone.om.http.auth.simple.anonymous.allowed 为 true。

### 为 S3G HTTP 启用 SIMPLE 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.s3g.http.auth.type | simple
ozone.s3g.http.auth.simple.anonymous.allowed | false

如果您不想在查询字符串参数中指定 user.name，更改 ozone.s3g.http.auth.simple.anonymous.allowed 为 true。

### 为 RECON HTTP 启用 SIMPLE 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
ozone.recon.http.auth.type | simple
ozone.recon.http.auth.simple.anonymous.allowed | false

如果您不想在查询字符串参数中指定 user.name，更改 ozone.recon.http.auth.simple.anonymous.allowed 为 true。

### 为 SCM HTTP 启用 SIMPLE 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
hdds.scm.http.auth.type | simple
hdds.scm.http.auth.simple.anonymous.allowed | false

如果您不想在查询字符串参数中指定 user.name，更改 hdds.scm.http.auth.simple.anonymous.allowed 为 true。

### 为 DATANODE HTTP 启用 SIMPLE 身份验证
参数 | 值
-----------------------------------|-----------------------------------------
hdds.datanode.http.auth.type | simple
hdds.datanode.http.auth.simple.anonymous.allowed | false

如果您不想在查询字符串参数中指定 user.name，更改 hdds.datanode.http.auth.simple.anonymous.allowed 为 true。
