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

其他任何 `javax.servlet.Filter` 实现——包括 Hadoop 的 `XFrameOptionsFilter` 及站点自定义的包装过滤器——均不可桥接。注册此类过滤器将导致守护进程（OM、SCM、Datanode、S3G、Recon、HttpFS）**启动中止**。

上述四个系列的子类也被接受，因为 Hadoop 自身已桥接的过滤器就是子类——`ProxyUserAuthenticationFilter`、`DelegationTokenAuthenticationFilter`，以及基于它们构建的 HttpFS 和 Recon 认证过滤器。如果您注册站点自定义的子类，请注意桥接会将过滤器包装并向下游传递的请求中的哪些内容带回 jakarta 链：**仅限认证结果**——`getRemoteUser`、`getUserPrincipal`、`getAuthType` 和 `isUserInRole`。过滤器设置的请求属性（attribute）会传递，而向下游传递的响应包装器会以错误方式使请求失败（而非被丢弃）；但该转发请求上的**其他任何**覆写——替换的请求头、参数或远程地址——都会被**静默丢弃**。上述四个已桥接系列，以及 Hadoop 对它们的子类，均仅覆写上述认证相关方法，因此此限制只影响自定义子类。

**升级注意事项：** 在迁移到 Jetty 12 之前，HTTP 服务启动失败仅会被记录日志，守护进程会在没有 Web UI 的情况下继续运行。迁移后，守护进程将拒绝启动，以确保配置错误的过滤器不会被静默跳过。如果您的集群将 `ozone.http.filter.initializers` 设置为自定义过滤器，请在升级前将其迁移至 `jakarta.servlet.Filter`。

### Jetty 12 URI 合规性

Jetty 12 会拒绝 Jetty 9.4 曾接受的语义模糊的 URI 结构，并返回 `400 Bad Request`：空路径段（如 `bucket//key`）、模糊的百分号编码、编码的路径分隔符以及可疑路径字符（解码后的反斜杠、`DEL` 或 C0 控制字符）。

由于 S3 对象键和 WebHDFS 路径本身就可能包含这些序列，**S3 Gateway REST 端点**和 **HttpFS** 放宽了上述四项检查，因此它们仍能提供迁移前所能提供的 URI。其他所有 Ozone HTTP 服务——OM、SCM、Datanode、Recon，以及 S3 Gateway 的 web 管理端点和 STS 端点——均使用 Jetty 12 的默认设置，对此类 URI 返回 `400 Bad Request`。

RFC 3986 明确禁止以未编码形式出现在 URI 中的非法字符——例如 `[`、`]`、`{`、`}` 和 `|`——在所有服务上都会被拒绝并返回 `400 Bad Request`，包括上述放宽了模糊性检查的两个服务。符合规范的 S3 和 WebHDFS 客户端已对这些字符进行百分号编码；如果客户端以未编码形式发送这些字符，则必须在升级前更新客户端。

### HttpFS `/conf` 响应的媒体类型

迁移后，所有 HTTP 服务的 `GET /conf` 均由 Ozone 自己的 `HddsConfServlet` 提供。OM、SCM、Datanode、Recon 以及 S3 Gateway 在 Jetty 12 迁移之前就已注册该 servlet，因此只有 **HttpFS** 会发生变化——它此前一直回落到 Hadoop 的 `ConfServlet`——而且变化仅限于响应的 XML 形式：

Accept 请求头 | 变更前（HttpFS） | 变更后
-----------------------------------|--------------------------------------|-------------------
包含 `json` | `application/json;charset=utf-8` | `application/json;charset=utf-8`
其他值或未设置 | `text/xml;charset=utf-8` | `application/xml;charset=utf-8`

响应格式仍然仅依据 `Accept` 请求头选择，文档内容本身没有变化——两个 servlet 都使用 Hadoop 的 `Configuration.dumpConfiguration` 和 `Configuration.writeXml` 生成内容。解析该响应头的客户端不受影响；但按字面字符串比较 XML 响应头 `Content-Type` 的监控或脚本需要更新。此外，HttpFS 还新增了 Hadoop 的 servlet 未提供的、仅 Ozone 支持的 `/conf?cmd=getOzoneTags` 和 `/conf?cmd=getPropertyByTag&tags=...` 命令。

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
