---
title: "安全化 Ozone"
date: "2019-04-03"
summary: 简要介绍 Ozone 中的安全概念以及安全化 OM 和 SCM 的步骤。
weight: 1
menu:
   main:
      parent: 安全
icon: tower
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


# Kerberos

Ozone 集群的安全依赖于 [Kerberos](https://web.mit.edu/kerberos/)。过去 HDFS 支持在隔离的安全网络中运行，因此可以不进行安全化的集群部署。

Ozone 在这方面与 HDFS 保持一致，但不久之后将 _默认启用安全机制_ 。目前，Ozone 集群启用安全机制需要将配置 **ozone.security.enabled** 设置为 _true_ ，以及将 **hadoop.security.authentication** 设置为 _kerberos_ 。

参数 | 值
----------------------|---------
ozone.security.enabled| _true_
hadoop.security.authentication| _kerberos_

# Tokens #

Ozone 使用 token 的方法来防止 Kerberos 服务器负载过重，当每秒处理上千个请求时，Kerberos 可能无法很好地工作。所以，每次当用户完成一次认证之后，Ozone 会向用户颁发代理 token 和块 token，应用程序可以使用这些 token 来对集群进行特定的操作，就像它们持有 kerberos 凭据一样，Ozone 支持以下类型的 token。

### 代理 Token ###
代理 token 允许应用模拟用户的 kerberos 凭据，它基于 kerberos 的身份认证，由 OM 颁发，当集群启用安全机制时，代理 token 功能默认启用。

### 块 Token ###

用户通过块 token 来读写一个块，它的作用是让数据节点知道用户是否有对块进行读和修改的权限。

### S3AuthInfo ###

S3 使用了一种不一样的共享秘密的安全机制，Ozone 支持 AWS Signature Version 4 协议，从用户的角度来看，Ozone 的 s3 感觉与 AWS S3 无异。 

S3 token 功能在启用安全机制的情况下也默认开启。


Ozone 的每个服务进程都需要一个 Kerberos 服务主体名和对应的 [kerberos keytab](https://web.mit.edu/kerberos/krb5-latest/doc/basic/keytab_def.html) 文件。

ozone-site.xml 中应进行如下配置：

<div class="card-group">
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Storage Container Manager</h3>
      <p class="card-text">
      <br>
        SCM 需要两个 Kerberos 主体，以及两个对应的 keytab 文件。
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">配置</th>
            <th scope="col">描述</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>hdds.scm.kerberos.principal</th>
            <td>SCM 服务主体，例如：scm/_HOST@REALM.COM</td>
          </tr>
          <tr>
            <td>hdds.scm.kerberos.keytab.file</th>
            <td>SCM 进程使用的 keytab 文件</td>
          </tr>
          <tr>
            <td>hdds.scm.http.auth.kerberos.principal</th>
            <td>SCM http 服务主体（当 SCM http 服务器启用了 SPNEGO）</td>
          </tr>
          <tr>
            <td>hdds.scm.http.auth.kerberos.keytab</th>
            <td>SCM http 服务使用的 keytab 文件（当 SCM http 服务器启用了 SPNEGO）</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Ozone Manager</h3>
      <p class="card-text">
      <br>
        和 SCM 一样，OM 也需要两个 Kerberos 主体和对应的 keytab 文件。
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">配置</th>
            <th scope="col">描述</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>ozone.om.kerberos.principal</th>
            <td>OM 服务主体，例如：om/_HOST@REALM.COM</td>
          </tr>
          <tr>
            <td>ozone.om.kerberos.keytab.file</th>
            <td>OM 进程使用的 keytab 文件</td>
          </tr>
          <tr>
            <td>ozone.om.http.auth.kerberos.principal</th>
            <td>OM http 服务主体（当 OM http 服务器启用了 SPNEGO）</td>
          </tr>
          <tr>
            <td>ozone.om.http.auth.kerberos.keytab</th>
            <td>OM http 服务使用的 keytab 文件（当 OM http 服务器启用了 SPNEGO）</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">S3 网关</h3>
      <p class="card-text">
      <br>
        S3 网关只需要一个服务主体，配置如下：
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">配置</th>
            <th scope="col">描述</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>ozone.s3g.kerberos.principal</th>
            <td>S3 网关主体，例如：s3g/_HOST@REALM</td>
          </tr>
          <tr>
            <td>ozone.s3g.kerberos.keytab.file</th>
            <td>S3 网关使用的 keytab 文件，例如：/etc/security/keytabs/s3g.keytab</td>
          </tr>
          <tr>
            <td>ozone.s3g.http.auth.kerberos.principal</th>
            <td>S3 网关主体（当 S3 网关 http 服务器启用了 SPNEGO），例如：HTTP/_HOST@EXAMPLE.COM</td>
          </tr>
          <tr>
            <td>ozone.s3g.http.auth.kerberos.keytab</th>
            <td>S3 网关使用的 keytab 文件（当 S3 网关 http 服务器启用了 SPNEGO）</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</div>
