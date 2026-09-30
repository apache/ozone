---
title: "安全化 S3"
date: "2019-04-03"
summary: Ozone 支持 S3 协议，并使用 AWS Signature Version 4 protocol which allows a seamless S3
 experience.
weight: 5
menu:
   main:
      parent: 安全
icon: cloud
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

用户需要由 AWS 网站生成的 AWS access key ID 和 AWS secret 来访问 AWS S3 的桶，当你使用 Ozone 的 S3 协议时，你也需要同样的 AWS access key 和 secret。

在 Ozone 中，用户可以直接下载 access key。用户需要先执行 `kinit` 命令进行 Kerberos 认证，认证通过后就可以下载 S3 access key 和 secret。和 AWS S3 一样，access key 和 secret 具有 S3 桶的全部权限，用户需要保管好 key 和 secret。

* S3 客户端可以从 OM 获取 access key id 和 secret。

```bash
ozone s3 getsecret
```

* 或者通过向 /secret S3 REST 端点发送请求。

```bash
curl -X PUT --negotiate -u : https://localhost:9879/secret
```

这条命令会与 Ozone 进行通信，对用户进行 Kerberos 认证并生成 AWS 凭据，结果会直接打印在屏幕上，你可以将其配置在 _.aws._ 文件中，这样可以在操作 Ozone S3 桶时自动进行认证。

<div class="alert alert-danger" role="alert">
请注意：这些 S3 凭据和你的 Kerberos 密码一样，具有你所有桶的完全访问权限。 
</div>


* 在 aws 配置中添加上述凭据：

```bash
aws configure set default.s3.signature_version s3v4
aws configure set aws_access_key_id ${accessId}
aws configure set aws_secret_access_key ${secret}
aws configure set region us-west-1
```
关于通过命令行和 S3 API 使用 S3，请参考 AWS S3 的文档。
