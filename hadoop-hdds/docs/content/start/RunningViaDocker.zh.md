---
title: 伪集群部署 Ozone
weight: 23

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

{{< requirements >}}
 * docker 和 docker-compose
{{< /requirements >}}

* 下载 Ozone 二进制压缩包并解压。

* 进入 docker compose 文件所在的目录，执行 `docker-compose` 命令，你的机器会启动一个运行在后台的 ozone 伪集群。

{{< highlight bash >}}
cd compose/ozone/

docker-compose up -d
{{< /highlight >}}

为了验证 Ozone 正常运行，我们可以登录到 Datanode 并运行 Ozone 的负载生成工具 _freon_。 ```exec datanode bash``` 命令会在 Datanode 上启动一个 bash，`ozone freon` 命令在 Datanode 所在的容器内执行，你随时可以通过  CTRL-C 退出 freon，命令行选项 ```rk``` 会让 freon 生成随机的键。

{{< highlight bash >}}
docker-compose exec datanode bash
ozone freon rk
{{< /highlight >}}

你可以通过 http://localhost:9874/ 访问 **OzoneManager UI** 来查看服务端处理 freon 负载的情况，以及浏览 ozone 的配置。

***恭喜，你成功运行了你的第一个 ozone 集群。***

关闭集群的命令为：
{{< highlight bash >}}
docker-compose down
{{< /highlight >}}

