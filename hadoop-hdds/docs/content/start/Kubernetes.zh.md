---
title: 在 Kubernetes 集群上部署 Ozone
weight: 22
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
 * 可用的 kubernetes 集群（LoadBalancer 和 PersistentVolume 非必需）
 * kubectl
{{< /requirements >}}


由于 _apache/ozone_ 镜像可以从 Docker Hub 获取到，K8s 上的部署过程和 Minikube 上的部署过程十分相似，唯一的区别是我们为 K8s 部署准备了专门的配置
文件（比如，我们可以在每个 K8s 节点上部署一个 Datanode）。


ozone 安装包中的 `kubernetes/examples` 目录包含了为不同用例设计的 Kubernetes 部署资源文件。

使用 ozone 子目录进行部署：

```
cd kubernetes/examples/ozone
kubectl apply -f .
```

用下面的命令检查结果：

```
kubectl get pod
访问 ozone 服务
```

现在你可以访问 ozone 的各个服务，默认情况下它们的端口并没有向外开放，不过你可以通过设置端口转发规则来开放外部访问：

```
kubectl port-forward s3g-0 9878:9878
kubectl port-forward scm-0 9876:9876
```
