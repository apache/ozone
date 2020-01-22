---
title: 在 Minikube 中运行 Ozone
weight: 21
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
 * minikube
 * kubectl
{{< /requirements >}}

ozone 发行包中的 `kubernetes/examples` 文件夹下包含了多种用途的 kubernetes 部署资源文件，这些资源文件默认使用 Docker Hub 上的 `apache/ozone` 镜像。

使用 minikube 资源集在 minikube 上进行部署：

```
cd kubernetes/examples/minikube
kubectl apply -f .
```

使用下面的命令检查结果：

```
kubectl get pod
```

注意：kubernetes/exampls/minikube 资源集为 minikube 部署进行了如下优化：

 * 即使你只有一个主机，也可以运行多个 Datanode（在实际的生产集群中，每个物理主机上通常只运行一个 Datanode）
 * Ozone 通过不同的节点端口提供服务

## 访问服务

现在你可以访问 Ozone 的各个服务，minikube 资源集为每个 web 端点额外定义了一个 NodePort 服务，NodePort 服务可以通过指定端口从任意节点访问：

```bash
kubectl get svc
NAME         TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)          AGE
datanode     ClusterIP   None            <none>        <none>           27s
kubernetes   ClusterIP   10.96.0.1       <none>        443/TCP          118m
om           ClusterIP   None            <none>        9874/TCP         27s
om-public    NodePort    10.108.48.148   <none>        9874:32649/TCP   27s
s3g          ClusterIP   None            <none>        9878/TCP         27s
s3g-public   NodePort    10.97.133.137   <none>        9878:31880/TCP   27s
scm          ClusterIP   None            <none>        9876/TCP         27s
scm-public   NodePort    10.105.231.28   <none>        9876:32171/TCP   27s
```

Minikube 为访问任意的 NodePort 服务提供了一个方便的命令：

```
minikube service s3g-public
# 此命令会在默认浏览器中打开 default/s3g-public 服务的页面...
```