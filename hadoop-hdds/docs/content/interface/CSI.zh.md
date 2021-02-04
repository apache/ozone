---
title: CSI 协议
weight: 3
menu:
   main:
      parent: "编程接口"
summary: Ozone 支持 容器存储接口 (CSI) 协议。你可以通过 Ozone CSI 挂载 Ozone 桶的方式使用 Ozone。
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

容器存储接口 `Container Storage Interface` (CSI) 使存储供应商（SP）能够一次性开发一个插件，并让它跨多个容器编排工作,
就像 Kubernetes 或者 YARN。

获取更多 CSI 的信息，可以参考[SCI spec](https://github.com/container-storage-interface/spec/blob/master/spec.md)

CSI 定义了一个简单的，包含3个接口（Identity， Controller， Node）的 GRPC 接口，它定义了容器编排器如何请求创建新的存储空间或挂载新创建的存储，
但没有定义如何挂载存储。

![CSI](CSI.png)

默认情况下，Ozone CSI 服务使用 S3 FUSE 驱动程序（[goofys](https://github.com/kahing/goofys)）挂载 Ozone 桶。
其他挂载方式（如专用 NFS 服务或本机FUSE驱动程序）的实现正在进行中。



Ozone CSI 是 CSI 的一种实现，它可以将 Ozone 用作容器的存储卷。 

## 入门

首先，我们需要一个带有 s3gateway 的 Ozone 集群，并且它的 OM 和 s3gateway 的端口都可以对 CSI pod 可见，
因为 CSIServer 将会访问 OM 来创建或者删除桶，同时 CSIServer 通过 goofys 创建一个可以访问 s3g 的挂载点来发布卷。 

如果你没有一个运行在 Kubernetes 上的 Ozone 集群，你可以参考[Kubernetes]({{< ref "start/Kubernetes.zh.md" >}}) 来创建一个。
使用来自 `kubernetes/examples/ozone`的资源，你可以找到所有需要的 Kubernetes 资源来和指定的 CSI 运行在一起
(参考 `kubernetes/examples/ozone/csi`)   

现在，使用如下命令，创建 CSI 相关的资源。

```bash
kubectl create -f /ozone/kubernetes/examples/ozone/csi
```

## 创建 pv-test 并查看结果

通过执行以下命令，创建 pv-test 相关的资源。

```bash
kubectl create -f /ozone/kubernetes/examples/ozone/pv-test
```

连接 pod scm-0 并在 /s3v/pvc* 桶中创建一个键值。

```bash
kubectl exec -it  scm-0  bash
[hadoop@scm-0 ~]$ ozone sh bucket list s3v
{
  "metadata" : { },
  "volumeName" : "s3v",
  "name" : "pvc-861e2d8b-2232-4cd1-b43c-c0c26697ab6b",
  "storageType" : "DISK",
  "versioning" : false,
  "creationTime" : "2020-06-11T08:19:47.469Z",
  "encryptionKeyName" : null
}
[hadoop@scm-0 ~]$ ozone sh key put /s3v/pvc-861e2d8b-2232-4cd1-b43c-c0c26697ab6b/A LICENSE.txt
```

现在，通过映射 `ozone-csi-test-webserver-7cbdc5d65c-h5mnn` 端口，我们可以使用浏览器展示其 UI 页面。

```bash
kubectl port-forward ozone-csi-test-webserver-7cbdc5d65c-h5mnn 8000:8000
```

最终，我们可以通过 `http://localhost:8000/` 看到结果

![pvtest-webui](pvtest-webui.png)
