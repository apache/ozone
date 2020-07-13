---
title: Kubernetes 上运行 Spark 和 OzoneFS
linktitle: Spark
summary: 如何在 K8s 上通过 Apache Spark 使用 Ozone ?
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

本页介绍如何通过以下组件在 Spark 中使用 Ozone 对象存储：

 - OzoneFS (兼容 Hadoop 的文件系统)
 - Hadoop 2.7 (包含在 Spark 发行包中)
 - Kubernetes 的 Spark 调度器
 - 本地 Spark 客户端


## 准备

下载 Spark 和 Ozone 的最新发行包并解压，本方法使用 `spark-2.4.6-bin-hadoop2.7` 进行了测试。

你还需要准备以下内容：

 * 用来上传下载 spark+ozone 镜像的仓库（本文档中使用 Docker Hub）
 * 自定义镜像的名称，形如 repo/name（本文档中使用 _myrepo/ozone-spark_）
 * 专门的 Kubernetes 命名空间（本文档中使用 _yournamespace_）

## 为 driver 创建 docker 镜像

### 创建 Spark driver/executor 基础镜像

首先使用 Spark 的镜像创建工具创建一个镜像。
在 Spark 发行包中运行以下命令：

```bash
./bin/docker-image-tool.sh -r myrepo -t 2.4.6 build
```

_注意_: 如果你使用 Minikube，需要加上 `-m` 参数来使用 Minikube 镜像的 docker 进程。

```bash
./bin/docker-image-tool.sh -m -r myrepo -t 2.4.6 build
```

`./bin/docker-image-tool.sh` 是 Spark 用来创建镜像的官方工具，上面的步骤会创建多个名为 _myrepo/spark_ 的 Spark 镜像，其中的第一个镜像用作接下来步骤的基础镜像。

### 定制镜像

创建一个用于定制镜像的目录。

从集群中拷贝 `ozone-site.xml`：

```bash
kubectl cp om-0:/opt/hadoop/etc/hadoop/ozone-site.xml .
```

从 Ozone 目录中拷贝 `ozonefs.jar`（__使用 hadoop2 版本！__）

```xml
<configuration>
    <property>
        <name>fs.AbstractFileSystem.o3fs.impl</name>
        <value>org.apache.hadoop.fs.ozone.OzFs</value>
     </property>
</configuration>
```
kubectl cp om-0:/opt/hadoop/share/ozone/lib/hadoop-ozone-filesystem-hadoop2-VERSION.jar hadoop-ozone-filesystem-hadoop2.jar
```


编写新的 Dockerfile 并构建镜像：
```
FROM myrepo/spark:2.4.6
ADD core-site.xml /opt/hadoop/conf/core-site.xml
ADD ozone-site.xml /opt/hadoop/conf/ozone-site.xml
ENV HADOOP_CONF_DIR=/opt/hadoop/conf
ENV SPARK_EXTRA_CLASSPATH=/opt/hadoop/conf
ADD hadoop-ozone-filesystem-hadoop2.jar /opt/hadoop-ozone-filesystem-hadoop2.jar
```

```bash
docker build -t myrepo/spark-ozone
```

对于远程的 Kubernetes 集群，你可能需要推送镜像：

```bash
docker push myrepo/spark-ozone
```

## 创建桶并获取 OzoneFS 路径

下载任意文本文件并保存为 `/tmp/alice.txt`。

```bash
kubectl port-forward s3g-0 9878:9878
aws s3api --endpoint http://localhost:9878 create-bucket --bucket=test
aws s3api --endpoint http://localhost:9878 put-object --bucket test --key alice.txt --body /tmp/alice.txt
```

记下 Ozone 文件系统的 URI，在接下来的 spark-submit 命令中会用到它。

## 创建服务账号

```bash
kubectl create serviceaccount spark -n yournamespace
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=yournamespace:spark --namespace=yournamespace
```
## 运行任务

运行如下的 spark-submit 命令，但需要对下列的值进行修改：

 * kubernetes master url（你可以查看 _~/.kube/config_ 来获取实际值）
 * kubernetes namespace（本例中为 _yournamespace_）
 * serviceAccountName (如果你按照上面的步骤做了，使用 _spark_ 即可）
 * container.image (在本例中该值为 _myrepo/spark-ozone_，在上一步中这个镜像被推送至镜像仓库）
 * 输入文件的位置（o3fs://...），使用上面 `ozone s3 path <桶名>` 命令输出中的字符串即可）

```bash
bin/spark-submit \
    --master k8s://https://kubernetes:6443 \
    --deploy-mode cluster \
    --name spark-word-count \
    --class org.apache.spark.examples.JavaWordCount \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.namespace=yournamespace \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=myrepo/spark-ozone \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --jars /opt/hadoop-ozone-filesystem-hadoop2.jar \
    local:///opt/spark/examples/jars/spark-examples_2.11-2.4.0.jar \
    o3fs://test.s3v.ozone-om-0.ozone-om:9862/alice.txt
```

使用 `kubectl get pod` 命令查看可用的 `spark-word-count-...` pod。

使用 `kubectl logs spark-word-count-1549973913699-driver` 命令查看计算结果。

输出的结果类似如下：

```
...
name: 8
William: 3
this,': 1
SOUP!': 1
`Silence: 1
`Mine: 1
ordered.: 1
considering: 3
muttering: 3
candle: 2
...
```
