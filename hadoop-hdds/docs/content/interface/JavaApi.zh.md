---
title: "Java API"
date: "2017-09-14"
weight: 1
menu:
   main:
      parent: "编程接口"
summary: Ozone 有一套基于原生 RPC 的 API，其它协议都由这个最底层的 API 扩展而来，它也是所有 Ozone 支持的协议中性能最好、功能最全的。
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

Ozone 自带了支持 RPC 的客户端库，对于一般的应用场景也可以使用兼容 AWS S3 的 REST 接口替代 Ozone 客户端 API。


## 创建 Ozone 客户端

ozone 客户端由客户端工厂创建，我们可以通过如下调用获得一个 RPC 客户端对象：

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getRpcClient();
{{< /highlight >}}

如果用户想要创建指定配置的客户端，可以这样调用：

{{< highlight java >}}
OzoneClient ozClient = OzoneClientFactory.getClient();
{{< /highlight >}}

这样就会返回一个合适的客户端对象。

## 使用 Ozone 客户端写数据

ozone 中的数据层次为卷、桶和键。卷是桶的集合，桶是键的集合，如果要向 ozone 写数据，你需要依次获得卷、桶和键。

### 创建卷

有了客户端对象之后，我们需要获取 ObjectStore 的引用，获取方法为：

{{< highlight java >}}
ObjectStore objectStore = ozClient.getObjectStore();
{{< /highlight >}}

ObjectStore 对象表示了客户端连接的集群。

{{< highlight java >}}
// 让我们创建一个卷来存储资产数据
// 使用默认参数来创建资产卷
objectStore.createVolume("assets");

// 验证 assets 卷是否已创建
OzoneVolume assets = objectStore.getVolume("assets");
{{< /highlight >}}


createVolume 方法也支持传入参数数组来指定卷的创建参数。

### 创建桶

有了卷之后，你就可以在卷中创建桶。

{{< highlight java >}}
// 创建一个名为 videos 的桶
assets.createBucket("videos");
OzoneBucket video = assets.getBucket("videos");
{{< /highlight >}}

此时我们有了一个可用的卷和一个桶，卷名为 _assets_ ，桶名为 _videos_ 。

接下来我们创建一个键。

### 键的读写

通过桶对象可以进行键的读写，下面的代码从本地磁盘读取名为 intro.mp4 的视频文件，并把它存储到我们刚刚创建的 _video_ 桶中。

{{< highlight java >}}
// 从文件读取数据，需要由用户提供读取函数
byte [] videoData = readFile("intro.mp4");

// 创建一个输出流并写数据
OzoneOutputStream videoStream = video.createKey("intro.mp4", 1048576);
videoStream.write(videoData);

// 写操作完成之后关闭输出流
videoStream.close();


// 我们可以使用同一个桶，通过创建输入流来读取刚刚写入的文件
// 先创建一个用来存储视频的字节数组
byte[] data = new byte[(int)1048576];
OzoneInputStream introStream = video.readKey("intro.mp4");
// 读取 intro.mp4 到缓冲区中
introStream.read(data);
introStream.close();
{{< /highlight >}}


下面是一个完整的代码示例，请注意代码中 close 函数的调用。

{{< highlight java >}}
// 创建客户端对象
OzoneClient ozClient = OzoneClientFactory.getClient();

// 通过客户端对象获取 ObjectStore 的引用
ObjectStore objectStore = ozClient.getObjectStore();

// 创建用于存储数据的 assets 卷
// 此处创建的卷采用默认参数
objectStore.createVolume("assets");

// 验证卷是否已创建
OzoneVolume assets = objectStore.getVolume("assets");

// 创建名为 videos 的桶
assets.createBucket("videos");
OzoneBucket video = assets.getBucket("videos");

// 从文件中读取数据，需要用户提供此函数
byte [] videoData = readFile("intro.mp4");

// 创建输出流并写数据
OzoneOutputStream videoStream = video.createKey("intro.mp4", 1048576);
videoStream.write(videoData);

// 写操作完成之后关闭输出流
videoStream.close();


// 我们可以使用同一个桶，通过创建输入流来读取刚刚写入的文件
// 先创建一个用来存储视频的字节数组

byte[] data = new byte[(int)1048576];
OzoneInputStream introStream = video.readKey("intro.mp4");
introStream.read(data);

// 读操作完成之后关闭输入流
introStream.close();

// 关闭客户端
ozClient.close();
{{< /highlight >}}
