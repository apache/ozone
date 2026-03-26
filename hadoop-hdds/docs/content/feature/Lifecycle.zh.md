---
title: "对象生命周期管理"
weight: 1
menu:
   main:
      parent: 特性
summary: 兼容 S3 的对象生命周期管理，支持自动过期清理对象
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

## 背景

在对象存储场景中，大量数据随着时间推移不再需要被访问或保留。手动清理这些过期数据既耗时又容易出错。对象生命周期管理提供了一种自动化的方式，让管理员可以在 Bucket 级别配置策略，使系统自动处理过期对象的清理工作。

Ozone 的对象生命周期管理功能参照 [AWS S3 Lifecycle Configuration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) 设计实现，通过 S3 Gateway 提供兼容的 API 接口。当前版本实现了 对象过期（Expiration） 动作，即根据对象的最后修改时间自动删除或移入回收站。

## 与 AWS S3 Lifecycle 的兼容性

Ozone 的生命周期管理以 AWS S3 Lifecycle 为参照进行设计，当前版本并未实现 S3 Lifecycle 的所有功能。以下是详细的兼容性对照。

### API 兼容性

| S3 API | 是否支持 | 说明 |
|--------|----------|------|
| `PutBucketLifecycleConfiguration` | 是 | 通过 S3 Gateway 的 `PUT /{bucket}?lifecycle` 设置 |
| `GetBucketLifecycleConfiguration` | 是 | 通过 S3 Gateway 的 `GET /{bucket}?lifecycle` 获取 |
| `DeleteBucketLifecycle` | 是 | 通过 S3 Gateway 的 `DELETE /{bucket}?lifecycle` 删除 |

### 生命周期动作（Actions）

| S3 Lifecycle 动作                    | 是否支持 | 说明 |
|------------------------------------|----------|------|
| Expiration（对象过期删除）                 | 是 | 支持 `Days` 和 `Date` 两种方式 |
| Transition（存储类转换）                  | 否 | Ozone 当前不支持类似 S3 的分层存储类转换 |
| NoncurrentVersionExpiration        | 否 | Ozone 的 Bucket 版本管理机制与 S3 不同 |
| NoncurrentVersionTransition        | 否 | 同上 |
| AbortIncompleteMultipartUpload [1] | 否 | 未实现自动清理未完成的分段上传 |
| ExpiredObjectDeleteMarker          | 否 | Ozone 不使用 S3 风格的删除标记机制 |

[1] Ozone 有单独的 Incomplete MultipartUpload 的清理服务 (MultipartUploadCleanupService)

### 过滤条件（Filter）

| S3 Filter 元素 | 是否支持 | 说明 |
|----------------|----------|------|
| Prefix | 是 | 支持顶层 Prefix 和 Filter 内的 Prefix |
| Tag | 是 | 支持按单个标签过滤 |
| And（Prefix + Tags） | 是 | 支持组合 Prefix 与多个 Tag 条件 |
| ObjectSizeGreaterThan | 否 | 不支持按对象大小下限过滤 |
| ObjectSizeLessThan | 否 | 不支持按对象大小上限过滤 |

### 其他差异

- Ozone 独有功能：Ozone 支持将过期对象移入回收站（`.Trash`）而非直接删除。
- Bucket Layout：Ozone 的 FSO (FILE_SYSTEM_OPTIMIZED) Bucket 支持基于目录树的递归评估，可以自动过期空目录。
- 管理操作：Ozone 提供了 `suspend` / `resume` 命令来动态控制生命周期服务的运行（S3 通过禁用规则的 `Status` 实现类似效果, Ozone 也支持次操作）, 可以直接停止所有生命周期服。

## 生命周期配置

Ozone 生命周期配置整体配置规则与 AWS 的 S3 Lifecycle 语义基本相同, 可以参考 AWS 的 S3 Lifecycle 来了解更多规则的细节.
### 整体结构

生命周期配置（Lifecycle Configuration）绑定在 Bucket 上，每个 Bucket 最多可设置一个生命周期配置，每个配置最多包含 1000 条规则（Rule）。

每条规则包含以下元素：

| 元素 | 说明 |
|------|------|
| ID | 规则的唯一标识，长度不超过 255 字符。如未指定则自动生成。 |
| Status | `Enabled` 或 `Disabled`，仅启用的规则会被执行。 |
| Filter / Prefix | 指定规则的作用范围，可以按对象名称前缀过滤。 |
| Expiration | 过期动作，通过 `Days` 或 `Date` 指定对象何时过期。 |

### 过期动作（Expiration）

过期动作支持两种方式：

- Days：对象自最后修改时间起经过指定天数后过期。必须为正整数, 不能为 0。
- Date：指定一个 UTC 时间点，所有在该时间点之前最后修改的对象被视为过期。

每条规则中最多指定一个 Expiration 动作，`Days` 和 `Date` 只能选其一。

Expiration 校验规则：

- `Days` 和 `Date` 必须且只能指定其中一个，不能同时指定，也不能都不指定。
- `Days` 必须为大于零的正整数。
- `Date` 必须符合 ISO 8601 格式，且必须同时包含时间和时区部分（不能省略）。合法示例：`2042-04-02T00:00:00Z`、`2042-04-02T00:00:00+00:00`。
- `Date` 转换为 UTC 后必须为午夜时刻（`00:00:00`），不允许指定非零的时分秒。
- `Date` 必须是相对于生命周期配置创建时间的未来时间，不能指定已过去的日期。

### 过滤器（Filter 与 Prefix）

规则可以通过以下方式指定作用范围：

- Prefix（顶层）：直接在 Rule 中设置 Prefix 字段，对所有匹配该前缀的对象生效。
- Filter：通过 Filter 元素指定，支持：
  - `Prefix`：按前缀过滤。
  - `Tag`：按单个标签过滤（Key/Value 对）。
  - `And`：组合 Prefix 与多个 Tag 条件。

通用校验规则：

- Prefix 与 Filter 不能同时使用，也不能都不指定。
- Prefix 设置为空字符串 `""` 表示规则对 Bucket 中的所有对象生效。
- Prefix 长度不能超过 1024 字节。
- Prefix 不能指向回收站目录（`.Trash` 或 `.Trash/` 开头的路径）。
- Filter 内部只能指定 Prefix、Tag、And 三者之一。
- Tag 的 Key 长度必须在 1 到 128 字节之间，Value 长度必须在 0 到 256 字节之间。
- And 操作符中，Tag 的 Key 不能重复。
- And 操作符必须包含 Tag；不允许只指定 Prefix 而不带 Tag。若只有 Tag 没有 Prefix，则 Tag 数量必须大于 1。

FSO Bucket 的额外校验规则：

对于 FILE_SYSTEM_OPTIMIZED (FSO) Bucket，Prefix 必须是规范化的合法路径（normalized and valid path）。具体要求如下：

- 不能以 `/` 开头。FSO Bucket 的 Prefix 是相对于 Bucket 根目录的路径，无需前导斜杠。
- 不能包含连续的斜杠 `//`。
- 路径组件中不能包含 `.`（当前目录）、`..`（父目录）或 `:`。

以下是合法与不合法 Prefix 的对照示例：

| Prefix | FSO Bucket 是否合法 | 原因 |
|--------|---------------------|------|
| `logs/` | 合法 | 规范化的目录前缀 |
| `data/2024/` | 合法 | 多级目录前缀 |
| `archive` | 合法 | 无斜杠的简单前缀 |
| `/logs/` | 不合法 | 不能以 `/` 开头，应使用 `logs/` |
| `data//backup/` | 不合法 | 包含连续斜杠 `//`，应使用 `data/backup/` |
| `data/../secret/` | 不合法 | 包含 `..`，不允许使用父目录引用 |
| `data/./logs/` | 不合法 | 包含 `.`，不允许使用当前目录引用 |
| `.Trash/` | 不合法 | 不能指向回收站目录 |

## S3 Gateway API

生命周期配置通过标准的 S3 API 操作进行管理，使用 `?lifecycle` 查询参数。

### 设置生命周期配置

以下为使用 `Days` 方式的示例：

```json
{
  "Rules": [
    {
      "ID": "expire-logs-after-30-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
```

使用 `Date` 方式的示例：

```json
{
  "Rules": [
    {
      "ID": "expire-temp-data",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "temp/"
      },
      "Expiration": {
        "Date": "2042-04-02T00:00:00Z"
      }
    }
  ]
}
```

使用 `And` 组合 Prefix 与 Tag 进行过滤的示例：

```json
{
  "Rules": [
    {
      "ID": "expire-tagged-objects",
      "Status": "Enabled",
      "Filter": {
        "And": {
          "Prefix": "data/",
          "Tags": [
            {
              "Key": "environment",
              "Value": "dev"
            }
          ]
        }
      },
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

使用 AWS CLI 设置生命周期配置：

```shell
aws s3api put-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9878 \
  --lifecycle-configuration file://lifecycle.json
```

### 获取生命周期配置

GET `/{bucket}?lifecycle`

```shell
aws s3api get-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9878
```

### 删除生命周期配置

DELETE `/{bucket}?lifecycle`

```shell
aws s3api delete-bucket-lifecycle \
  --bucket mybucket \
  --endpoint-url http://localhost:9878
```

## Bucket Layout 支持

Ozone 支持三种 Bucket Layout：OBJECT_STORE (OBS)、LEGACY 和 FILE_SYSTEM_OPTIMIZED (FSO)，生命周期管理在不同 Layout 下的行为有所差异。

### OBS 和 LEGACY Bucket

对于 OBS 和 LEGACY Bucket，生命周期服务直接遍历 Key Table，根据 Key 名称进行前缀匹配。匹配成功且满足过期条件的对象会被直接删除（OBS Bucket 不支持回收站）或移入回收站（LEGACY Bucket）。

### FSO Bucket

对于 FSO Bucket，生命周期服务基于目录树进行递归评估：

1. 解析前缀中的目录路径，从目录表中找到对应的目录。
2. 以深度优先方式遍历目录树，逐层评估文件和子目录。
3. 如果一个目录下的所有文件和子目录都已过期，则该目录本身也被标记为过期。

Prefix 的语义差异：

| Prefix | OBS/LEGACY 行为 | FSO 行为 |
|--------|-----------------|----------|
| `""` (空) | 匹配所有对象 | 匹配所有对象和目录 |
| `key` | 匹配以 `key` 开头的所有 Key | 匹配以 `key` 开头的文件和目录 |
| `dir/` | 匹配以 `dir/` 开头的所有 Key | 匹配 `dir` 目录下的文件和子目录，不包括 `dir` 本身 |
| `dir1/dir2` | 匹配以 `dir1/dir2` 开头的所有 Key | 匹配 `dir1` 下以 `dir2` 开头的文件和目录 |

<div class="alert alert-warning" role="alert">

对于 FSO Bucket，目录的修改时间（ModificationTime）不会因其子文件或子目录的变化而更新。因此在配置前缀时，如果只需过期目录下的内容而不想意外删除目录本身，请在前缀末尾加上 `/`。例如，使用 `data/` 而非 `data`。

</div>

## 回收站集成

默认情况下，生命周期服务会将过期的对象移入回收站（`.Trash` 目录），而不是直接删除。这为误操作提供了一层保护。

- 当 `ozone.lifecycle.service.move.to.trash.enabled` 设为 `true`（默认）时，过期对象被移入 `.Trash/<owner>/Current/` 路径下。
- OBS Bucket 不支持回收站，过期对象会被直接删除。
- 设为 `false` 时，所有过期对象将被直接删除。

移入回收站的对象仍遵循 Ozone 的回收站清理策略，到期后会被最终删除。

## 配置

生命周期服务默认处于禁用状态，需要在 `ozone-site.xml` 中显式启用。

```XML
<property>
   <name>ozone.lifecycle.service.enabled</name>
   <value>true</value>
   <description>启用对象生命周期管理服务。</description>
</property>
```

以下是所有相关的配置项：

| 配置项 | 说明                                         |
|--------|--------------------------------------------|
| `ozone.lifecycle.service.enabled` | 是否启用生命周期管理服务。                              |
| `ozone.lifecycle.service.interval` | 生命周期管理服务的扫描间隔。                             |
| `ozone.lifecycle.service.timeout` | 生命周期评估任务的超时阈值。该配置不会中断正在执行的任务，仅当单个 Bucket 的评估任务实际执行时间超过该值时，在任务结束后打印 WARN 级别日志以便运维排查。 |
| `ozone.lifecycle.service.workers` | 生命周期管理服务的工作线程数，必须大于 0。每个 Bucket 由一个线程负责检查和处理，最多同时处理的 Bucket 数等于该值，其余 Bucket 将排队等待。设置过高会增加 OM 上并发 RocksDB 读写和 Ratis 请求的压力，可能影响集群性能。 |
| `ozone.lifecycle.service.delete.batch-size` | 单次批量删除请求中包含的最大对象数。每批 Key 会封装为一次 Ratis 删除请求提交到 OM，过大的批次会增大单次 Ratis 日志条目的大小并占用更多内存，不建议超过 1000。 |
| `ozone.lifecycle.service.move.to.trash.enabled` | 启用时将过期对象移入回收站，禁用时直接删除。OBS Bucket 不适用。      |
| `ozone.lifecycle.service.delete.cached.directory.max-count` | FSO Bucket 递归评估时内存中缓存的最大目录数，超出此限制时本次评估将中止。 |

## 管理操作

Ozone 提供了管理命令来查看和控制生命周期服务的运行状态。

### 查看服务状态

```shell
ozone admin om lifecycle status [-id=<omServiceId>] [-host=<omHost>]
```

### 暂停服务

```shell
ozone admin om lifecycle suspend [-id=<omServiceId>] [-host=<omHost>]
```

暂停后，服务不会启动新的评估任务。已在运行中的任务会在检测到暂停状态后停止。

<div class="alert alert-warning" role="alert">

暂停操作仅对当前 OM 运行期有效。OM 重启后，服务将根据 `ozone.lifecycle.service.enabled` 的配置值恢复运行。

</div>

### 恢复服务

```shell
ozone admin om lifecycle resume [-id=<omServiceId>] [-host=<omHost>]
```

## 注意事项

- 生命周期配置目前仅能通过 S3 API 进行设置。如果需要为 Bucket 配置生命周期规则，必须启动 S3 Gateway（S3G）服务，并通过 S3 接口访问对应的 Bucket。
- 生命周期服务默认禁用，需要设置 `ozone.lifecycle.service.enabled=true` 才能启用。
- 在 OM HA 模式下，只有 Leader OM 会执行生命周期评估任务。
- 对于 FSO Bucket，目录只有在其所有子文件和子目录都已过期的情况下才会被标记为过期并删除。
- 对于 FSO Bucket 使用 Prefix 时，如果 Prefix 不以 `/` 结尾，则会同时匹配名称相同的目录及以该前缀开头的同级目录（如 `dir` 同时匹配 `dir` 和 `dir1`）。
- 当 OM RocksDB 中存在无效的生命周期配置时，服务会跳过该配置并记录错误日志，不会影响其他 Bucket 的处理。

### OM Leader 切换对生命周期服务的影响

在 OM HA 部署中，生命周期服务仅在 Leader OM 上运行。当执行 Transfer Leader 操作时：

1. 旧 Leader 上正在运行的生命周期评估任务会被中断。
2. 新 Leader 当选后，会重新从头启动生命周期服务，已经评估过的 Bucket 不会被跳过，任务将从第一个 Bucket 重新开始。
3. 如果 `ozone.lifecycle.service.run.interval` 配置为较大值（例如默认的 `24h`），Transfer Leader 后新一轮任务可能要到第二天才会被调度执行。

因此，在频繁进行 Leader 切换的场景下，建议关注生命周期服务的实际执行情况，确保过期对象能被及时清理。

### 大批量 Key 过期对元数据性能的影响

如果某个 Bucket 在短时间内（例如 24 小时以内）有大量 Key 过期并被删除（例如超过 1 亿个 Key），可能会导致 RocksDB 中产生大量的墓碑（tombstone）记录，从而引发以下问题：

- 该 Bucket 上的元数据操作延迟显著增高，尤其是 `list` 等需要遍历 RocksDB 的操作。
- 读取性能下降，因为 RocksDB 需要在查询时跳过大量已删除的墓碑记录。

缓解措施：

1. 启用自动压缩服务（推荐）：设置 `ozone.om.compaction.service.enabled=true`，并确保 `ozone.om.compaction.service.columnfamilies` 中包含 `keyTable,fileTable,directoryTable`（默认值已包含）。该服务默认每 6 小时执行一次压缩，可通过 `ozone.om.compaction.service.run.interval` 调整间隔。

2. 手动压缩：使用 `ozone repair om compact` 命令对受影响的 Column Family 执行手动压缩：

```bash
ozone repair om compact --cf keyTable
ozone repair om compact --cf fileTable
ozone repair om compact --cf directoryTable
```

如果是 OM HA 环境，可以通过 `--service-id` 和 `--node-id` 指定目标 OM 节点：

```bash
ozone repair om compact --cf keyTable --service-id omServiceId --node-id om1
```

压缩操作是异步执行的，可以在对应 OM 节点的日志中查看完成状态。

## 参考文档

 * [设计文档]({{< ref path="design/s3-object-lifecycle-management.md" lang="en">}})
 * [AWS S3 Lifecycle 概览](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
 * [AWS S3 对象过期](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)
 * [AWS S3 设置 Lifecycle 配置](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html)
 * [AWS S3 Lifecycle 配置示例](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)
