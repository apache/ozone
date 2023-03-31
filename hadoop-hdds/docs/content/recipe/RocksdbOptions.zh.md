---
title: 自定义 Rocksdb 参数
linktitle: Rocksdb Options
summary: OM/SCM/DN 使用的自定义的 Rocksdb 参数
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

Ozone 使用的 rocksdb 参数大多数为默认参数。本页介绍如何通过配置文件来使用自定义的 rocksdb 参数。

## 使用 Rocksdb 配置文件

将对应的配置文件 `file` 放在 OZONE 的配置目录 `$OZONE_DIR/etc/hadoop` 下，然后重启服务即可。以下列出了 OM/SCM/DN 默认加载的 rocksdb 配置文件名。

服务 | 文件名     
----|---------
OM  | om.db.ini 
SCM | scm.db.ini 
DN  | dn-crl.db.ini

Rocksdb 配置文件格式请参考：[rocksdb_option_file_example.ini](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini)

## 验证配置文件生效

查看 rocksdb 目录下的 `OPTIONS-xxxxxx` 相应配置项是否符合预期。例如，OM 的配置文件 `$OZONE_DIR/etc/hadoop/om.db.ini` 如下：(修改 `max_background_flushes=4`)

```ini
[DBOptions]
   max_background_flushes=4
   compaction_readahead_size=4194304
   bytes_per_sync=1048576
   max_background_compactions=4
   max_background_jobs=48
   create_missing_column_families=true
   create_if_missing=true

[CFOptions "default"]

[TableOptions/BlockBasedTable "default"]
```
重启 OM 后，查看 OM 的 rocksdb 目录（`${DBDATA}/om.db`）下 `OPTIONS-xxxxxx` 内容， `max_background_flushes=4` 表明配置生效。
