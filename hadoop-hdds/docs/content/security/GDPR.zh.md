---
title: "Ozone 中的 GDPR"
date: "2019-09-17"
weight: 3
summary: Ozone 中的 GDPR
menu:
   main:
      parent: 安全
icon: user
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

<div class="alert alert-warning">

注意：本页面翻译的信息可能滞后，最新的信息请参看英文版的相关页面。

</div>

在 Ozone 中遵守 GDPR 规范非常简单，只需要在创建桶时指定 `--enforcegdpr=true` 或  `-g=true` 参数，这样创建出的桶都是符合 GDPR 规范的，当然，在桶中创建的键也都自动符合。

GDPR 只能在新桶上应用，对于已有的桶，要想使它符合 GDPR 规范，只能创建一个符合 GDPR 规范的新桶，然后把旧桶中的数据拷贝到新桶。

创建符合 GDPR 规范的桶的示例：

`ozone sh bucket create --enforcegdpr=true /hive/jan`

`ozone sh bucket create -g=true /hive/jan`

如果你想创建普通的桶，省略 `--enforcegdpr` 和 `-g` 参数即可。