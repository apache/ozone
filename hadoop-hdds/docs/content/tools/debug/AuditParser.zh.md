---
title: "审计解析器"
date: 2018-12-17
summary: 审计解析器工具用来查看 Ozone 的审计日志。
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

审计解析器工具用于查询 Ozone 的审计日志。它会在指定的路径下创建一个 sqlite 数据库，若数据库已存在则不再创建。

这个数据库只包含了一个名为 `audit` 的表，定义如下：

CREATE TABLE IF NOT EXISTS audit (
datetime text,
level varchar(7),
logger varchar(7),
user text,
ip text,
op text,
params text,
result varchar(7),
exception text,
UNIQUE(datetime,level,logger,user,ip,op,params,result))

用法：
{{< highlight bash >}}
ozone debug auditparser <数据库文件的路径> [命令] [参数]
{{< /highlight >}}

将审计日志加载到数据库：
{{< highlight bash >}}
ozone debug auditparser <数据库文件的路径> load <审计日志的路径>
{{< /highlight >}}
Load 命令会创建如上所述的审计表。

运行一个自定义的只读查询：
{{< highlight bash >}}
ozone debug auditparser <数据库文件的路径> query <双引号括起来的 select 查询>
{{< /highlight >}}

审计解析起自带了一些模板（最常用的查询）

运行模板查询：
{{< highlight bash >}}
ozone debug auditparser <数据库文件的路径 template <模板名称>
{{< /highlight >}}

Ozone 提供了以下模板：

|模板名称|描述|SQL|
|----------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
|top5users|Top 5 users|select user,count(*) as total from audit group by user order by total DESC limit 5|
|top5cmds|Top 5 commands|select op,count(*) as total from audit group by op order by total DESC limit 5|
|top5activetimebyseconds|Top 5 active times, grouped by seconds|select substr(datetime,1,charindex(',',datetime)-1) as dt,count(*) as thecount from audit group by dt order by thecount DESC limit 5|
