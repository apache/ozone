---
title: Ozone Overview
menu: main
weight: -10
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Apache Hadoop Ozone

Ozone is a scalable, distributed object store for Hadoop.  Applications like
Apache Spark, Hive and YARN, can run against Ozone without any
modifications. Ozone comes with a [Java client library]({{< ref "JavaApi.md"
>}}) and a  [command line interface] ({{< ref "CommandShell.md#shell" >}})  which makes it easy to use Ozone. This client library supports both RPC and REST protocols.

Ozone consists of volumes, buckets, and Keys.

* Volumes are similar to user accounts. Only administrators can create or delete volumes.
* Buckets are similar to directories. A bucket can contain any number of keys,  but buckets cannot contain other buckets.
* Keys are similar to files. A bucket can contain any number of keys.



<a href="{{< ref "RunningViaDocker.md" >}}"><button class="btn btn-danger btn-lg">Getting started</button></a>

