---
title: HttpFS Gateway
weight: 4
menu:
main:
parent: "Client Interfaces"
summary: Ozone HttpFS is a WebHDFS compatible interface implementation, as a separate role it provides an easy integration with Ozone.
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

Ozone HttpFS can be used to integrate Ozone with other tools via REST API.

## Introduction

Ozone HttpFS is forked from the HDFS HttpFS endpoint implementation ([HDDS-5448](https://issues.apache.org/jira/browse/HDDS-5448)). It is added as a separate role to Ozone, like S3G. 

HttpFS is a service that provides a REST HTTP gateway supporting File System operations (read and write). It is interoperable with the **webhdfs** REST HTTP API.

HttpFS can be used to access data on an Ozone cluster behind of a firewall (the HttpFS service acts as a gateway and is the only system that is allowed to cross the firewall into the cluster).

HttpFS can be used to access data in Ozone using HTTP utilities (such as curl and wget) and HTTP libraries Perl from other languages than Java.

The **webhdfs** client FileSystem implementation can be used to access HttpFS using the Ozone filesystem command line tool (`ozone fs`) as well as from Java applications using the Hadoop FileSystem Java API.

HttpFS has built-in security supporting Hadoop pseudo authentication and Kerberos SPNEGO and other pluggable authentication mechanisms. It also provides Hadoop proxy user support.


## Getting started

HttpFS service itself is a Jetty based web-application that uses the Hadoop FileSystem API to talk to the cluster, it is a separate service which provides access to Ozone via a REST API. It should be started additionally to the regular Ozone components.

You can start a docker based cluster, including the HttpFS gateway from the release package.

Go to the `compose/ozone` directory and start the server:

```bash
docker-compose up -d --scale datanode=3
```

You can/should find now the HttpFS gateway in docker with the name `ozone_httpfs`.
HttpFS HTTP web-service API calls are HTTP REST calls that map to an Ozone file system operation. For example, using the `curl` Unix command.

E.g. in the docker cluster you can execute commands like these:

* `curl -i -X PUT "http://httpfs:14000/webhdfs/v1/vol1?op=MKDIRS&user.name=hdfs"` creates a volume called `vol1`.


* `$ curl 'http://httpfs-host:14000/webhdfs/v1/user/foo/README.txt?op=OPEN&user.name=foo'` returns the contents of the `/user/foo/README.txt` key.


## Supported operations

These are the WebHDFS REST API operations that are supported/unsupported in Ozone.

### File and Directory Operations

Operation                       |      Support
--------------------------------|---------------------
Create and Write to a File      | supported
Append to a File                | not implemented in Ozone FileSystem API
Concat File(s)                  | not implemented in Ozone FileSystem API
Open and Read a File            | supported
Make a Directory                | supported
Create a Symbolic Link          | unsupported
Rename a File/Directory         | unsupported
Delete a File/Directory         | supported
Truncate a File                 | not implemented in Ozone FileSystem API
Status of a File/Directory      | supported
List a Directory                | supported
List a File                     | supported
Iteratively List a Directory    | supported


### Other File System Operations

Operation                             |      Support
--------------------------------------|---------------------
Get Content Summary of a Directory    | supported
Get Quota Usage of a Directory        | supported
Set Quota                             | not implemented in Ozone
Set Quota By Storage Type             | not implemented in Ozone
Get File Checksum                     | not implemented in Ozone
Get Home Directory                    | supported
Get Trash Root                        | supported
Set Permission                        | supported
Set Owner                             | supported
Set Replication Factor                | supported
Set Access or Modification Time       | supported
Modify ACL Entries                    | not implemented in Ozone FileSystem API
Remove ACL Entries                    | not implemented in Ozone FileSystem API
Remove Default ACL                    | not implemented in Ozone FileSystem API
Remove ACL                            | not implemented in Ozone FileSystem API
Set ACL                               | not implemented in Ozone FileSystem API
Get ACL Status                        | not implemented in Ozone FileSystem API
Check access                          | not implemented in Ozone FileSystem API



## Hadoop user and developer documentation about HttpFS

* [HttpFS Server Setup](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/ServerSetup.html)

* [Using HTTP Tools](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/ServerSetup.html)