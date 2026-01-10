---
title: HttpFS Gateway
weight: 7
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

Ozone HttpFS is forked from the HDFS HttpFS endpoint implementation ([HDDS-5448](https://issues.apache.org/jira/browse/HDDS-5448)). Ozone HttpFS is intended to be added optionally as a role in an Ozone cluster, similar to [S3 Gateway]({{< ref "design/s3gateway.md" >}}).

HttpFS is a service that provides a REST HTTP gateway supporting File System operations (read and write). It is interoperable with the **webhdfs** REST HTTP API.

HttpFS can be used to access data on an Ozone cluster behind of a firewall. For example, the HttpFS service acts as a gateway and is the only system that is allowed to cross the firewall into the cluster.

HttpFS can be used to access data in Ozone using HTTP utilities (such as curl and wget) and HTTP libraries Perl from other languages than Java.

The **webhdfs** client FileSystem implementation can be used to access HttpFS using the Ozone filesystem command line tool (`ozone fs`) as well as from Java applications using the Hadoop FileSystem Java API.

HttpFS has built-in security supporting Hadoop pseudo authentication and Kerberos SPNEGO and other pluggable authentication mechanisms. It also provides Hadoop proxy user support.


## Getting started

HttpFS service itself is a Jetty based web-application that uses the Hadoop FileSystem API to talk to the cluster, it is a separate service which provides access to Ozone via a REST APIs. It should be started in addition to other regular Ozone components.

To try it out, follow the instructions from the link below to start the Ozone cluster with Docker Compose. 

https://ozone.apache.org/docs/edge/start/startfromdockerhub.html

```bash
docker compose up -d --scale datanode=3
```

You can/should find now the HttpFS gateway in docker with the name like `ozone_httpfs`,
and it can be accessed through `localhost:14000`.
HttpFS HTTP web-service API calls are HTTP REST calls that map to an Ozone file system operation.

Here's some example usage:

### Create a volume

```bash
# creates a volume called `volume1`.
curl -i -X PUT "http://localhost:14000/webhdfs/v1/volume1?op=MKDIRS&user.name=hdfs"
```

Example Output:

```bash
HTTP/1.1 200 OK
Date: Sat, 18 Oct 2025 07:51:21 GMT
Cache-Control: no-cache
Expires: Sat, 18 Oct 2025 07:51:21 GMT
Pragma: no-cache
Content-Type: application/json
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
Set-Cookie: hadoop.auth="u=hdfs&p=hdfs&t=simple-dt&e=1760809881100&s=OCdVOi8eyMguFySkmEJxm5EkRfj6NbAM9agi5Gue1Iw="; Path=/; HttpOnly
Content-Length: 17

{"boolean":true}
```

### Create a bucket

```bash
# creates a bucket called `bucket1`.
curl -i -X PUT "http://localhost:14000/webhdfs/v1/volume1/bucket1?op=MKDIRS&user.name=hdfs"
```

Example Output:

```bash
HTTP/1.1 200 OK
Date: Sat, 18 Oct 2025 07:52:06 GMT
Cache-Control: no-cache
Expires: Sat, 18 Oct 2025 07:52:06 GMT
Pragma: no-cache
Content-Type: application/json
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
Set-Cookie: hadoop.auth="u=hdfs&p=hdfs&t=simple-dt&e=1760809926682&s=yvOaeaRCVJZ+z+nZQ/rM/Y01pzEmS9Pe2mE9f0b+TWw="; Path=/; HttpOnly
Content-Length: 17

{"boolean":true}
```

### Upload a file

```bash
echo "hello" >> ./README.txt
curl -i -X PUT "http://localhost:14000/webhdfs/v1/volume1/bucket1/user/foo/README.txt?op=CREATE&data=true&user.name=hdfs" -T ./README.txt -H "Content-Type: application/octet-stream" 
```

Example Output:

```bash
HTTP/1.1 100 Continue

HTTP/1.1 201 Created
Date: Sat, 18 Oct 2025 08:33:33 GMT
Cache-Control: no-cache
Expires: Sat, 18 Oct 2025 08:33:33 GMT
Pragma: no-cache
X-Content-Type-Options: nosniff
X-XSS-Protection: 1; mode=block
Set-Cookie: hadoop.auth="u=hdfs&p=hdfs&t=simple-dt&e=1760812413286&s=09t7xKu/p/fjCJiQNL3bvW/Q7mTw28IbeNqDGlslZ6w="; Path=/; HttpOnly
Location: http://localhost:14000/webhdfs/v1/volume1/bucket1/user/foo/README.txt
Content-Type: application/json
Content-Length: 84

{"Location":"http://localhost:14000/webhdfs/v1/volume1/bucket1/user/foo/README.txt"}
```

### Read the file content

```bash
# returns the content of the key `/user/foo/README.txt`.
curl 'http://localhost:14000/webhdfs/v1/volume1/bucket1/user/foo/README.txt?op=OPEN&user.name=foo'
hello
```

## Supported operations

Here are the tables of WebHDFS REST APIs and their state of support in Ozone.

### File and Directory Operations

Operation                       |      Support
--------------------------------|---------------------
Create and Write to a File      | supported
Append to a File                | not implemented in Ozone
Concat File(s)                  | not implemented in Ozone
Open and Read a File            | supported
Make a Directory                | supported
Create a Symbolic Link          | not implemented in Ozone
Rename a File/Directory         | supported (with limitations)
Delete a File/Directory         | supported
Truncate a File                 | not implemented in Ozone
Status of a File/Directory      | supported
List a Directory                | supported
List a File                     | supported
Iteratively List a Directory    | unsupported


### Other File System Operations

Operation                             |      Support
--------------------------------------|---------------------
Get Content Summary of a Directory    | supported
Get Quota Usage of a Directory        | supported
Set Quota                             | not implemented in Ozone FileSystem API
Set Quota By Storage Type             | not implemented in Ozone
Get File Checksum                     | unsupported (to be fixed)
Get Home Directory                    | unsupported (to be fixed)
Get Trash Root                        | unsupported
Set Permission                        | not implemented in Ozone FileSystem API
Set Owner                             | not implemented in Ozone FileSystem API
Set Replication Factor                | not implemented in Ozone FileSystem API
Set Access or Modification Time       | not implemented in Ozone FileSystem API
Modify ACL Entries                    | not implemented in Ozone FileSystem API
Remove ACL Entries                    | not implemented in Ozone FileSystem API
Remove Default ACL                    | not implemented in Ozone FileSystem API
Remove ACL                            | not implemented in Ozone FileSystem API
Set ACL                               | not implemented in Ozone FileSystem API
Get ACL Status                        | not implemented in Ozone FileSystem API
Check access                          | not implemented in Ozone FileSystem API

## Proxy User Configuration

HttpFS supports proxy user (user impersonation) functionality, which allows a user to perform operations on behalf of another user. This is useful when HttpFS is used as a gateway and you want to allow certain users to impersonate other users.

To configure proxy users, you need to add the following properties to `httpfs-site.xml`:

### Configuration Properties

For each user that should be allowed to perform impersonation, you need to configure two properties:

1. **`httpfs.proxyuser.#USER#.hosts`**: List of hosts from which the user is allowed to perform impersonation operations.
2. **`httpfs.proxyuser.#USER#.groups`**: List of groups whose users can be impersonated by the specified user.

Replace `#USER#` with the actual username of the user who should be allowed to perform impersonation.

### Example Configuration

```xml
<property>
  <name>httpfs.proxyuser.knoxuser.hosts</name>
  <value>*</value>
  <description>
    List of hosts the 'knoxuser' user is allowed to perform 'doAs'
    operations.
    
    The value can be the '*' wildcard or a comma-separated list of hostnames.
    
    For multiple users, copy this property and replace the user name
    in the property name.
  </description>
</property>

<property>
  <name>httpfs.proxyuser.knoxuser.groups</name>
  <value>*</value>
  <description>
    List of groups the 'knoxuser' user is allowed to impersonate users
    from to perform 'doAs' operations.
    
    The value can be the '*' wildcard or a comma-separated list of group names.
    
    For multiple users, copy this property and replace the user name
    in the property name.
  </description>
</property>
```

In this example, the user `knoxuser` is allowed to impersonate any user from any host. For production environments, it's recommended to restrict these values to specific hosts and groups instead of using the wildcard `*`.

### Troubleshooting

If you encounter an error like:
```
User: user/host@REALM is not allowed to impersonate user01
```

This indicates that the proxy user configuration is missing or incorrect. Ensure that:
1. The `httpfs.proxyuser.#USER#.hosts` property is set with appropriate host values
2. The `httpfs.proxyuser.#USER#.groups` property is set with appropriate group values
3. The HttpFS service has been restarted after configuration changes

## Hadoop user and developer documentation about HttpFS

* [HttpFS Server Setup](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/ServerSetup.html)

* [Using HTTP Tools](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/ServerSetup.html)
