---
title: "Setup"
weight: 1
menu:
   main:
      parent: "Multi-Tenancy"
summary: Preparing Ozone clusters to enable Multi-Tenancy feature
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

Steps to enable Multi-Tenancy feature in Ozone clusters. 


### Setting up Multi-Tenancy with Apache Ranger (For production)

First make sure ACL is enabled, and `RangerOzoneAuthorizer` is the effective ACL authorizer implementation in-use for Ozone.
If that is not the case, [follow this]({{< ref "security/SecurityWithRanger.md" >}}). 

Then simply add the following configs to `ozone-site.xml`:

```xml
<property>
	<name>ozone.om.ranger.https-address</name>
	<value>https://RANGER_HOSTNAME:6182</value>
</property>
<property>
	<name>ozone.om.ranger.https.admin.api.user</name>
	<value>RANGER_ADMIN_USERNAME</value>
</property>
<property>
	<name>ozone.om.ranger.https.admin.api.passwd</name>
	<value>RANGER_ADMIN_PASSWORD</value>
</property>
```

Finally restart all OzoneManagers to apply the new configs.

Now you can follow the [Multi-Tenancy CLI command guide]({{< ref "interface/Tenant.md" >}}) to try the commands. 


### Try in a Docker Compose cluster (For developers)

Developers are encouraged to try out the CLI commands inside the `./compose/ozonesecure/docker-compose.yaml` cluster environment that ships with Ozone.

The Docker Compose cluster has Kerberos and security pre-configured.
But note it differs from an actual production cluster that Ranger has been replaced with a mock server. And OzoneManager does **not** use Ranger for ACL.

Because the mock server does not mock all Ranger endpoints, some operations that works for a real Ranger deployment will not work by default. e.g. assigning users to a tenant other than `tenantone`.
But one can add new custom endpoints in `./compose/ozonesecure/mockserverInitialization.json` as needed.

To launch the Docker Compose cluster locally, from Ozone distribution root:

```shell
cd compose/ozonesecure
docker-compose up -d --scale datanode=3
docker-compose exec scm bash
```

It might be necessary to run the following command first before testing the tenant commands in the `compose/ozonesecure` Docker environment
in order to workaround a Docker-specific DNS issue when first contacting Ranger.

```shell
bash-4.2$ curl -k https://ranger:6182/
{}
```

Then all subsequent requests to Ranger (mock server) should work as expected.

Otherwise you might see such DNS error:

```shell
bash-4.2$ ozone tenant create tenantone
2022-02-16 00:00:00,000 [main] INFO rpc.RpcClient: Creating Tenant: 'tenantone', with new volume: 'tenantone'
INTERNAL_ERROR No subject alternative DNS name matching ranger found.
```


Operations requiring Ozone cluster administrator privilege are run as `om/om` user:

```shell
kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM
```
