---
title: "Setup"
weight: 11
menu:
   main:
      parent: "S3 Multi-Tenancy"
summary: Preparing Ozone clusters to enable Multi-Tenancy feature
hideFromSectionPage: true
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

Steps to enable S3 Multi-Tenancy feature in Ozone clusters.


## Setting up S3 Multi-Tenancy for production

### Secure the cluster

Follow [this guide]({{< ref "security/SecureOzone.md" >}}) to Kerberize (secure) the cluster if the cluster is not Kerberized yet.

### Set up S3 Gateway

Follow [this guide]({{< ref "interface/S3.md" >}}) the cluster to set up at least one S3 Gateway if the cluster doesn't have a S3 Gateway yet.

### Set up Apache Ranger

First make sure ACL is enabled, and `RangerOzoneAuthorizer` is the effective ACL authorizer implementation in-use for Ozone.
If that is not the case, [follow this]({{< ref "security/SecurityWithRanger.md" >}}). 

Add the following configs to all Ozone Managers' `ozone-site.xml`:

```xml
<property>
	<name>ozone.om.multitenancy.enabled</name>
	<value>true</value>
</property>
<property>
	<name>ozone.om.ranger.https-address</name>
	<value>https://RANGER_HOST:6182</value>
</property>
<property>
	<name>ozone.om.ranger.service</name>
	<value>RANGER_OZONE_SERVICE_NAME</value>
</property>
```

The value of `ozone.om.ranger.service` should match Ozone's "Service Name" as configured under "Service Manager" page in Ranger Admin Server Web UI. e.g. `cm_ozone`.

To authenticate to Apache Ranger Admin Server, `ozone.om.kerberos.principal` and `ozone.om.kerberos.keytab.file` will be picked up from the existing configs used for Kerberos security setup.

- Note: Make sure the user behind the Kerberos principal (e.g. `om`) has Admin privilege in Ranger, otherwise some functionality will break.
This is a limitation of Apache Ranger at the moment.
e.g. background sync won't be able to get the policyVersion to function properly, and create/update/delete Ranger role will fail.

In addition, if one wants to test Ranger with user name and clear text password login (not recommended in production), add the following configs to Ozone Manager:

```xml
<property>
	<name>ozone.om.ranger.https.admin.api.user</name>
	<value>RANGER_ADMIN_USERNAME</value>
</property>
<property>
	<name>ozone.om.ranger.https.admin.api.passwd</name>
	<value>RANGER_ADMIN_PASSWORD</value>
</property>
```

Note if both Ranger user name and password are configured, it will be chosen over the (default and recommended) Kerberos keytab authentication method.

Finally restart all OzoneManagers to apply the new configs.

Now you can follow the [Multi-Tenancy CLI command]({{< ref "feature/S3-Tenant-Commands.md" >}}) guide to try the commands. 


## Try in a Docker Compose cluster (For developers)

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

Operations requiring Ozone cluster administrator privilege should be run as `om` user:

```shell
kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM
```
