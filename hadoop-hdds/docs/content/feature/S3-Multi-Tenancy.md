---
title: "S3 Multi-Tenancy"
weight: 10
menu:
   main:
      parent: Features
summary: Ozone Multi-Tenancy that allows multiple tenants to share the same Ozone cluster. Compatible with S3 API.
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

Before Ozone multi-tenancy, all S3 access to Ozone (via [S3 Gateway]({{< ref "interface/S3.md" >}})) are
confined to a **single** designated S3 volume (that is volume `s3v`, by default).

Ozone multi-tenancy allows **multiple** S3-accessible volumes to be created.
Each volume can be managed separately by their own tenant admins via CLI for user operations, and via Apache Ranger for access control.

The concept **tenant** is introduced to Ozone by multi-tenancy.
Each tenant has its own designated volume.
Each user assigned to a tenant will be able to access the associated volume with an _Access ID & Secret Key_ pair
generated when an Ozone cluster admin or tenant admin assigns the user to the tenant using CLI.

> This multi-tenant feature allows Ozone resources to be compartmentalized in different isolation zones (tenants).

> This multi-tenant support will also allow users to access Ozone volumes over AWS S3 APIs (without any modifications to the APIs).

## Basics

1. Initial tenant creation has to be done by an Ozone cluster admin under the CLI.
2. The Ozone cluster admin will have to assign the first user of a tenant. Once assigned, an _Access ID & Secret Key pair_ (key pair) will be generated for that user for access via S3 Gateway. 
   - The key pair serves to authenticate the end user to the Ozone Manager (via client requests from S3 Gateway). Tenant volume is selected based on the Access ID.
   - After successful authentication, Ozone Manager uses the underlying user name (not the Access ID) to identify the user. The user name is used to perform authorization checks in Apache Ranger.
   - A key pair is tied to the user name it is assigned to. If a user is assigned key pairs in multiple tenants, all key pairs point to the same user name internally in Ozone Manager.
   - A user can be only assigned one key pair in a same tenant. Ozone Manager rejects the tenant user assign request if a user is already assigned to the same tenant (i.e. when the user has already been assigned an Access ID in this tenant).
   - One user can be assigned to multiple tenants. The user will have a different key pair to access each tenant. For instance, `testuser` could use `tenantone$testuser` to access `tenantone` buckets, and use `tenanttwo$testuser` to access `tenanttwo` buckets via S3 Gateway.
     - A bucket link can be set up if cross-tenant (cross-volume) access is desired. See **Creating bucket links** section in [Tenant commands]({{< ref "feature/S3-Tenant-Commands.md" >}}).
3. The Ozone cluster admin can then assign tenant admin roles to that user.
4. Tenant admin are able to assign new users to the tenant.
   - They can even assign new tenant admins in their tenant, if they are delegated tenant admins, which is the default. See **Assign a user as a tenant admin** section in [Tenant commands]({{< ref "feature/S3-Tenant-Commands.md" >}}).
   - Note that tenant admins still need to use Ozone tenant CLI to assign new users to the tenant.
     - Once tenant admins get the Kerberos TGT (via `kinit`), they can run `user assign` command to assign new users. Ozone Manager will recognize that they are the tenant admins and allow the user to do so in their tenants.
5. After that, users can use any S3-compatible client (awscli, Python boto3 library, etc.) to access the buckets in the tenant volume via S3 Gateway using the generated key pairs.


## Access Control

Ozone multi-tenancy relies on [Apache Ranger]({{< ref "security/SecurityWithRanger.md" >}}) to enforce access control to resources.

See [Access Control]({{< ref "feature/S3-Multi-Tenancy-Access-Control.md" >}}) for more information.

## Setup

See [Multi-Tenancy Setup]({{< ref "S3-Multi-Tenancy-Setup.md" >}}).

## Usage

See [Tenant Subcommands]({{< ref "feature/S3-Tenant-Commands.md" >}}).

## References

 * For developers: check out the upstream jira [HDDS-4944](https://issues.apache.org/jira/browse/HDDS-4944) and the attached design docs.
