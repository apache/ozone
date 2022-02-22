---
title: "Multi-Tenancy"
weight: 1
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
2. The Ozone cluster admin will have to assign the first user of a tenant. Once assigned, an _Access ID & Secret Key_ pair will be generated for that user for access via S3 Gateway. 
3. The Ozone cluster admin can then assign tenant admin roles to that user.
4. Tenant admin are able to assign new users to the tenant. 
   - They can even assign new tenant admins in their tenant, if they are delegated tenant admins, which is the default. See the usage below for more details.
   - Note that tenant admins still need to use Ozone tenant CLI to assign new users to the tenant.
     - On a Kerberized cluster, once tenant admins get the Kerberos TGT (via `kinit`), they can run `user assign` command to assign new users. OzoneManager will recognize that they are the tenant admins and allow the user to do so in their tenants.  
5. After that, users can use handy S3-compatible tools (awscli, Python boto3 library, etc.) to access the buckets in the tenant volume via S3 Gateway using the generated _Access ID & Secret Key_ pairs.


## Access Control

Ozone multi-tenancy relies on [Apache Ranger]({{< ref "security/SecurityWithRanger.md" >}}) to enforce access control to resources.

By default, a group of Ranger policies are created when a tenant is created on the tenant volume:

1. All users are able to create new buckets;
2. Only the bucket owner (i.e. the user that creates the bucket) and tenant admins can access the bucket content. 
   - Note: For Ozone admins, there typically are independent Ranger policies that grants them full access to the cluster, so they should be able to access the buckets as well. But it is still possible to create new policies to explicitly deny them access to buckets. 


## Setup

See [Multi-Tenancy Setup]({{< ref "Multi-Tenancy-Setup.md" >}}) page.

## Usage

See [Tenant Subcommands]({{< ref "interface/Tenant.md" >}}) page.

## References

 * For developers: check out the upstream jira [HDDS-4944](https://issues.apache.org/jira/browse/HDDS-4944) and the attached design docs.
