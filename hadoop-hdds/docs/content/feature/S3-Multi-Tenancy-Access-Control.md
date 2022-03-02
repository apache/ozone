---
title: "Access Control"
weight: 1
menu:
   main:
      parent: "S3 Multi-Tenancy"
summary: Access Control with Ranger in Ozone Multi-Tenancy
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

### Ranger Policies

When a tenant is created, Ozone will create a set of Ranger policies on the tenant's volume which allow the following:

1. All users are able to create new buckets;
2. Only the bucket owner (i.e. the user that creates the bucket) and tenant admins can access the bucket content.
    - Note: For Ozone admins, typically there would be other Ranger policies that grants them full access to the cluster, if this is case they should be able to access the buckets as well. Though it is still possible to create new Ranger policies to explicitly deny them access to buckets.

Ranger admin is responsible for manually adding new policies to grant or deny any other access patterns. For example:
- Allow all users in a tenant read-only access to a bucket.
  - Corresponding Ranger policy Allow Condition: `Roles = tenantName-UserRole, Permissions = READ,LIST`

It is recommended to add new policies instead of editing the default tenant policies. If one has to, **DO NOT** remove the **Policy Label** on those default tenant policies.

### Ranger Roles

These new Ranger policies would have the corresponding **Ranger roles** added in their **Allow Conditions**.

Namely, `tenantName-UserRole` and `tenantName-AdminRole` Ranger roles are created when a tenant is created by an Ozone administrator under the CLI.

`tenantName-UserRole` contains a list of all user names that are assigned to this tenant.

`tenantName-AdminRole` contains a list of all tenant admins that are assigned to this tenant.

We leverage Ranger roles mainly for the advantage of easier user management in a tenant:
1. When new users are assigned to a tenant, Ozone Manager simply adds the new user to `tenantName-UserRole` Ranger role.
2. When new tenant admins are assigned, Ozone Manager simply adds the user name to `tenantName-AdminRole` Ranger role. Delegated tenant admins will have the "Role Admin" checkbox checked, while non-delegated tenant admins won't.
    - Role admins in a Ranger role has the permission to edit that Ranger role.
3. And because `tenantName-AdminRole` is the "Role Admin" of `tenantName-UserRole`, whichever user in the `tenantName-AdminRole` automatically has the permission to add new users to the tenant, meaning all tenant admins (whether delegated or not) has the permission to assign and revoke users in this tenant.

- **DO NOT** manually edit any Ranger roles created by the CLI. Any changes to them will be overwritten by the Ranger sync thread.


### Ranger Sync

A Ranger Sync thread has been implemented to keep the Ranger policy and role states in-sync with Ozone Manager database in case of Ozone Manager crashes during tenant administrative operations.

The Ranger Sync thread does the following:
1. Cleans up any default tenant policies if a tenant is already deleted.
2. Checks if default tenant roles are out-of-sync (could be caused by OM crash during user assign/revoke operation). Overwrites them if this is the case.

