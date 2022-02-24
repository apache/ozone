---
title: "S3 Tenant commands"
weight: 3
menu:
   main:
      parent: "Client Interfaces"
summary: Ozone subcommands for S3 tenant management
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

For a higher level understanding of multi-tenancy architecture, see [Multi-Tenancy feature]({{< ref "feature/S3-Multi-Tenancy.md" >}}).

All Multi-Tenancy subcommands are located under CLI `ozone tenant`.

The commands below assume a Kerberized Ozone cluster with Ranger install. Enabling HTTPS on S3 Gateway is optional but recommended.

## Quick Start

### Setup

Follow the [Multi-Tenancy Setup]({{< ref "feature/S3-Multi-Tenancy-Setup.md" >}}) guide if you haven't done so.

If the OzoneManagers are running in HA, append `--om-service-id=` accordingly to the commands.

### Create a tenant

Create a new tenant in the current Ozone cluster.
This operation requires Ozone cluster administrator privilege.

Creating a tenant creates a volume of the exact same name. Volume name restrictions apply.

```shell
ozone tenant create <TENANT_NAME>
```

Example:

```shell
bash-4.2$ kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM
bash-4.2$ ozone tenant create tenantone
2022-02-16 00:00:00,000 [main] INFO rpc.RpcClient: Creating Tenant: 'tenantone', with new volume: 'tenantone'
Created tenant 'tenantone'.
```


### List tenants

List all tenants in an Ozone cluster.

```shell
ozone tenant list
```

Example:

```shell
bash-4.2$ ozone tenant list
tenantone
```


### Assign a user to a tenant

The first user in a tenant must be assigned by an Ozone cluster administrator.

By default when user `testuser` is assigned to tenant `tenantone`, the generated Access ID for the user in this tenant is `tenantone$testuser`.

- Be sure to enclose the Access ID in single quotes in Bash when using it so it doesn't get auto-translated into environment variables.

It is possible to assign a user to multiple tenants.

```shell
ozone tenant user assign <USER_NAME> --tenant=<TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user assign testuser --tenant=tenantone
Assigned 'testuser' to 'tenantone' with accessId 'tenantone$testuser'.
export AWS_ACCESS_KEY_ID='tenantone$testuser'
export AWS_SECRET_ACCESS_KEY='<GENERATED_SECRET>'
```


### Assign a user as a tenant admin

The first user in a tenant must be assigned by an Ozone cluster administrator.

Both delegated and non-delegated tenant admin can assign and revoke **regular** tenant users.

The only difference between delegated tenant admin and non-delegated tenant admin is that delegated tenant admin can assign and revoke tenant **admins** in the tenant,
while non-delegated tenant admin can't.

Unless specified, `ozone tenant assignadmin` assigns **delegated** tenant admins by default.

It is possible to assign a user to be tenant admins in multiple tenants.

```shell
ozone tenant user assignadmin <ACCESS_ID> --tenant=<TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user assignadmin 'tenantone$testuser' --tenant=tenantone
Assigned admin to 'tenantone$testuser' in tenant 'tenantone'
```

Once `testuser` becomes a tenant admin of `tenantone`, one can kinit as `testuser` and assign new users to the tenant,
even new tenant admins (if delegated). Example commands for illustration:

```shell
kinit -kt /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM
ozone tenant user assign testuser2 --tenant=tenantone
ozone tenant user assignadmin 'tenantone$testuser2' --tenant=tenantone
```


### List users in a tenant

```shell
ozone tenant user list --tenant=<TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user list --tenant=tenantone
- User 'testuser' with accessId 'tenantone$testuser'
```


### Get tenant user info

This command lists all tenants a user is assigned to.

```shell
ozone tenant user info <USER_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user info testuser
User 'testuser' is assigned to:
- Tenant 'tenantone' delegated admin with accessId 'tenantone$testuser'
```


### Bonus: Accessing a bucket in a tenant volume via S3 Gateway using S3 API

- {{< detail-tag "Click here to expand/collapse" >}}

#### Configure AWS CLI

```shell
bash-4.2$ aws configure
AWS Access Key ID [****************fslf]: tenantone$testuser
AWS Secret Access Key [****************fslf]: <GENERATED_SECRET>
Default region name [us-west-1]:
Default output format [None]:
```

#### List buckets, create a bucket

```shell
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 list-buckets
{
    "Buckets": []
}
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 create-bucket --bucket bucket-test1
{
    "Location": "http://s3g:9878/bucket-test1"
}
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 list-buckets
{
    "Buckets": [
        {
            "Name": "bucket-test1",
            "CreationDate": "2022-02-16T00:05:00.000Z"
        }
    ]
}
```

#### Put object (key) to a bucket, list objects

```shell
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 put-object --bucket bucket-test1 --key file1 --body README.md
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 list-objects --bucket bucket-test1
{
    "Contents": [
        {
            "Key": "file1",
            "LastModified": "2022-02-16T00:10:00.000Z",
            "ETag": "2022-02-16T00:10:00.000Z",
            "Size": 3811,
            "StorageClass": "STANDARD"
        }
    ]
}
```

#### Get object (key) from a bucket

```shell
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 get-object --bucket bucket-test1 --key file1 file1-get.txt
{
    "AcceptRanges": "bytes",
    "LastModified": "Wed, 16 Feb 2022 00:10:00 GMT",
    "ContentLength": 3811,
    "CacheControl": "no-cache",
    "ContentType": "application/octet-stream",
    "Expires": "Wed, 16 Feb 2022 00:15:00 GMT",
    "Metadata": {}
}
bash-4.2$ diff file1-get.txt README.md
```

{{< /detail-tag >}}


### Revoke a tenant admin

```shell
ozone tenant user revokeadmin <ACCESS_ID>
```

Example:

```shell
bash-4.2$ ozone tenant user revokeadmin 'tenantone$testuser'
Revoked admin role of 'tenantone$testuser'.
```


### Revoke user access from a tenant

```shell
ozone tenant user revoke <ACCESS_ID>
```

Example:

```shell
bash-4.2$ ozone tenant user revoke 'tenantone$testuser'
Revoked accessId 'tenantone$testuser'.
```


### Delete a tenant

In order to be able to delete a tenant, the tenant has to be empty. i.e. All users need to be revoked before a tenant can be deleted.
Otherwise OM will throw `TENANT_NOT_EMPTY` exception and refuse to delete the tenant.

Note that it is intentional by design that the volume created and associated with the tenant during tenant creation is not removed.
An admin has to remove the volume manually as prompt in the CLI, if deemed necessary.

```shell
ozone tenant delete <TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant delete tenantone
Deleted tenant 'tenantone'.
But the associated volume 'tenantone' is not removed. To delete it, run
    ozone sh volume delete tenantone
```

If an Ozone cluster admin (or whoever has the permission to delete the volume in Ranger) tries delete a volume before the tenant is deleted using the command above,
the `ozone sh volume delete` command would fail because the volume reference count is not zero:

```shell
bash-4.2$ ozone sh volume delete tenantone
VOLUME_IS_REFERENCED Volume reference count is not zero (1). Ozone features are enabled on this volume. Try `ozone tenant delete <tenantId>` first.
```
