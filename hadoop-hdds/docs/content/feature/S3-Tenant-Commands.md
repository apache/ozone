---
title: "Tenant commands"
weight: 12
menu:
   main:
      parent: "S3 Multi-Tenancy"
summary: Ozone subcommands for S3 tenant management
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

For a higher level understanding of multi-tenancy architecture, see [Multi-Tenancy feature]({{< ref "feature/S3-Multi-Tenancy.md" >}}).

All Multi-Tenancy subcommands are located under CLI `ozone tenant`.

The commands below assume a Kerberized Ozone cluster with Ranger install. Enabling HTTPS on S3 Gateway is optional but recommended.

The exit code of a successful tenant command should be `0`.
A non-zero exit code indicates failure, which should be accompanied an error message.


## Quick Start

### Setup

Follow the [Multi-Tenancy Setup]({{< ref "feature/S3-Multi-Tenancy-Setup.md" >}}) guide if you haven't done so.

If the OzoneManagers are running in HA, append `--om-service-id=` accordingly to the commands.

### Create a tenant

Create a new tenant in the current Ozone cluster.
This operation requires Ozone cluster administrator privilege.

Apart from adding new OM DB entries, creating a tenant also does the following in the background:
1. Creates a volume of the exact same name. Therefore, volume name restrictions apply to the tenant name as well. Specifying a custom volume name during tenant creation is not supported yet. Tenant volume cannot be changed once the tenant is created.
2. Creates two new Ranger roles, `tenantName-UserRole` and `tenantName-AdminRole`.
3. Creates new Ranger policies that allows all tenant users to list and create buckets by default under the tenant volume, but only bucket owners and tenant admins are allowed to access the bucket contents.

```shell
ozone tenant [--verbose] create <TENANT_NAME>
```

Example:

```shell
bash-4.2$ kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM
bash-4.2$ ozone tenant create tenantone
2022-02-16 00:00:00,000 [main] INFO rpc.RpcClient: Creating Tenant: 'tenantone', with new volume: 'tenantone'
```

Verbose output example:

```shell
bash-4.2$ kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM
bash-4.2$ ozone tenant --verbose create tenantone
2022-02-16 00:00:00,000 [main] INFO rpc.RpcClient: Creating Tenant: 'tenantone', with new volume: 'tenantone'
{
  "tenantId": "tenantone"
}
```


### List tenants

List all tenants in an Ozone cluster. Optionally, use `--json` to print the detailed result in JSON.

```shell
ozone tenant list [--json]
```

Example:

```shell
bash-4.2$ ozone tenant list
tenantone
```

```shell
bash-4.2$ ozone tenant list --json
[
  {
    "tenantId": "tenantone",
    "bucketNamespaceName": "tenantone",
    "userRoleName": "tenantone-UserRole",
    "adminRoleName": "tenantone-AdminRole",
    "bucketNamespacePolicyName": "tenantone-VolumeAccess",
    "bucketPolicyName": "tenantone-BucketAccess"
  }
]
```


### Assign a user to a tenant

The first user in a tenant must be assigned by an Ozone cluster administrator.

By default when user `testuser` is assigned to tenant `tenantone`, the generated Access ID for the user in this tenant is `tenantone$testuser`.

- Be sure to enclose the Access ID in single quotes in Bash when using it so it doesn't get expanded as environment variables.

It is possible to assign a user to multiple tenants.

```shell
ozone tenant [--verbose] user assign <USER_NAME> --tenant=<TENANT_NAME>
```

`<USER_NAME>` should be a short user name for a Kerberos principal, e.g. `testuser` when the Kerberos principal is `testuser/scm@EXAMPLE.COM`

Example:

```shell
bash-4.2$ ozone tenant user assign testuser --tenant=tenantone
export AWS_ACCESS_KEY_ID='tenantone$testuser'
export AWS_SECRET_ACCESS_KEY='<GENERATED_SECRET>'

bash-4.2$ ozone tenant user assign testuser --tenant=tenantone
TENANT_USER_ACCESS_ID_ALREADY_EXISTS accessId 'tenantone$testuser' already exists!
```

```shell
bash-4.2$ ozone tenant --verbose user assign testuser2 --tenant=tenantone
export AWS_ACCESS_KEY_ID='tenantone$testuser2'
export AWS_SECRET_ACCESS_KEY='<GENERATED_SECRET>'
Assigned 'testuser2' to 'tenantone' with accessId 'tenantone$testuser2'.
```


### Assign a user as a tenant admin

The first user in a tenant must be assigned by an Ozone cluster administrator.

Both delegated and non-delegated tenant admin can assign and revoke **regular** tenant users.

The only difference between delegated tenant admin and non-delegated tenant admin is that delegated tenant admin can assign and revoke tenant **admins** in the tenant,
while non-delegated tenant admin can't.

By default, `ozone tenant assignadmin` assigns a **non-delegated** tenant admin.
To assign a **delegated** tenant admin, specify `--delegated` or `-d`.

It is possible to assign a user to be tenant admins in multiple tenants. Just a reminder, the user would have a different access ID under each tenant.

```shell
ozone tenant user assignadmin <ACCESS_ID> [-d|--delegated] --tenant=<TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user assignadmin 'tenantone$testuser' --tenant=tenantone
```

By default, if the command succeeds, it exits with `0` and prints nothing. Use `--verbose` to print the result in JSON.

```shell
bash-4.2$ ozone tenant --verbose user assignadmin 'tenantone$testuser' --tenant=tenantone
{
  "accessId": "tenantone$testuser",
  "tenantId": "tenantone",
  "isAdmin": true,
  "isDelegatedAdmin": true
}
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
ozone tenant user list [--json] <TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user list tenantone
- User 'testuser' with accessId 'tenantone$testuser'
- User 'testuser2' with accessId 'tenantone$testuser2'
```

```shell
bash-4.2$ ozone tenant user list --json tenantone
[
  {
    "user": "testuser",
    "accessId": "tenantone$testuser"
  },
  {
    "user": "testuser2",
    "accessId": "tenantone$testuser2"
  }
]
```

### Get tenant user info

This command lists all tenants a user is assigned to.

```shell
ozone tenant user info [--json] <USER_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant user info testuser
User 'testuser' is assigned to:
- Tenant 'tenantone' delegated admin with accessId 'tenantone$testuser'
```

```shell
bash-4.2$ ozone tenant user info --json testuser
{
  "user": "testuser",
  "tenants": [
    {
      "accessId": "tenantone$testuser",
      "tenantId": "tenantone",
      "isAdmin": true,
      "isDelegatedAdmin": true
    }
  ]
}
```

### Get tenant user secret key

Get secret key by tenant user access ID.

Unlike `ozone s3 getsecret`, it doesn’t generate a key if the access ID doesn’t exist.

```shell
ozone tenant user get-secret <ACCESS_ID>
```
or
```shell
ozone tenant user getsecret <ACCESS_ID>
```

Example:

```shell
bash-4.2$ ozone tenant user get-secret 'tenantone$testuser'
export AWS_ACCESS_KEY_ID='tenantone$testuser'
export AWS_SECRET_ACCESS_KEY='<GENERATED_SECRET>'
```

### Set tenant user secret key

Set secret key for a tenant user access ID.

Secret key length should be at least 8 characters.

```shell
ozone tenant user set-secret <ACCESS_ID> --secret <SECRET_KEY>
```

or

```shell
ozone tenant user setsecret <ACCESS_ID> --secret <SECRET_KEY>
```

Example:

```shell
bash-4.2$ ozone tenant user set-secret 'tenantone$testuser' --secret 'NEW_SECRET'
export AWS_ACCESS_KEY_ID='tenantone$testuser'
export AWS_SECRET_ACCESS_KEY='NEW_SECRET'
```

### Revoke a tenant admin

```shell
ozone tenant [--verbose] user revokeadmin <ACCESS_ID>
```

Example:

```shell
bash-4.2$ ozone tenant user revokeadmin 'tenantone$testuser'
```

```shell
bash-4.2$ ozone tenant --verbose user revokeadmin 'tenantone$testuser'
{
  "accessId": "tenantone$testuser",
  "isAdmin": false,
  "isDelegatedAdmin": false
}
```


### Revoke user access from a tenant

```shell
ozone tenant [--verbose] user revoke <ACCESS_ID>
```

Example:

```shell
bash-4.2$ ozone tenant user revoke 'tenantone$testuser'
```

With verbose output:

```shell
bash-4.2$ ozone tenant --verbose user revoke 'tenantone$testuser'
Revoked accessId 'tenantone$testuser'.
```


### Delete a tenant

In order to be able to delete a tenant, the tenant has to be empty. i.e. All users need to be revoked before a tenant can be deleted.
Otherwise OM will throw `TENANT_NOT_EMPTY` exception and refuse to delete the tenant.

Note that it is intentional by design that the volume created and associated with the tenant during tenant creation is not removed.
An admin has to remove the volume manually as prompt in the CLI, if deemed necessary.

Verbose option, in addition, will print the Ozone Manager RAW response in JSON.

```shell
ozone tenant [--verbose] delete <TENANT_NAME>
```

Example:

```shell
bash-4.2$ ozone tenant delete tenantone
Deleted tenant 'tenantone'.
But the associated volume 'tenantone' is not removed. To delete it, run
    ozone sh volume delete tenantone
```

With verbose output:

```shell
bash-4.2$ ozone tenant --verbose delete tenantone
Deleted tenant 'tenantone'.
But the associated volume 'tenantone' is not removed. To delete it, run
    ozone sh volume delete tenantone

{
  "tenantId": "tenantone",
  "volumeName": "tenantone",
  "volumeRefCount": 0
}
```

If an Ozone cluster admin (or whoever has the permission to delete the volume in Ranger) tries delete a volume before the tenant is deleted using the command above,
the `ozone sh volume delete` command would fail because the volume reference count is not zero:

```shell
bash-4.2$ ozone sh volume delete tenantone
VOLUME_IS_REFERENCED Volume reference count is not zero (1). Ozone features are enabled on this volume. Try `ozone tenant delete <tenantId>` first.
```


## Creating bucket links

Bucket links can be used to allow access to buckets outside of the tenant volume.

Bucket (sym)links are a special type of bucket that points to other buckets in the same Ozone cluster. It is similar to POSIX symbolic links.

An example to create a bucket link:

```shell
$ ozone tenant linkbucket /vol1/bucket1 /tenantone/linked-bucket1
```

The command above creates a bucket symlink `linked-bucket1` in volume `tenantone`, which points to `bucket1` in `vol1`.

As long as the user running this command has the permission to create a bucket in the target volume `tenantone`, the command will succeed.

- The link bucket command itself does not check for permission to access the source volume and bucket.
- The link bucket command will not even check if the source volume and bucket exists.
- Permission check will be performed when the bucket symlink is actually accessed.
  - In order to grant a user in tenant `tenantone` access the bucket, a new policy should be added by a Ranger admin that allow that user intended permissions (`READ, WRITE, LIST, CREATE, DELETE, ...`) to the source bucket `bucket1` in volume `vol1`.
- At the moment, `ozone tenant linkbucket` command is equivalent to `ozone sh bucket link` command (see **Expose any volume** section in [S3 protocol]({{< ref "S3.md" >}})).


## Example: Accessing a bucket in a tenant volume via S3 Gateway using S3 API

Here is an example of accessing the bucket using AWS CLI in the Docker Compose cluster, with tenant `tenantone` created and `testuser` assigned to the tenant.

### Configure AWS CLI

```shell
bash-4.2$ aws configure
AWS Access Key ID [****************fslf]: tenantone$testuser
AWS Secret Access Key [****************fslf]: <GENERATED_SECRET>
Default region name [us-west-1]:
Default output format [None]:
```

### List buckets, create a bucket

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

In the Docker Compose cluster, the AWS CLI might report `AccessDenied` because it uses a mocked Ranger endpoint (which can't be used to perform authorization). A production Ranger setup uses [`RangerOzoneAuthorizer`](https://github.com/apache/ranger/blob/master/plugin-ozone/src/main/java/org/apache/ranger/authorization/ozone/authorizer/RangerOzoneAuthorizer.java) in OM for authorization while the `ozonesecure` Docker Compose cluster still uses `OzoneNativeAuthorizer`. So a workaround is to set the volume owner to `testuser` to gain full access, as `OzoneNativeAuthorizer` grants the volume owner full permission:

```shell
ozone sh volume update tenantone --user=testuser
```

The bucket created with `aws s3api` is also visible under Ozone CLI:

```shell
bash-4.2$ ozone sh bucket list /tenantone
[ {
  "metadata" : { },
  "volumeName" : "tenantone",
  "name" : "bucket-test1",
  "storageType" : "DISK",
  "versioning" : false,
  "usedBytes" : 0,
  "usedNamespace" : 0,
  "creationTime" : "2022-02-16T00:05:00.000Z",
  "modificationTime" : "2022-02-16T00:05:00.000Z",
  "quotaInBytes" : -1,
  "quotaInNamespace" : -1,
  "bucketLayout" : "OBJECT_STORE",
  "owner" : "root",
  "link" : false
} ]
```

### Put object (key) to a bucket, list objects

```shell
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 put-object --bucket bucket-test1 --key file1 --body README.md
bash-4.2$ aws s3api --endpoint-url http://s3g:9878 list-objects --bucket bucket-test1
{
    "Contents": [
        {
            "Key": "file1",
            "LastModified": "2022-02-16T00:10:00.000Z",
            "ETag": "e99f93dedfe22e9a133dc3c634f14634",
            "Size": 3811,
            "StorageClass": "STANDARD"
        }
    ]
}
```

### Get object (key) from a bucket

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

