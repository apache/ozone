---
title: "Transparent Data Encryption"
date: "2019-April-03"
summary: TDE allows data on the disks to be encrypted-at-rest and automatically decrypted during access. 
weight: 2
menu:
   main:
      parent: Security
icon: lock
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

Ozone TDE setup process and usage are very similar to HDFS TDE.
The major difference is that Ozone TDE is enabled at Ozone bucket level
when a bucket is created.

### Setting up the Key Management Server

To use TDE, admin must setup a Key Management Server and provide that URI to
Ozone/HDFS. Since Ozone and HDFS can use the same Key Management Server, this
 configuration can be provided via *hdfs-site.xml*.

Property| Value
-----------------------------------|-----------------------------------------
hadoop.security.key.provider.path  | KMS uri. <br> e.g. kms://http@kms-host:9600/kms

### Using Transparent Data Encryption
If this is already configured for your cluster, then you can simply proceed
to create the encryption key and enable encrypted buckets.

To create an encrypted bucket, client need to:

   * Create a bucket encryption key with hadoop key CLI, which is similar to
  how you would use HDFS encryption zones.

  ```bash
  hadoop key create encKey
  ```
  The above command creates an encryption key for the bucket you want to protect.
  Once the key is created, you can tell Ozone to use that key when you are
  reading and writing data into a bucket.

   * Assign the encryption key to a bucket.

  ```bash
  ozone sh bucket create -k encKey /vol/encryptedbucket
  ```

After this command, all data written to the _encryptedbucket_ will be encrypted
via the encKey and while reading the clients will talk to Key Management
Server and read the key and decrypt it. In other words, the data stored
inside Ozone is always encrypted. The fact that data is encrypted at rest
will be completely transparent to the clients and end users.

### Using Transparent Data Encryption from S3G

There are two ways to create an encrypted bucket that can be accessed via S3 Gateway.

#### Option 1. Create a bucket using shell under "/s3v" volume

  ```bash
  ozone sh bucket create -k encKey --layout=FILE_SYSTEM_OPTIMIZED /s3v/encryptedbucket
  ```

#### Option 2. Create a link to an encrypted bucket under "/s3v" volume

  ```bash
  ozone sh bucket create -k encKey --layout=FILE_SYSTEM_OPTIMIZED /vol/encryptedbucket
  ozone sh bucket link /vol/encryptedbucket /s3v/linkencryptedbucket
  ```

Note 1: An encrypted bucket cannot be created via S3 APIs. It must be done using Ozone shell commands as shown above.
After creating an encrypted bucket, all the keys added to this bucket using s3g will be encrypted.

Note 2: `--layout=FILE_SYSTEM_OPTIMIZED` is added in the command line above to allow HCFS (o3fs / ofs) access.

In non-secure mode, the user running the S3Gateway daemon process is the proxy user, 
while in secure mode the S3Gateway Kerberos principal (ozone.s3g.kerberos.principal) is the proxy user. 
S3Gateway proxy's all the users accessing the encrypted buckets to decrypt the key. 
For this purpose on security enabled cluster, during S3Gateway server startup 
logins using configured 
**ozone.s3g.kerberos.keytab.file**  and **ozone.s3g.kerberos.principal**. 

The below two configurations must be added to the kms-site.xml to allow the S3Gateway principal to act as a proxy for other users. In this example, "ozone.s3g.kerberos.principal" is assumed to be "s3g"

```
<property>
  <name>hadoop.kms.proxyuser.s3g.users</name>
  <value>user1,user2,user3</value>
  <description>
        Here the value can be all the S3G accesskey ids accessing Ozone S3 
        or set to '*' to allow all the accesskey ids.
  </description>
</property>

<property>
  <name>hadoop.kms.proxyuser.s3g.hosts</name>
  <value>s3g-host1.com</value>
  <description>
         This is the host where the S3Gateway is running. Set this to '*' to allow
         requests from any hosts to be proxied.
  </description>
</property>
```

### KMS Authorization

If Ranger authorization is enabled for KMS, then decrypt key permission should be given to
access key id user(currently access key is kerberos principal) to decrypt the encrypted key 
to read/write a key in the encrypted bucket.
