---
title: "Transparent Data Encryption"
date: "2019-04-03"
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

Ozone Transparent Data Encryption (TDE) enables you to encrypt data at rest. TDE is enabled at the bucket level when a bucket is created. To use TDE, an administrator must first configure a Key Management Server (KMS). Ozone can work with **Hadoop KMS** and **Ranger KMS**. The KMS URI needs to be provided to Ozone via the `core-site.xml` configuration file.

Once the KMS is configured, users can create an encryption key and then create an encrypted bucket using that key. All data written to an encrypted bucket will be transparently encrypted on the server-side, and data read from the bucket will be transparently decrypted.

### Configuring TDE

1.  **Set up a Key Management Server (KMS):**
  * **Hadoop KMS:** Follow the instructions in the [Hadoop KMS documentation](https://hadoop.apache.org/docs/r3.4.1/hadoop-kms/index.html).
  * **Ranger KMS:** Ranger KMS can also be used. For Ranger KMS, encryption keys can be managed via the Ranger KMS management console or its [REST API](https://ranger.apache.org/kms/apidocs/index.html), in addition to the `hadoop key` command line interface.

2.  **Configure Ozone:**
    Add the following property to Ozone’s `core-site.xml`:

        <property>
          <name>hadoop.security.key.provider.path</name>
          <value>kms://http@kms-host:9600/kms</value>
        </property>

    Replace `kms://http@kms-host:9600/kms` with the actual URI of your KMS. For example, `kms://http@kms1.example.com:9600/kms`

### Creating an Encryption Key

Use the `hadoop key create` command to create an encryption key in the configured KMS:

```shell
  hadoop key create <key_name> [-size <key_bit_length>] [-cipher <cipher_suite>] [-description <description>]
```

* `<key_name>`: The name of the encryption key.
* **`-size <key_bit_length>` (Optional):** Specifies the key bit length. The default is 128 bits (defined by `hadoop.security.key.default.bitlength`).
Ranger KMS supports both 128 and 256 bits. Hadoop KMS is also commonly used with 128 and 256 bit keys; for specific version capabilities, consult the Hadoop KMS documentation. Valid AES key lengths are 128, 192, and 256 bits.
* **`-cipher <cipher_suite>` (Optional):** Specifies the cipher suite. Currently, only **`AES/CTR/NoPadding`** (the default) is supported.
* `-description <description>` (Optional): A description for the key.

For example:

```shell
  hadoop key create enckey -size 256 -cipher AES/CTR/NoPadding -description "Encryption key for my_bucket"
```

### Creating an Encrypted Bucket

Use the Ozone shell `ozone sh bucket create` command with the `-k` (or `--bucketkey`) option to specify the encryption key:

```shell
  ozone sh bucket create --bucketkey <key_name> /<volume_name>/<bucket_name>
```

For example:

```shell
  ozone sh bucket create --bucketkey enckey /vol1/encrypted_bucket
```

Now, all data written to `/vol1/encrypted_bucket` will be encrypted at rest. As long as the client is configured correctly to use the key, such encryption is completely transparent to the end users.

### Performance Optimization for TDE

Since Ozone leverages Hadoop's encryption library, performance optimization strategies similar to HDFS encryption apply:

{{<requirements warning>}}
**Hardware Acceleration: Architecture Support**
The OpenSSL-based hardware acceleration discussed below is currently only supported on x86 architectures. ARM64 architectures are not supported at this time.
{{</requirements>}}

1. **Enable AES-NI Hardware Acceleration:**
  * Install OpenSSL development libraries: On most Linux distributions, install `openssl-devel` (or `libssl-dev` on Debian/Ubuntu) to provide `libcrypto.so`, which is utilized by the Hadoop native library for hardware-accelerated encryption.
   * Use servers with CPUs that support the AES-NI instruction set and RDRAND instruction (most modern Intel and AMD CPUs do)

2. **Install and Configure Native Libraries:**
   * Ensure that the native `libhadoop.so` library is properly installed
   ```shell
   ozone debug checknative
   ```
   * The output should show "true" for the hadoop library
   * To troubleshoot native library loading issues on Ozone Datanode and applications, configure their log level to DEBUG. The log messages below are examples, and actual paths may vary. The following log message indicates that the libhadoop native library fails to load:
   ```
   25/06/14 01:25:21 DEBUG util.NativeCodeLoader: Trying to load the custom-built native-hadoop library...
   25/06/14 01:25:21 DEBUG util.NativeCodeLoader: Failed to load native-hadoop with error: java.lang.UnsatisfiedLinkError: no hadoop in java.library.path
   25/06/14 01:25:21 DEBUG util.NativeCodeLoader: java.library.path=/opt/hadoop/lib/native:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
   25/06/14 01:25:21 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
   ```
   * And the following log message indicates OpenSSL library fails to load:
   ```
   25/06/14 01:18:53 DEBUG crypto.OpensslCipher: Failed to load OpenSSL Cipher.
   java.lang.UnsatisfiedLinkError: Cannot load libcrypto.so (libcrypto.so: cannot open shared object file: No such file or directory)!
   at org.apache.hadoop.crypto.OpensslCipher.initIDs(Native Method)
   at org.apache.hadoop.crypto.OpensslCipher.<clinit>(OpensslCipher.java:89)
   at org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec.<init>(OpensslAesCtrCryptoCodec.java:50)
   ```

3. **Validate Hardware Acceleration:**
   * To verify if AES-NI is being utilized, check OpenSSL acceleration:
   ```shell
   openssl speed -evp aes-256-ctr
   ```

### Using Transparent Data Encryption from S3G

Ozone’s S3 Gateway (S3G) allows you to access encrypted buckets. However, it's important to note that **Ozone does not support S3-SSE (Server-Side Encryption) or S3-CSE (Client-Side Encryption) in the way AWS S3 does.** That said, Ozone S3 buckets can be encrypted using Ranger KMS or Hadoop KMS to provide a guarantee similar to S3-SSE with client-supplied keys (SSE-C).

When creating an encrypted bucket that will be accessed via S3G:

1.  **Create the bucket under the `/s3v` volume:**
    The `/s3v` volume is the default volume for S3 buckets.

```shell
  ozone sh bucket create --bucketkey <key_name> /s3v/<bucket_name> --layout=OBJECT_STORE
```

2.  **Alternatively, create an encrypted bucket elsewhere and link it:**

```shell
  ozone sh bucket create --bucketkey <key_name> /<volume_name>/<bucket_name> --layout=OBJECT_STORE
  ozone sh bucket link /<volume_name>/<bucket_name> /s3v/<link_name>
```

Note 1: An encrypted bucket cannot be created via S3 APIs. It must be done using Ozone shell commands as shown above.
After creating an encrypted bucket, all the keys added to this bucket using s3g will be encrypted.

Note 2: `--layout=OBJECT_STORE` is specified in the above examples
for full compatibility with S3 (which is the default value for the `--layout`
argument, but explicitly added here to make a point).

Bucket created with the `OBJECT_STORE` type will NOT be accessible via
HCFS (ofs or o3fs) at all. And such access will be rejected. For instance:

```bash
  $ ozone fs -ls ofs://ozone1/s3v/encryptedbucket/
  -ls: Bucket: encryptedbucket has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.
  ```

```bash
  $ ozone fs -ls o3fs://encryptedbucket.s3v.ozone1/
  22/02/07 00:00:00 WARN fs.FileSystem: Failed to initialize fileystem o3fs://encryptedbucket.s3v.ozone1/: java.lang.IllegalArgumentException: Bucket: encryptedbucket has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.
  -ls: Bucket: encryptedbucket has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.
  ```

If one wants the bucket to be accessible from both S3G and HCFS (ofs and o3fs)
at the same time, use `--layout=FILE_SYSTEM_OPTIMIZED` instead.

However, in buckets with `FILE_SYSTEM_OPTIMIZED` layout, some irregular S3 key
names may be rejected or normalized, which can be undesired.
See [Prefix based File System Optimization]({{< relref "../feature/PrefixFSO.md" >}}) for more information.

When accessing an S3G-enabled encrypted bucket:

The below three configurations must be added to the kms-site.xml to allow the S3Gateway principal to act as a proxy for other users. In this example,
`ozone.s3g.kerberos.principal` is assumed to be `s3g`

```xml
<property>
  <name>hadoop.kms.proxyuser.s3g.users</name>
  <value>user1,user2,user3</value>
  <description>
    Specifies the list of users that the S3 Gateway (`s3g`) is allowed to impersonate when interacting with the KMS. Use `*` to allow all users.
  </description>
</property>
<property>
  <name>hadoop.kms.proxyuser.s3g.groups</name>
  <value>group1,group2,group3</value>
  <description>
    Specifies the list of groups whose members `s3g` is allowed to impersonate when making requests to the KMS. Use `*` to allow all groups.
  </description>
</property>
<property>
  <name>hadoop.kms.proxyuser.s3g.hosts</name>
  <value>s3g-host1.com</value>
  <description>
    Specifies the hostnames or IPs from which `s3g` is permitted to send proxy requests to the KMS. Use `*` to allow all hosts.
  </description>
</property>
```

### KMS Authorization

Key Management Servers (KMS) may enforce key access authorization. **Hadoop KMS supports ACLs (Access Control Lists) for fine-grained permission control, while Ranger KMS supports Ranger policies for encryption keys.** Ensure that the appropriate users have the necessary permissions based on the KMS type in use.

For example, when using Ranger KMS for authorization, to allow the user `om` (the Ozone Manager user) to access the key `enckey` and the user `hdfs` (a typical HDFS service user) to manage keys, you might have policies in Ranger KMS like:

* **Policy for `om` user (or the user running the Ozone Manager):**
  * Resource: `keyname=enckey`
  * Permissions: `DECRYPT_EEK` (Decrypt Encrypted Encryption Key)
* **Policy for S3 Gateway proxy user (e.g., the user specified in `ozone.s3g.kerberos.principal`, typically `s3g`):**
  * Resource: `keyname=enckey` (or specific keys for S3 buckets)
  * Permissions: `DECRYPT_EEK`
* **Policy for administrative users (e.g., `hdfs` or a keyadmin group):**
  * Resource: `keyname=*` (or specific keys)
  * Permissions: `CREATE_KEY`, `DELETE_KEY`, `GET_KEYS`, `ROLL_NEW_VERSION`

Refer to the Ranger documentation for detailed instructions on configuring KMS policies if you are using Ranger KMS. For Hadoop KMS, consult its [Hadoop KMS documentation](https://hadoop.apache.org/docs/r3.4.1/hadoop-kms/index.html#ACLs_.28Access_Control_Lists.29) for managing ACLs.

### Additional References

* For more background on Transparent Data Encryption concepts, you can refer to the [Transparent Encryption in HDFS documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html).
* For detailed information on Hadoop KMS, see the [Hadoop KMS documentation](https://hadoop.apache.org/docs/r3.4.1/hadoop-kms/index.html).
* For OpenSSL crypto performance tuning, refer to [Intel's AES-NI optimization guide](https://software.intel.com/content/www/us/en/develop/articles/intel-advanced-encryption-standard-aes-instructions-set.html).
