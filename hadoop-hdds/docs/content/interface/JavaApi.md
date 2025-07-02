---
title: "Java API"
date: "2017-09-14"
weight: 5
menu:
  main:
    parent: "Client Interfaces"
summary: Ozone has a set of Native RPC based APIs. This is the lowest level API's on which all other protocols are built. This is the most performant and feature-full of all Ozone protocols.
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

## I. Introduction to Ozone Java APIs

### A. Purpose of This Document

This document provides Java developers with a comprehensive guide to integrating their applications with Apache Ozone. It includes detailed information and end-to-end examples for both the native Object Store Client API and the Hadoop Compatible FileSystem API (OFS/o3fs). The aim of this guide is to offer a holistic understanding of Ozone integration for Java developers.

### B. Two Primary Java APIs for Ozone Interaction

Apache Ozone provides two main avenues for Java applications to interact with the storage system, catering to different use cases and existing application architectures:

1.  **[Native Ozone Client API](#native-java-api):** Ozone's Java client library uses RPC for high-performance communication with the Ozone cluster. It offers direct access to all Ozone functionalities (volumes, buckets, keys) and is the recommended API for new Ozone-specific applications or those needing maximum performance and feature access.

2.  **[Hadoop Compatible FileSystem API (OFS & o3fs)](#hcfs-api):** Ozone's Java API allows HDFS/HCFS-compatible applications to work with Ozone with little to no code changes by implementing the `org.apache.hadoop.fs.FileSystem` interface. This benefits Hadoop ecosystem applications like Spark, Hive, and YARN, enabling them to use Ozone for storage. The API supports two schemes: `ofs://` for a view across all volumes and buckets (like a traditional filesystem) and `o3fs://` which scopes the view to a specific bucket as the root.

Ozone offers two APIs: a native API for cloud-native development and an HCFS interface for Hadoop compatibility. This dual approach provides developers flexibility in choosing the API that suits their application's architecture and integration needs, whether for new Ozone applications or migrating Hadoop workloads.

## II. Setting Up Your Development Environment

To begin developing Java applications that interact with Apache Ozone, a proper development environment must be configured. This involves setting up the Java Development Kit (JDK), Apache Maven for project management, and correctly specifying the necessary Ozone client dependencies and configurations.

### A. Prerequisites

Ozone Java API Development Environment Setup
Before you begin developing with the Ozone Java API, ensure your environment meets these prerequisites:

* **Java Development Kit (JDK):** A compatible JDK version is required. Typically, JDK 8 or a later version is recommended, aligning with the common requirements of the Hadoop ecosystem.
* **Apache Maven:** Maven is essential for managing project dependencies, building your application, and packaging it. Ensure Maven is installed and configured correctly on your system.

For detailed instructions on setting up your basic build environment, please refer to the [From Source](start/FromSource.md) page.

### B. Maven Dependencies

Apache Ozone's client libraries are readily available from Maven Central, simplifying the process of including them in your Java projects. Depending on which Ozone Java API you intend to use, you will need to add specific dependencies to your project's `pom.xml` file.

**1. Native Ozone Client**

For applications using the native Ozone RPC-based client API, the primary dependency is `ozone-client`. This artifact typically brings in other required modules, such as `hdds-client` and `ozone-interface-client`, as transitive dependencies, simplifying your `pom.xml` configuration.

Add the following snippet to your `pom.xml`'s `<dependencies>` section:
```xml
<dependency>
    <groupId>org.apache.ozone</groupId>
    <artifactId>ozone-client</artifactId>
    <version>${ozone.version}</version>
</dependency>
```

**2. Hadoop Compatible FileSystem Client (OFS/o3fs)**

For applications that will use Ozone via the Hadoop Compatible FileSystem API (schemes `ofs://` or `o3fs://`), a different set of dependencies is required. For Hadoop 3.x environments, the recommended artifact is `ozone-filesystem-hadoop3-client`. This version is "shaded," meaning its dependencies (like Protocol Buffers) are repackaged to avoid conflicts with versions potentially used by your application or other libraries. This is generally a safer choice for broader compatibility.

Add the following to your `pom.xml`:
```xml
<dependency>
    <groupId>org.apache.ozone</groupId>
    <artifactId>ozone-filesystem-hadoop3-client</artifactId>
    <version>${ozone.version}</version>
</dependency>
```
If you are working in an environment where dependency conflicts are not a concern, or if you specifically need the non-shaded version, `org.apache.ozone:ozone-filesystem-hadoop3` can be used. For Hadoop 2.x environments, a corresponding `ozone-filesystem-hadoop2-client` artifact is available.

The use of shaded JARs, particularly for the Hadoop Compatible FileSystem client, is a significant consideration for developers. Big data applications often incorporate numerous libraries, each with its own set of transitive dependencies. Libraries like Protocol Buffers are common, and version mismatches can lead to runtime errors such as `NoSuchMethodError` or other classpath conflicts. The `-client` suffixed JARs (e.g., `ozone-filesystem-hadoop3-client`) mitigate this risk by shading their dependencies, effectively isolating Ozone's internal library versions from those of the parent application. Therefore, for most OFS/o3fs use cases, the shaded JAR is strongly recommended to ensure a smoother integration experience.

**Table: Core Maven Dependencies for Ozone Java Development**

To provide a quick reference, the following table summarizes the key Maven dependencies:

| API Type                     | Group ID           | Artifact ID                       | Notes                                                 |
| :--------------------------- | :----------------- |:----------------------------------|:------------------------------------------------------|
| Native Client API            | `org.apache.ozone` | `ozone-client`                    | Main client for native RPC interaction.               |
| Hadoop FS API (Hadoop 3.x)   | `org.apache.ozone` | `ozone-filesystem-hadoop3`        | For `ofs://` and `o3fs://` access. Unshaded protobuf. |
| Hadoop FS API (Hadoop 3.x)   | `org.apache.ozone` | `ozone-filesystem-hadoop3-client` | For `ofs://` and `o3fs://` access. Shaded protobuf.   |
| Hadoop FS API (Hadoop 2.x)   | `org.apache.ozone` | `ozone-filesystem-hadoop2`        | If targeting Hadoop 2.x environments.                 |

### C. Client Configuration

Ozone clients require configuration information to connect to and interact with an Ozone cluster. This is typically provided through XML configuration files placed in the application's classpath.

**1. `ozone-site.xml` (for Native Client and OFS/o3fs)**

The `ozone-site.xml` file contains core configuration properties for Ozone clients.

**Table: Key `ozone-site.xml` Properties for Java Clients**

| Property Name                             | Example Value (Non-HA)      | Example Value (HA)               | Description                                                                                                   | When Required                                             |
| :---------------------------------------- | :-------------------------- | :------------------------------- |:--------------------------------------------------------------------------------------------------------------| :-------------------------------------------------------- |
| `ozone.om.address`                        | `om-host.example.com:9862`  | (Not used directly for client)   | Address of the Ozone Manager (OM) for non-HA setups.                                                          | Non-HA setups.                                            |
| `ozone.om.service.ids`                    |                             | `omservice1`                     | A logical name (service ID) for the OM HA cluster. Multiple service IDs can be defined for multiple clusters. | HA setups.                                                |
| `ozone.om.nodes.omservice1`               |                             | `omNode1,omNode2,omNode3`        | Comma-separated list of logical node IDs for OM instances within the specified service ID.                    | HA setups (suffix with actual service ID).                |
| `ozone.om.address.omservice1.omNode1`     |                             | `om-host1.example.com:9862`      | Hostname/IP and port for a specific OM node within an HA service. Define for each node.                       | HA setups (suffix with actual service ID & node ID).      |
| `ozone.security.enabled`                  | `false`                     | `true`                           | Enables or disables Ozone security features (e.g., Kerberos).                                                 | If connecting to a secure cluster.                        |
| `hadoop.security.authentication`          |                             | `kerberos`                       | Specifies the authentication mechanism, typically 'kerberos' for secure clusters.                             | If `ozone.security.enabled` is true.                      |
| `ozone.om.kerberos.principal`             |                             | `om/_HOST@REALM`                 | The Kerberos principal for the Ozone Manager.                                                                 | Secure HA/Non-HA setups.                                  |
| `ozone.scm.client.address`                | `localhost:9860`            | `scm-host.example.com:9860`      | Address for the SCM client service.                                                                           | Generally required.                                       |
| `ozone.scm.names`                         | `localhost`                 | `scm-host1,scm-host2:9861`       | Comma-separated list of SCM hostnames or IPs for Datanode discovery.                                          | Generally required for cluster operation.                 |

*Example `ozone-site.xml` for a non-HA setup:*
```xml
<configuration>
  <property>
    <name>ozone.om.address</name>
    <value>your-om-host:9862</value>
    <description>The address of the Ozone Manager service.</description>
  </property>
  <property>
    <name>ozone.scm.client.address</name>
    <value>your-scm-host:9860</value>
    <description>The address of the Storage Container Manager client service.</description>
  </property>
  <property>
    <name>ozone.scm.names</name>
    <value>your-scm-host</value>
    <description>Comma separated list of SCM hostnames or IP addresses.</description>
  </property>
</configuration>
```

*Example `ozone-site.xml` for an OM HA setup:*
```xml
<configuration>
  <property>
    <name>ozone.om.service.ids</name>
    <value>myOmService</value>
  </property>
  <property>
    <name>ozone.om.nodes.myOmService</name>
    <value>om1,om2,om3</value>
  </property>
  <property>
    <name>ozone.om.address.myOmService.om1</name>
    <value>om-host1.example.com:9862</value>
  </property>
  <property>
    <name>ozone.om.address.myOmService.om2</name>
    <value>om-host2.example.com:9862</value>
  </property>
  <property>
    <name>ozone.om.address.myOmService.om3</name>
    <value>om-host3.example.com:9862</value>
  </property>
  <property>
    <name>ozone.client.failover.proxy.provider.myOmService</name>
    <value>org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider</value>
  </property>
</configuration>
```
The configuration for High Availability (HA) environments is notably more involved than for single Ozone Manager setups. In an HA configuration, clients do not connect to a single OM address. Instead, they use a logical `serviceId` which resolves to a list of OM nodes participating in the HA group. The `ozone-site.xml` must define this `serviceId`, enumerate the logical node IDs within that service, and then provide the actual address for each logical node ID. This setup allows the client to discover and failover between OM leader and follower nodes.

**2. `core-site.xml` (primarily for OFS/o3fs)**

When using the Hadoop Compatible FileSystem API (`ofs://` or `o3fs://`), you also need to configure `core-site.xml` to register the Ozone filesystem implementations and potentially set Ozone as the default filesystem.

Key properties for `core-site.xml`:

* `fs.defaultFS`: This property can be set to an Ozone path (e.g., `ofs://omservice/` or `o3fs://mybucket.myvolume/`) to make Ozone the default filesystem for Hadoop applications.
  * For `ofs://`, the path should be `ofs://<omServiceIdOrHost>/`. This allows access to any volume and bucket on the cluster using relative paths.
  * For `o3fs://`, the path is `o3fs://<bucket>.<volume>/`. This scopes all filesystem operations to the specified bucket.
    The `ofs` scheme offers a more HDFS-like global namespace experience when set as `fs.defaultFS`, as it doesn't tie the default filesystem to a single bucket.
* `fs.ofs.impl`: Set to `org.apache.hadoop.fs.ozone.RootedOzoneFileSystem` to register the `ofs` scheme implementation.
* `fs.AbstractFileSystem.o3fs.impl`: Set to `org.apache.hadoop.fs.ozone.OzFs` for the `o3fs` scheme.
* `fs.o3fs.impl`: Set to `org.apache.hadoop.fs.ozone.OzoneFileSystem` also for `o3fs`.

*Example `core-site.xml` for using `ofs` as the default filesystem:*
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>ofs://myOmService/</value>
  </property>
  <property>
    <name>fs.ofs.impl</name>
    <value>org.apache.hadoop.fs.ozone.RootedOzoneFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.OzFs</value>
  </property>
  <property>
    <name>fs.o3fs.impl</name>
    <value>org.apache.hadoop.fs.ozone.OzoneFileSystem</value>
  </property>
</configuration>
```
Ensure that both `ozone-site.xml` and `core-site.xml` (if using OFS/o3fs) are present in the classpath of your Java application.

## III. Interacting with Ozone: The Native Java Client API {#native-java-api}

The native Ozone Java client API provides a direct, RPC-based mechanism for interacting with an Ozone cluster. It offers comprehensive control over Ozone's core entities: volumes, buckets, and keys. This section details how to establish connections, manage these entities, and handle potential errors.

### A. Establishing a Connection

Connecting to Ozone programmatically involves several steps, starting with configuration and culminating in an `OzoneClient` instance.

**1. `OzoneConfiguration`**

The `org.apache.hadoop.hdds.conf.OzoneConfiguration` class is the foundation for client-side settings. It loads configurations, typically from an `ozone-site.xml` file expected to be in the application's classpath. Properties can also be set programmatically.

```java
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

//...
OzoneConfiguration conf = new OzoneConfiguration();
// If ozone-site.xml is not in classpath, or for overrides:
// For a non-HA setup:
// conf.set("ozone.om.address", "om-host.example.com:9862");
// For an HA setup, ensure ozone.om.service.ids and related properties are set.
// e.g., if ozone.om.service.ids=myOmService, the client will use this.
```
This configuration object will carry all necessary parameters, such as the Ozone Manager (OM) address or service ID for HA setups.

**2. `OzoneClientFactory`**

The `org.apache.hadoop.ozone.client.OzoneClientFactory` is the entry point for acquiring an `OzoneClient` instance. It provides methods to create clients based on the provided configuration:

* `getRpcClient(OzoneConfiguration conf)`: Explicitly requests an RPC-based client. This is generally recommended when intending to use the native RPC protocol.
* `getClient(OzoneConfiguration conf)`: Returns an appropriate client based on the broader configuration. While it can return an RPC client, it might also consider other client types if configured.

For clarity and to ensure use of the native RPC interface, `getRpcClient` is preferred.

**3. Obtaining `OzoneClient` and `ObjectStore`**

Once the `OzoneConfiguration` is prepared, an `OzoneClient` can be instantiated. From the `OzoneClient`, an `ObjectStore` instance is obtained, which serves as the primary interface for performing operations on volumes and S3-compatible buckets.

*Basic Connection Example:*
```java
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import java.io.IOException;

public class OzoneConnectExample {
    public static void main(String[] args) {
        OzoneConfiguration conf = new OzoneConfiguration();
        // Ensure ozone-site.xml is in classpath or set properties for OM connection:
        // e.g., conf.set("ozone.om.address", "your_om_host:9862"); for non-HA
        // or ensure HA properties like ozone.om.service.ids are set for HA.

        OzoneClient ozoneClient = null;
        try {
            ozoneClient = OzoneClientFactory.getRpcClient(conf);
            ObjectStore objectStore = ozoneClient.getObjectStore();
            System.out.println("Successfully connected to Ozone and got ObjectStore.");
            // Further operations (volume, bucket, key management) go here
        } catch (IOException e) {
            System.err.println("Error connecting to Ozone or getting ObjectStore: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (ozoneClient != null) {
                try {
                    ozoneClient.close(); // Crucial: Always close the client
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
```
Resource management is critical: `OzoneClient` instances manage network connections and other resources. They must be closed when no longer needed, typically in a `finally` block or using a try-with-resources statement if `OzoneClient` implemented `AutoCloseable` (which it does, as it extends `Closeable`).

**4. Secure Connection (Kerberos)**

To connect to a Kerberized Ozone cluster, specific security properties must be configured in `ozone-site.xml` (e.g., `ozone.security.enabled=true`, `hadoop.security.authentication=kerberos`, `ozone.om.kerberos.principal`). Additionally, the client application needs to authenticate using Kerberos credentials, typically via a keytab.

*Secure Connection Snippet (within the try block of the example above):*
```java
//... (OzoneConfiguration 'conf' is initialized and includes Kerberos properties)
// import org.apache.hadoop.security.UserGroupInformation;

// String omPrincipal = "om/om-host.example.com@YOUR_REALM"; // Example principal
// String keytabPathLocal = "/path/to/your/client.keytab";    // Path to client's keytab

// UserGroupInformation.setConfiguration(conf);
// UserGroupInformation.loginUserFromKeytab(omPrincipal, keytabPathLocal);

// ozoneClient = OzoneClientFactory.getRpcClient(conf);
// ObjectStore objectStore = ozoneClient.getObjectStore();
//...
```
This sequence, involving `UserGroupInformation`, must be executed before obtaining the `OzoneClient`.

### B. Managing Volumes

Volumes are the top-level organizational units in Ozone, analogous to accounts or namespaces. They are typically created and managed by administrators.

**1. `VolumeArgs` for Customization**

The `org.apache.hadoop.ozone.client.VolumeArgs` class allows for specifying custom properties when creating a volume. It uses a builder pattern for ease of use. Common properties include owner, administrator, quota (both storage space and namespace), and metadata.

*`VolumeArgs` Builder Example:*
```java
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.OzoneConsts; // For quota units like GB, TB
import org.apache.hadoop.ozone.security.acl.OzoneAcl; // For ACLs
import java.util.Arrays;

//...
VolumeArgs volumeArgs = VolumeArgs.newBuilder()
   .setOwner("finance_dept_user")        // Sets the owner of the volume
   .setAdmin("ozone_admin_group")        // Sets the administrator group/user
   .setQuota("10", OzoneConsts.QUOTA_UNIT_TB) // Sets storage space quota to 10 TB
    //.setQuota("10000", OzoneConsts.QUOTA_UNIT_BUCKETS) // Example for namespace quota (number of buckets)
   .addMetadata("department", "Finance") // Custom metadata
   .addMetadata("costCenter", "FC123")
    // Example for setting ACLs if needed, format might vary based on OzoneAcl parsing
    //.setAcls(Arrays.asList(OzoneAcl.parseAcl("user:finance_user:rw")))
   .build();
```
The `VolumeArgs` builder provides a fluent interface to define these parameters before creating the volume.

**2. Creating Volumes**

Volumes are created using the `ObjectStore` instance.

* `objectStore.createVolume(String volumeName)`: Creates a volume with default arguments.
* `objectStore.createVolume(String volumeName, VolumeArgs volumeArgs)`: Creates a volume with the specified custom properties.

*Code Example:*
```java
// Assuming objectStore is an initialized ObjectStore instance
String defaultVolumeName = "defaultUserVolume";
String customVolumeName = "financeDataLake";

try {
    objectStore.createVolume(defaultVolumeName);
    System.out.println("Volume created with default arguments: " + defaultVolumeName);

    VolumeArgs customArgs = VolumeArgs.newBuilder()
                               .setOwner("finance_user")
                               .setQuota("50", OzoneConsts.QUOTA_UNIT_TB) // 50 Terabytes
                               .build();
    objectStore.createVolume(customVolumeName, customArgs);
    System.out.println("Volume created with custom arguments: " + customVolumeName);
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.VOLUME_ALREADY_EXISTS
    System.err.println("Error creating volume: " + e.getMessage());
    e.printStackTrace();
}
```

**3. Retrieving a Volume**

To work with an existing volume or get its details, retrieve an `OzoneVolume` object.

* `OzoneVolume volume = objectStore.getVolume(String volumeName);`

This object provides methods to access volume attributes like name, owner, quota, and creation time.

*Code Example:*
```java
// Assuming objectStore and customVolumeName from previous example
try {
    OzoneVolume retrievedVolume = objectStore.getVolume(customVolumeName);
    System.out.println("Retrieved Volume Name: " + retrievedVolume.getName());
    System.out.println("Owner: " + retrievedVolume.getOwner());
    System.out.println("Admin: " + retrievedVolume.getAdmin());
    System.out.println("Quota in Bytes: " + retrievedVolume.getQuotaInBytes());
    System.out.println("Namespace Quota (max buckets): " + retrievedVolume.getQuotaInNamespace());
    System.out.println("Creation Time: " + retrievedVolume.getCreationTime());
    System.out.println("Metadata: " + retrievedVolume.getMetadata());
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.VOLUME_NOT_FOUND
    System.err.println("Error retrieving volume: " + e.getMessage());
    e.printStackTrace();
}
```

**4. Listing Volumes**

Ozone allows listing volumes, optionally filtering by a prefix. The `listVolumes` method returns an `Iterator`, which is suitable for handling potentially large numbers of volumes without loading all information into memory at once.

* `Iterator<? extends OzoneVolume> volumeIterator = objectStore.listVolumes(String volumePrefix);`
* `Iterator<? extends OzoneVolume> volumeIterator = objectStore.listVolumes(String volumePrefix, String startVolume, int maxResults);` (Common pagination pattern)

*Code Example:*
```java
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneVolume;

// Assuming objectStore is initialized
try {
    System.out.println("Listing all volumes accessible by the current user:");
    Iterator<? extends OzoneVolume> volIter = objectStore.listVolumes(null); // null prefix lists all
    while (volIter.hasNext()) {
        OzoneVolume vol = volIter.next();
        System.out.println(String.format("- Name: %s, Owner: %s, Quota: %d bytes, Created: %s",
                             vol.getName(), vol.getOwner(), vol.getQuotaInBytes(), vol.getCreationTime()));
    }

    System.out.println("\nListing volumes with prefix 'finance':");
    volIter = objectStore.listVolumes("finance");
    while (volIter.hasNext()) {
        OzoneVolume vol = volIter.next();
        System.out.println("- " + vol.getName());
    }
} catch (IOException e) {
    System.err.println("Error listing volumes: " + e.getMessage());
    e.printStackTrace();
}
```
The iterator-based approach is a standard Java pattern for efficiently handling collections that might be fetched in batches from the server.

**5. Deleting Volumes**

Volumes can be deleted using the `ObjectStore`. Typically, a volume must be empty (contain no buckets) before it can be deleted, unless a recursive deletion mechanism is invoked (which is more common via CLI tools for administrative safety).

* `objectStore.deleteVolume(String volumeName);`

*Code Example:*
```java
String volumeToDelete = "archiveVolume"; // Ensure this volume is empty
try {
    objectStore.deleteVolume(volumeToDelete);
    System.out.println("Volume deleted successfully: " + volumeToDelete);
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.VOLUME_NOT_FOUND or ResultCodes.VOLUME_NOT_EMPTY
    System.err.println("Error deleting volume: " + e.getMessage());
    e.printStackTrace();
}
```

### C. Managing Buckets

Buckets are containers for keys (objects) and reside within volumes. They are analogous to directories in a filesystem or buckets in Amazon S3.

**1. `BucketArgs` for Customization**

Similar to `VolumeArgs`, `org.apache.hadoop.ozone.client.BucketArgs` is used to specify custom properties for buckets during creation. This includes settings like versioning, storage type, encryption key, ACLs, default replication configurations, and quotas.

*`BucketArgs` Builder Example:*
```java
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto; // For StorageType
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.ozone.OzoneConsts;

//...
BucketArgs.Builder bucketArgsBuilder = BucketArgs.newBuilder();
bucketArgsBuilder.setVersioning(true); // Enable versioning for objects in this bucket
bucketArgsBuilder.setStorageType(StorageTypeProto.SSD); // Prefer SSD storage for this bucket
// For encryption, the key 'myKMSKeyForBucket' must be pre-configured in the KMS
// bucketArgsBuilder.setEncryptionKey("myKMSKeyForBucket");
// Example for setting a default replication configuration for keys in this bucket
ReplicationConfig ratisThreeReplication = ReplicationConfig.getInstance(
    ReplicationType.RATIS, ReplicationFactor.THREE);
bucketArgsBuilder.setDefaultReplicationConfig(ratisThreeReplication);
bucketArgsBuilder.setQuota("500", OzoneConsts.QUOTA_UNIT_GB); // Set bucket storage quota
// bucketArgsBuilder.setQuota("100000", OzoneConsts.QUOTA_UNIT_KEYS); // Set bucket namespace (key count) quota

BucketArgs customBucketArgs = bucketArgsBuilder.build();
```
The `BucketArgs.Builder` provides a fluent API to define these diverse properties. Features like encryption key assignment and quotas are critical for production environments and are configured via these arguments.

**2. Creating Buckets**

Buckets are created within a specific `OzoneVolume`.

* `retrievedVolume.createBucket(String bucketName);`: Creates a bucket with default arguments.
* `retrievedVolume.createBucket(String bucketName, BucketArgs bucketArgs);`: Creates a bucket with specified custom properties.

*Code Example:*
```java
// Assuming retrievedVolume is an initialized OzoneVolume object
String defaultBucketName = "rawLogs";
String customBucketName = "processedAnalytics";

try {
    retrievedVolume.createBucket(defaultBucketName);
    System.out.println("Bucket created with default arguments: " + defaultBucketName);

    BucketArgs analyticsBucketArgs = BucketArgs.newBuilder()
                                       .setVersioning(false)
                                       .setStorageType(StorageTypeProto.DISK) // Standard disk storage
                                       .build();
    retrievedVolume.createBucket(customBucketName, analyticsBucketArgs);
    System.out.println("Bucket created with custom arguments: " + customBucketName);
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.BUCKET_ALREADY_EXISTS
    System.err.println("Error creating bucket: " + e.getMessage());
    e.printStackTrace();
}
```

**3. Retrieving a Bucket**

To perform operations on keys within a bucket or to get its details, an `OzoneBucket` object is needed.

* `OzoneBucket bucket = retrievedVolume.getBucket(String bucketName);`

This object allows access to bucket attributes like name, volume name, versioning status, storage type, and creation time.

*Code Example:*
```java
// Assuming retrievedVolume and customBucketName from previous example
try {
    OzoneBucket retrievedBucket = retrievedVolume.getBucket(customBucketName);
    System.out.println("Retrieved Bucket Name: " + retrievedBucket.getName());
    System.out.println("Belongs to Volume: " + retrievedBucket.getVolumeName());
    System.out.println("Versioning Enabled: " + retrievedBucket.getVersioning());
    System.out.println("Storage Type: " + retrievedBucket.getStorageType());
    System.out.println("Encryption Key Name: " + retrievedBucket.getEncryptionKeyName());
    System.out.println("Creation Time: " + retrievedBucket.getCreationTime());
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.BUCKET_NOT_FOUND
    System.err.println("Error retrieving bucket: " + e.getMessage());
    e.printStackTrace();
}
```

**4. Listing Buckets in a Volume**

Buckets within a volume can be listed using an iterator, similar to listing volumes.

* `Iterator<? extends OzoneBucket> bucketIterator = retrievedVolume.listBuckets(String bucketPrefix);`
* `Iterator<? extends OzoneBucket> bucketIterator = retrievedVolume.listBuckets(String bucketPrefix, String startBucket, int maxResults);` (Common pagination pattern)

*Code Example:*
```java
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneBucket;

// Assuming retrievedVolume is initialized
try {
    System.out.println("Listing all buckets in volume '" + retrievedVolume.getName() + "':");
    Iterator<? extends OzoneBucket> bucketIter = retrievedVolume.listBuckets(null); // null prefix lists all
    while (bucketIter.hasNext()) {
        OzoneBucket b = bucketIter.next();
        System.out.println(String.format("- Name: %s, Versioning: %b, StorageType: %s, Created: %s",
                             b.getName(), b.getVersioning(), b.getStorageType(), b.getCreationTime()));
    }
} catch (IOException e) {
    System.err.println("Error listing buckets: " + e.getMessage());
    e.printStackTrace();
}
```

**5. Deleting Buckets**

Buckets can be deleted from a volume. Typically, a bucket must be empty (contain no keys or active multipart uploads) before it can be deleted.

* `retrievedVolume.deleteBucket(String bucketName);` (Method on `OzoneVolume` is symmetric to `createBucket` and `getBucket`)

*Code Example:*
```java
String bucketToDelete = "temporaryUploads"; // Ensure this bucket is empty
try {
    retrievedVolume.deleteBucket(bucketToDelete);
    System.out.println("Bucket deleted successfully: " + bucketToDelete);
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.BUCKET_NOT_FOUND or ResultCodes.BUCKET_NOT_EMPTY
    System.err.println("Error deleting bucket: " + e.getMessage());
    e.printStackTrace();
}
```

### D. Managing Keys (Objects)

Keys are the fundamental data units in Ozone, representing the files or objects stored within buckets.

**1. Writing/Uploading Keys**

Data is written to keys using output streams. Ozone provides `OzoneOutputStream` for byte array-based writes and `OzoneDataStreamOutput` for `ByteBuffer`-based streaming writes.

* Using `OzoneOutputStream`:
  * `OzoneOutputStream outputStream = retrievedBucket.createKey(String keyName, long sizeInBytes);`
  * `OzoneOutputStream outputStream = retrievedBucket.createKey(String keyName, long sizeInBytes, ReplicationConfig replicationConfig, Map<String, String> metadata);` This more complete signature allows specifying replication and custom metadata.
  * `outputStream.write(byte[] data);`
  * `outputStream.close();` This step is crucial as it finalizes the key creation and flushes data.

* Using `OzoneDataStreamOutput` (for `ByteBuffer`):
  * `OzoneDataStreamOutput dataStreamOutput = retrievedBucket.createStreamKey(String keyName, long sizeInBytes, ReplicationConfig replicationConfig, Map<String, String> metadata);`
  * `dataStreamOutput.write(ByteBuffer buffer);`
  * `dataStreamOutput.close();`

*Code Example (using `OzoneOutputStream`):*
```java
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
// Assuming retrievedBucket is an initialized OzoneBucket object
// Assuming ratisThreeReplication is a valid ReplicationConfig object

String keyName = "documents/report.txt";
String content = "This is the content of the report.";
byte[] dataBytes = content.getBytes(StandardCharsets.UTF_8);
Map<String, String> metadata = new HashMap<>();
metadata.put("contentType", "text/plain");
metadata.put("author", "datascience_team");

// Using try-with-resources ensures the stream is closed
try (OzoneOutputStream out = retrievedBucket.createKey(keyName, dataBytes.length,
                                                      retrievedBucket.getDefaultReplicationConfig(), // Or a custom ReplicationConfig
                                                      metadata)) {
    out.write(dataBytes);
    System.out.println("Key created and data written: " + keyName);
} catch (IOException e) {
    System.err.println("Error writing key: " + e.getMessage());
    e.printStackTrace();
}
```
Failing to close the output stream can result in incomplete or corrupted keys. The try-with-resources statement is highly recommended for managing stream lifecycles.

**2. Reading Keys**

Keys are read using `OzoneInputStream`.

* `OzoneInputStream inputStream = retrievedBucket.readKey(String keyName);` 
* `inputStream.read(byte[] buffer);`
* `inputStream.close();`

*Code Example:*
```java
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.OzoneKeyDetails; // To get key size for buffer allocation
import java.nio.charset.StandardCharsets;

// Assuming retrievedBucket and keyName from previous example
try {
    // It's often good practice to get key details first to know its size
    OzoneKeyDetails keyDetails = retrievedBucket.getKey(keyName);
    if (keyDetails == null) {
        System.err.println("Key not found: " + keyName);
        return; // or throw new FileNotFoundException
    }
    byte[] buffer = new byte[(int) keyDetails.getDataSize()]; // Allocate buffer based on key size

    // Using try-with-resources for the input stream
    try (OzoneInputStream in = retrievedBucket.readKey(keyName)) {
        int bytesRead = in.read(buffer);
        System.out.println("Successfully read " + bytesRead + " bytes from key: " + keyName);
        System.out.println("Content: " + new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
    }
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.KEY_NOT_FOUND
    System.err.println("Error reading key: " + e.getMessage());
    e.printStackTrace();
}
```

**3. Retrieving Key Information**

To get metadata about a key without reading its content, use the `getKey` method.

* `OzoneKeyDetails keyDetails = retrievedBucket.getKey(String keyName);` (Method inferred from typical API patterns, CLI `key info` , and usage in read example above)
  The `OzoneKeyDetails` object contains information such as data size, creation/modification times, replication type and factor, and block locations.

*Code Example (integrated into the read example above).*

**4. Listing Keys in a Bucket**

Keys within a bucket can be listed using an iterator, with options for prefix filtering and pagination.

* `Iterator<? extends OzoneKey> keyIterator = retrievedBucket.listKeys(String keyPrefix);`
* `Iterator<? extends OzoneKey> keyIterator = retrievedBucket.listKeys(String keyPrefix, String startKey);` (Common pagination: start after `startKey`)
* `Iterator<? extends OzoneKey> keyIterator = retrievedBucket.listKeys(String keyPrefix, String startKey, boolean shallow);` (The `shallow` parameter can be used to list only immediate children if the delimiter is also specified, useful for emulating directory structures).

*Code Example:*
```java
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneKey;

// Assuming retrievedBucket is initialized
try {
    System.out.println("Listing all keys in bucket '" + retrievedBucket.getName() + "':");
    Iterator<? extends OzoneKey> keyIter = retrievedBucket.listKeys(null); // null prefix lists all keys
    while (keyIter.hasNext()) {
        OzoneKey key = keyIter.next();
        System.out.println(String.format("- Name: %s, Size: %d bytes, Created: %s",
                             key.getName(), key.getDataSize(), key.getCreationTime()));
    }

    System.out.println("\nListing keys with prefix 'documents/':");
    keyIter = retrievedBucket.listKeys("documents/");
    while (keyIter.hasNext()) {
        OzoneKey key = keyIter.next();
        System.out.println("- " + key.getName());
    }
} catch (IOException e) {
    System.err.println("Error listing keys: " + e.getMessage());
    e.printStackTrace();
}
```

**5. Deleting Keys**

Keys can be deleted from a bucket.

* `retrievedBucket.deleteKey(String keyName);`

*Code Example:*
```java
String keyToDelete = "archive/old_report.txt";
try {
    retrievedBucket.deleteKey(keyToDelete);
    System.out.println("Key deleted successfully: " + keyToDelete);
} catch (IOException e) {
    // Handle specific OMException like ResultCodes.KEY_NOT_FOUND
    System.err.println("Error deleting key: " + e.getMessage());
    e.printStackTrace();
}
```

### E. Error Handling

Robust applications must handle potential errors during interactions with Ozone. Operations can throw `java.io.IOException` or more specific Ozone subclasses.

Key exceptions include:

* **`org.apache.hadoop.ozone.om.exceptions.OMException`**: This is a common exception type for errors originating from the Ozone Manager. It provides a `getResultCode()` method which returns a `ResultCodes` enum value, allowing for fine-grained error handling based on the specific issue encountered (e.g., `VOLUME_NOT_FOUND`, `BUCKET_ALREADY_EXISTS`, `KEY_NOT_FOUND`, `PERMISSION_DENIED`).
* **`org.apache.hadoop.hdds.scm.storage.StorageContainerException`**: Errors related to storage containers on DataNodes might manifest as this exception, or be wrapped within a more general `IOException` by the client.
* **Generic `java.io.IOException`**: Can be thrown for various reasons, including network connectivity problems, timeouts, or other underlying I/O failures.

It is crucial to catch these exceptions and, particularly for `OMException`, inspect the `ResultCode` to understand the nature of the error and respond appropriately.

*Code Example (Illustrating `OMException` handling):*
```java
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.proto.OzoneManagerProtocolProtos.ResultCodes; // For specific error codes

//... (within a method interacting with Ozone)
try {
    // Example: Attempt to get a non-existent volume
    OzoneVolume volume = objectStore.getVolume("phantomVolume");
    //... further operations...
} catch (OMException ome) {
    System.err.println("Ozone Manager operation failed: " + ome.getMessage());
    System.err.println("OM Error Code: " + ome.getResultCode());

    if (ome.getResultCode() == ResultCodes.VOLUME_NOT_FOUND) {
        System.err.println("Detailed: The specified volume does not exist. Check volume name or creation status.");
    } else if (ome.getResultCode() == ResultCodes.PERMISSION_DENIED) {
        System.err.println("Detailed: Permission denied. Verify client credentials and ACLs on the Ozone resource.");
    } else if (ome.getResultCode() == ResultCodes.TIMEOUT) {
        System.err.println("Detailed: The operation timed out. Check network connectivity and OM load.");
    }
    // Log the full stack trace for debugging
    ome.printStackTrace();
} catch (IOException ioe) {
    // Catch other IOExceptions that are not OMExceptions
    System.err.println("A general I/O error occurred: " + ioe.getMessage());
    ioe.printStackTrace();
}
```
This pattern of catching `OMException` specifically and then a general `IOException` allows for targeted error recovery or reporting while ensuring that broader I/O issues are also handled.

## IV. Leveraging the Hadoop Compatible FileSystem API (OFS & o3fs) {#hcfs-api}

Apache Ozone provides a Hadoop Compatible FileSystem (HCFS) interface, enabling applications built for HDFS or other HCFS-compliant filesystems to run on Ozone with minimal to no code changes. This is achieved through the `ofs://` (Ozone File System) and `o3fs://` (Ozone Object File System) schemes, which implement the `org.apache.hadoop.fs.FileSystem` API.
See page [OFS](Ofs.md) and [O3FS](O3fs.md) for more details.


**1. Obtaining a `org.apache.hadoop.fs.FileSystem` Instance**

To use either scheme, a standard Hadoop `org.apache.hadoop.fs.FileSystem` instance is obtained via the static `FileSystem.get(URI, Configuration)` method. The `Configuration` object passed must be loaded with the necessary settings from `core-site.xml` (to register the Ozone filesystem implementations and optionally set `fs.defaultFS`) and `ozone-site.xml` (to specify the Ozone Manager address or HA service ID).

*Code Example (Instantiating `FileSystem` for Ozone):*
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import java.io.IOException;

public class OzoneFSClientExample {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // Ensure core-site.xml and ozone-site.xml are in the classpath.
        // Alternatively, properties can be set programmatically:

        // For OFS (RootedOzoneFileSystem)
        // conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
        // conf.set("fs.defaultFS", "ofs://your_om_service_id_or_host/"); // Optional default

        // For o3fs (OzoneFileSystem)
        // conf.set("fs.AbstractFileSystem.o3fs.impl", "org.apache.hadoop.fs.ozone.OzFs");
        // conf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem");
        // conf.set("fs.defaultFS", "o3fs://yourbucket.yourvolume/"); // Optional default

        // Common Ozone OM configuration (if not in ozone-site.xml)
        // conf.set("ozone.om.address", "your_om_host:9862"); // For non-HA
        // Or HA service ID properties for HA setups.

        FileSystem ofsFs = null;
        FileSystem o3fsFs = null;

        try {
            // Example for OFS:
            // Assumes 'myOmService' is an OM HA service ID configured in ozone-site.xml
            URI ofsUri = URI.create("ofs://myOmService/salesVolume/quarter1Bucket/");
            ofsFs = FileSystem.get(ofsUri, conf);
            System.out.println("Successfully obtained OFS FileSystem for: " + ofsFs.getUri());
            System.out.println("OFS Working Directory: " + ofsFs.getWorkingDirectory());

            // Example for o3fs:
            // Assumes 'logsBucket.appVolume' is an existing bucket and volume
            URI o3fsUri = URI.create("o3fs://logsBucket.appVolume/");
            o3fsFs = FileSystem.get(o3fsUri, conf);
            System.out.println("Successfully obtained o3fs FileSystem for: " + o3fsFs.getUri());
            System.out.println("o3fs Working Directory: " + o3fsFs.getWorkingDirectory());

            // Perform file operations using ofsFs or o3fsFs...

        } catch (IOException e) {
            System.err.println("Error obtaining FileSystem for Ozone: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // It's good practice to close FileSystem instances when done
            if (ofsFs != null) {
                try {
                    ofsFs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (o3fsFs != null) {
                try {
                    o3fsFs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
```
This example demonstrates obtaining `FileSystem` objects for both `ofs` and `o3fs` schemes.

### B. Common FileSystem Operations

Once a `FileSystem` instance for Ozone is obtained, applications can use the standard Hadoop `FileSystem` API methods for common operations.

* **Creating Directories:** `boolean mkdirs(Path p)`
  * For `ofs://`, this can also create volumes and buckets if the path structure corresponds to `/volumeName` or `/volumeName/bucketName` and they don't already exist. This is a powerful feature allowing filesystem-like administration.
  * *Example (OFS):*
      ```java
      // Assuming ofsFs is an initialized FileSystem for ofs
      // Path dirPath = new Path("ofs://myOmService/newVolume/newBucket/dataDirectory");
      // Or, if fs.defaultFS="ofs://myOmService/":
      Path dirPath = new Path("/newVolume/newBucket/dataDirectory");
      try {
          if (ofsFs.mkdirs(dirPath)) {
              System.out.println("OFS Directory (and potentially volume/bucket) created: " + dirPath);
          } else {
              System.out.println("OFS Directory already exists or creation failed: " + dirPath);
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

* **Creating and Writing Files:** `FSDataOutputStream create(Path p, boolean overwrite)`
  * Returns an `FSDataOutputStream` to write data to the specified path.
  * *Example (o3fs):*
      ```java
      import org.apache.hadoop.fs.FSDataOutputStream;
      // Assuming o3fsFs is an initialized FileSystem for o3fs (e.g., for o3fs://logs.prodVolume/)
      Path filePath = new Path("/application_logs/app_20231026.log"); // Relative to the o3fs root bucket
      try (FSDataOutputStream out = o3fsFs.create(filePath, true)) { // true to overwrite if exists
          out.writeUTF("Log entry: Application started.\n");
          out.writeUTF("Log entry: Processing data batch 123.\n");
          System.out.println("File written to o3fs: " + o3fsFs.makeQualified(filePath));
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

* **Opening and Reading Files:** `FSDataInputStream open(Path p)`
  * Returns an `FSDataInputStream` to read data from the specified path.
  * *Example (ofs):*
      ```java
      import org.apache.hadoop.fs.FSDataInputStream;
      import java.io.BufferedReader;
      import java.io.InputStreamReader;
      // Assuming ofsFs and the file from previous mkdirs example exists
      // Path fileToRead = new Path("/newVolume/newBucket/dataDirectory/sample.txt");
      // (Assuming sample.txt was written there)
      // For this example, let's assume a file was written to ofs://myOmService/salesVolume/quarter1Bucket/report.txt
      Path fileToRead = new Path("ofs://myOmService/salesVolume/quarter1Bucket/report.txt");
      try (FSDataInputStream in = ofsFs.open(fileToRead);
           BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
          String line;
          System.out.println("Content of " + fileToRead + ":");
          while ((line = reader.readLine()) != null) {
              System.out.println(line);
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

* **Listing Directory Contents:** `FileStatus[] listStatus(Path p)`
  * Returns an array of `FileStatus` objects representing files and directories under path `p`.
  * *Example (o3fs):*
      ```java
      import org.apache.hadoop.fs.FileStatus;
      // Assuming o3fsFs is initialized (e.g., for o3fs://logs.prodVolume/)
      Path dirToList = new Path("/application_logs/"); // List contents of application_logs directory in the bucket
      try {
          FileStatus[] statuses = o3fsFs.listStatus(dirToList);
          System.out.println("Listing for " + o3fsFs.makeQualified(dirToList) + ":");
          for (FileStatus status : statuses) {
              System.out.println((status.isDirectory() ? "d " : "- ") + status.getPath().getName() +
                                 " (Size: " + status.getLen() + ")");
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

* **Getting File/Directory Information:** `FileStatus getFileStatus(Path p)`
  * Returns a `FileStatus` object containing metadata about the file or directory at path `p`.
  * *Example (ofs):*
      ```java
      // Assuming ofsFs and dirPath from mkdirs example
      try {
          FileStatus status = ofsFs.getFileStatus(dirPath);
          System.out.println("Status for " + dirPath + ":");
          System.out.println("Is Directory: " + status.isDirectory());
          System.out.println("Modification Time: " + status.getModificationTime());
          System.out.println("Owner: " + status.getOwner());
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

* **Deleting Files/Directories:** `boolean delete(Path p, boolean recursive)`
  * Deletes the file or directory at path `p`. If `recursive` is true, non-empty directories will be deleted.
  * *Example (o3fs):*
      ```java
      // Assuming o3fsFs and filePath from create example
      try {
          if (o3fsFs.delete(filePath, false)) { // false: don't delete if it's a non-empty directory
              System.out.println("File deleted from o3fs: " + o3fsFs.makeQualified(filePath));
          } else {
              System.out.println("Failed to delete from o3fs: " + o3fsFs.makeQualified(filePath));
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
      ```

### C. Complete Runnable Example (using OFS)

The following is a self-contained example demonstrating common operations using the `ofs://` scheme with the Hadoop `FileSystem` API. Ensure `core-site.xml` and `ozone-site.xml` are configured and in the classpath, and the necessary Maven dependency (`ozone-filesystem-hadoop3-client`) is included.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class CompleteOFSExample {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // Ensure core-site.xml and ozone-site.xml are in classpath.
        // Example: fs.defaultFS=ofs://myOmService/
        // Example: ozone.om.service.ids=myOmService (and other HA props)

        URI baseOzoneUri;
        try {
            // Using fs.defaultFS if set, otherwise specify the full URI.
            String defaultFs = conf.get("fs.defaultFS");
            if (defaultFs != null && defaultFs.startsWith("ofs://")) {
                baseOzoneUri = URI.create(defaultFs);
            } else {
                // Fallback or specific URI if fs.defaultFS is not set to OFS
                baseOzoneUri = URI.create("ofs://myOmService/"); // Replace with your OM Service ID or host
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid URI for Ozone OFS: " + e.getMessage());
            return;
        }

        try (FileSystem fs = FileSystem.get(baseOzoneUri, conf)) {
            System.out.println("Successfully connected to Ozone OFS: " + fs.getUri());

            Path volumePath = new Path("/myJavaVolume"); // Will create volume 'myJavaVolume'
            Path bucketPath = new Path(volumePath, "myJavaBucket"); // Will create bucket 'myJavaBucket' in 'myJavaVolume'
            Path dirPath = new Path(bucketPath, "myDir");
            Path filePath = new Path(dirPath, "myFile.txt");
            String fileContent = "Hello Ozone via OFS Java API!";

            // 1. Create directory (implicitly creates volume and bucket if they don't exist via OFS)
            if (fs.mkdirs(dirPath)) {
                System.out.println("Directory created: " + dirPath);
            } else {
                System.out.println("Directory already exists or failed to create: " + dirPath);
            }

            // 2. Write a file
            try (FSDataOutputStream out = fs.create(filePath, true)) {
                out.write(fileContent.getBytes(StandardCharsets.UTF_8));
                System.out.println("File written: " + filePath);
            }

            // 3. Read the file
            try (FSDataInputStream in = fs.open(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                System.out.println("Reading file content from: " + filePath);
                String line = reader.readLine();
                System.out.println("Content: " + line);
                if (!fileContent.equals(line)) {
                    System.err.println("Error: File content mismatch!");
                }
            }

            // 4. List contents of the directory
            System.out.println("Listing contents of: " + dirPath);
            FileStatus[] statuses = fs.listStatus(dirPath);
            for (FileStatus status : statuses) {
                System.out.println(" - " + status.getPath().getName() + (status.isDirectory() ? "" : " [FILE]") +
                                   ", Size: " + status.getLen());
            }

            // 5. Delete the file
            if (fs.delete(filePath, false)) {
                System.out.println("File deleted: " + filePath);
            } else {
                System.out.println("Failed to delete file: " + filePath);
            }

            // 6. Delete the directory (and its parent bucket and volume if desired and empty)
            // For this example, just deleting the innermost directory.
            // Recursive delete for directories is needed if they contain other files/dirs.
            if (fs.delete(dirPath, true)) { // true for recursive if it might contain other items
                System.out.println("Directory deleted: " + dirPath);
            } else {
                System.out.println("Failed to delete directory: " + dirPath);
            }
            
            // To delete bucket and volume, they must be empty.
            // if (fs.delete(bucketPath, true)) System.out.println("Bucket deleted: " + bucketPath);
            // if (fs.delete(volumePath, true)) System.out.println("Volume deleted: " + volumePath);


        } catch (IOException e) {
            System.err.println("An error occurred with Ozone OFS: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```
This complete example provides a practical template for developers to start using the Hadoop `FileSystem` API with Ozone. Test classes within the Ozone codebase, such as `TestOzoneFileSystem.java`, also offer valuable insights into various API usages and expected behaviors, although they are not direct user examples.

## V. Conclusion

### A. Summary of Key Takeaways

Apache Ozone offers robust and flexible Java client APIs for application integration. Developers can choose between:
1.  The **Native Ozone Client API**, which provides high-performance, RPC-based access to the full suite of Ozone's object storage features, including detailed management of volumes, buckets, and keys with customizable arguments.
2.  The **Hadoop Compatible FileSystem API** (via `ofs://` and `o3fs://` schemes), which allows existing Hadoop ecosystem applications to leverage Ozone as a storage backend with minimal to no code changes, preserving investments in HDFS-based tools and workflows.

Effective use of these APIs requires proper environment setup, including correct Maven dependencies (`ozone-client` for native, `ozone-filesystem-hadoop3-client` for HCFS) and accurate client-side configurations in `ozone-site.xml` and `core-site.xml`. Special attention should be paid to HA configurations and the implications of shaded vs. non-shaded JARs for compatibility.

Thorough error handling, particularly by inspecting `OMException` result codes, and diligent resource management (e.g., closing client and stream objects) are crucial for building resilient and stable applications.

### B. Pointers to Further Resources

For continued learning and deeper exploration of Apache Ozone, the following resources are recommended:

* **Apache Ozone Javadoc:** For detailed API specifications of all public Java classes and methods. (Refer to the Ozone website for the direct link to the latest Javadoc).
* **Ozone Community Channels:**
  * **Mailing Lists:** `dev@ozone.apache.org` for development discussions and user support.
  * **Slack:** The `#ozone` channel on the official ASF Slack instance.
  * **GitHub Discussions:** For questions, community syncs, and feature discussions.
* **Apache Ozone Source Code:** The GitHub repository (`apache/ozone`) contains the source code, including examples and test cases that can provide further insight into API usage.

By leveraging these resources and the guidance provided in this document, Java developers can successfully integrate their applications with Apache Ozone, harnessing its capabilities as a scalable and distributed object store.

