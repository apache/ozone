---
title: Delta Sharing Protocol Support for Apache Ozone
summary: Enable Apache Ozone to expose stored data via the Delta Sharing protocol
date: 2026-06-22
jira: HDDS-15642
status: proposed
author: Abhishek Pal
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Delta Sharing Protocol Support for Apache Ozone - Design Doc

## Table of Contents
1. [Motivation](#1-motivation)
2. [Background](#2-background)
3. [Proposal](#3-proposal)
   * [3.1 High-Level Architecture](#31-high-level-architecture)
   * [3.2 Concept Mapping](#32-concept-mapping-ozone-to-delta-sharing)
   * [3.3 REST API Implementation](#33-rest-api-implementation)
   * [3.4 Data Access Modes](#34-data-access-modes)
   * [3.5 Authentication and Authorization](#35-authentication-and-authorization)
   * [3.6 Configuration](#36-configuration)
   * [3.7 Metadata Management](#37-metadata-management)
4. [Implementation Phases](#4-implementation-phases)
5. [Future Work: Beyond S3 and Parquet](#5-future-work-beyond-s3-and-parquet)
   * [5.1 Apache Iceberg Table Support](#51-apache-iceberg-table-support)
   * [5.2 Additional Data Formats](#52-additional-data-formats-orc-avro-csv-json)
   * [5.3 Raw Data Sharing from Ozone](#53-raw-data-sharing-from-ozone)
   * [5.4 Native Ozone Protocol Access](#54-native-ozone-protocol-access-beyond-s3)
   * [5.5 Cross-Cluster and Federation](#55-cross-cluster-and-federation)
6. [Open Questions](#6-open-questions)

---

## 1. Motivation

Organizations often store large datasets in Apache Ozone and need to share that data with partners, teams, or analytics platforms (such as Databricks, Apache Spark, Pandas, or Tableau) without copying the data. Today, sharing data stored in Ozone requires either:

1. Moving data out of Ozone into a cloud storage service that supports Delta Sharing.
2. Setting up custom data pipelines to export and import data.
3. Giving direct S3/Ozone access credentials to the consumer, which is hard to audit and govern.

All of these approaches have drawbacks: data duplication, stale copies, operational cost, and security risk.

**Delta Sharing** is an open protocol for secure, real-time exchange of large datasets. It is a simple REST protocol that allows a data provider to share access to datasets stored in cloud storage systems (S3, ADLS, GCS) without requiring the data consumer to run any specific platform. It uses pre-signed URLs or temporary credentials so consumers can read data directly from storage.

By implementing a Delta Sharing server on top of Ozone, we can:
- Allow Ozone users to share datasets with consumers who use Pandas, Spark, Databricks, Tableau, or any other Delta Sharing client.
- Avoid data movement: the consumer reads data directly from Ozone via pre-signed URLs or temporary S3 credentials.
- Provide fine-grained access control through bearer-token-based authorization.
- Position Ozone as a first-class data lakehouse storage platform.

This is similar to what MinIO has done with their AIStor product, where they integrated the Delta Sharing protocol natively into their storage layer to enable direct connectivity with Databricks without data replication.

---

## 2. Background

### 2.1 Delta Sharing Protocol Summary

Delta Sharing is defined at [https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md).

Key concepts:
- **Share**: A logical grouping of data shared with one or more recipients.
- **Schema**: A logical grouping of tables within a share.
- **Table**: A Delta Lake table (or a view on top of one). Data is stored as Parquet files.
- **Recipient**: A principal that holds a bearer token to access shared tables.
- **Sharing Server**: A server that implements the Delta Sharing REST APIs.

The protocol supports two access modes:
- **URL-based access (`url`)**: Server returns pre-signed URLs for individual Parquet data files. The client fetches files directly from storage using those URLs.
- **Directory-based access (`dir`)**: Server issues temporary cloud credentials (for example, AWS STS tokens) so the client can read from the table root directory. The client reads the Delta log and data files using the storage API.

A recipient connects to the sharing server using a **profile file** which contains the server endpoint and a bearer token.

### 2.2 Apache Ozone S3 Gateway

Ozone already exposes an S3-compatible API via its S3 Gateway component (`hadoop-ozone/s3gateway`). The S3 Gateway:
- Translates S3 API calls to Ozone internal operations.
- Supports GET/PUT/DELETE of objects, multipart upload, listing, etc.
- Supports AWS Signature V4 authentication.
- Can generate S3 pre-signed URLs for objects via its REST interface.

### 2.3 Ozone Namespace

Ozone organizes data as:
```
Volume -> Bucket -> Key (object path)
```

A bucket can be configured with different layouts:
- **OBS (Object Store)**: Flat namespace, S3-compatible.
- **FSO (File System Optimized)**: Hierarchical namespace with directory support.

---

## 3. Proposal

### 3.1 High-Level Architecture

We propose adding a new **Delta Sharing Gateway** service to Ozone. This service:
1. Implements the Delta Sharing REST API specification.
2. Manages share/schema/table metadata (which Ozone keys are exposed, to whom).
3. Translates table read requests into pre-signed URLs or temporary S3 credentials for Ozone objects.
4. Authenticates recipients via bearer tokens.

```
                           +---------------------------+
                           |   Delta Sharing Client    |
                           |  (Spark, Pandas, Tableau) |
                           +------------+--------------+
                                        |
                                        | REST API (Bearer Token)
                                        v
                           +---------------------------+
                           |  Delta Sharing Gateway    |
                           |  (new Ozone service)      |
                           |                           |
                           |  - Share/Schema/Table     |
                           |    metadata management    |
                           |  - Token authentication   |
                           |  - Pre-signed URL gen     |
                           +------+----------+---------+
                                  |          |
                    +-------------+          +------------+
                    |                                     |
                    v                                     v
          +-----------------+                   +-----------------+
          | Ozone Manager   |                   |  S3 Gateway     |
          | (metadata)      |                   | (pre-sign URLs) |
          +-----------------+                   +-----------------+
                    |
                    v
          +-----------------+
          |   Datanodes     |
          |   (data)        |
          +-----------------+
```

**Data flow for a table read request:**
1. Client sends `POST /shares/{share}/schemas/{schema}/tables/{table}/query` with a bearer token.
2. Delta Sharing Gateway validates the token against its recipient registry.
3. Gateway looks up the table configuration to find the Ozone bucket and key prefix where the Parquet/Delta files are stored.
4. Gateway lists the relevant data files (from the Delta log `_delta_log/` or by listing Parquet files).
5. For each file, the gateway generates a pre-signed URL using Ozone's S3 API.
6. Gateway returns the protocol, metadata, and file list in the ndjson response format.
7. Client downloads files directly from Ozone S3 Gateway using the pre-signed URLs.

### 3.2 Concept Mapping: Ozone to Delta Sharing

| Delta Sharing Concept | Ozone Mapping |
|:----------------------|:--------------|
| Share | A named sharing configuration (stored in gateway metadata) |
| Schema | Mapped to an Ozone volume or a logical namespace within a share |
| Table | An Ozone bucket + key prefix containing Delta/Parquet data |
| Recipient | A bearer token entry with permissions to one or more shares |
| Data files | Parquet files stored as Ozone keys under a bucket prefix |
| Delta log | `_delta_log/` directory under the table prefix in Ozone |

Example mapping:
```
Share: "analytics_share"
  Schema: "sales_data"     -> Ozone volume: vol1
    Table: "transactions"  -> Ozone bucket: vol1/sales-bucket, prefix: tables/transactions/
    Table: "customers"     -> Ozone bucket: vol1/sales-bucket, prefix: tables/customers/
```

### 3.3 REST API Implementation

The Delta Sharing Gateway will implement the following REST endpoints (as defined by the protocol):

| API | Method | Path | Description |
|:----|:-------|:-----|:------------|
| List Shares | GET | `/delta-sharing/shares` | List all shares the recipient can access |
| Get Share | GET | `/delta-sharing/shares/{share}` | Get metadata for a single share |
| List Schemas | GET | `/delta-sharing/shares/{share}/schemas` | List schemas in a share |
| List Tables | GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables` | List tables in a schema |
| List All Tables | GET | `/delta-sharing/shares/{share}/all-tables` | List all tables in a share |
| Query Table Version | GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/version` | Get the current table version |
| Query Table Metadata | GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/metadata` | Get the table schema and metadata |
| Read Table Data | POST | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/query` | Read data files from a table |
| Read Change Data Feed | GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/changes` | Read change data feed |
| Generate Temp Credentials | POST | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/temporary-table-credentials` | Get temporary S3 credentials |

All APIs use `Authorization: Bearer {token}` for authentication.

The response format follows the protocol specification — ndjson (newline-delimited JSON) with protocol, metadata, and file action objects.

### 3.4 Data Access Modes

We plan to support both access modes:

#### 3.4.1 URL-Based Access (Phase 1)

This is the simpler mode and will be implemented first.

For each data file in the table, the server generates a pre-signed S3 URL using Ozone's S3 API. The client uses these URLs to download files directly.

**Pre-signed URL generation:**
- The gateway authenticates itself to Ozone using a service principal.
- For each file path, it generates a time-limited pre-signed GET URL.
- The URL expiration time is configurable (default: 1 hour).
- The response includes `expirationTimestamp` for each file so clients know when to refresh.

Example response:
```json
{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"table-uuid","format":{"provider":"parquet"},"schemaString":"{...}","partitionColumns":["date"]}}
{"file":{"url":"https://ozone-s3:9878/sales-bucket/tables/transactions/date=2024-01-15/part-00000.snappy.parquet?X-Amz-Signature=...","id":"abc123","partitionValues":{"date":"2024-01-15"},"size":104857600,"stats":"{\"numRecords\":50000}"}}
```

#### 3.4.2 Directory-Based Access (Phase 2)

For advanced clients that can read the Delta log directly, the server can issue temporary S3 credentials scoped to the table's key prefix.

This requires Ozone's STS (Security Token Service) support to generate short-lived credentials with a policy that restricts read access to the specific prefix.

**Credential generation:**
- The gateway calls Ozone's STS-like mechanism to get temporary access keys.
- The credentials are scoped to read-only access on the table's key prefix.
- Clients use these credentials with any S3-compatible SDK to read the Delta log and data files directly.

Example response:
```json
{
  "credentials": {
    "location": "s3a://sales-bucket/tables/transactions",
    "awsTempCredentials": {
      "accessKeyId": "...",
      "secretAccessKey": "...",
      "sessionToken": "..."
    },
    "expirationTime": 1718298900000
  }
}
```

### 3.5 Authentication and Authorization

#### Recipient Authentication

Recipients authenticate using bearer tokens. The gateway maintains a token registry:

```yaml
recipients:
  - name: "data-science-team"
    token: "generated-secure-token"
    shares: ["analytics_share", "ml_share"]
    expirationTime: "2026-12-31T23:59:59Z"
  - name: "partner-org"
    token: "another-secure-token"
    shares: ["partner_share"]
```

#### Service Authentication to Ozone

The Delta Sharing Gateway itself authenticates to Ozone (OM and S3 Gateway) using a dedicated service user. This service user needs read access to all keys that are shared via the protocol.

### 3.6 Configuration

#### Gateway Configuration

```xml
<!-- ozone-site.xml -->

<!-- Enable the Delta Sharing Gateway -->
<property>
  <name>ozone.delta.sharing.enabled</name>
  <value>true</value>
</property>

<!-- HTTP port for the Delta Sharing Gateway -->
<property>
  <name>ozone.delta.sharing.http.port</name>
  <value>8990</value>
</property>

<!-- Pre-signed URL expiration time in seconds -->
<property>
  <name>ozone.delta.sharing.url.expiration.seconds</name>
  <value>3600</value>
</property>

<!-- Path to share configuration file -->
<property>
  <name>ozone.delta.sharing.config.path</name>
  <value>/etc/ozone/delta-sharing.yaml</value>
</property>

<!-- Service identity for accessing Ozone data -->
<property>
  <name>ozone.delta.sharing.service.id</name>
  <value>delta-sharing-service</value>
</property>
```

#### Share Configuration File

Shares, schemas, tables, and recipients are defined in a YAML configuration file:

```yaml
version: 1

shares:
  - name: "analytics_share"
    id: "550e8400-e29b-41d4-a716-446655440000"
    comment: "Analytics datasets for the data science team"
    schemas:
      - name: "sales"
        tables:
          - name: "transactions"
            location: "o3://vol1/sales-bucket/tables/transactions"
            format: "delta"
          - name: "customers"
            location: "o3://vol1/sales-bucket/tables/customers"
            format: "parquet"
      - name: "marketing"
        tables:
          - name: "campaigns"
            location: "o3://vol1/marketing-bucket/tables/campaigns"
            format: "delta"

recipients:
  - name: "data-science-team"
    bearerToken: "${generated_token}"
    shares: ["analytics_share"]
    expirationTime: "2027-01-01T00:00:00Z"
```

### 3.7 Metadata Management

#### Delta Table Metadata

For tables in Delta format, the gateway reads the `_delta_log/` directory to determine:
- Table schema (from the metadata action in the Delta log).
- Table version (latest commit number).
- Active files (from the checkpoint and subsequent commits).
- Partition columns.

For tables in plain Parquet format (no Delta log), the gateway:
- Infers schema from one of the Parquet file footers.
- Assigns version 0 (static table).
- Lists all Parquet files as the file set.

#### Table Version Tracking

The gateway caches table metadata and refreshes it when:
- A `version` API call arrives and the cached version is stale (based on a configurable TTL).
- A `query` or `metadata` API call arrives.

Caching avoids repeatedly reading the Delta log from Ozone on every request.

---

## 4. Implementation Phases

### Phase 1: Core Protocol with URL-Based Access

**Goal:** A working Delta Sharing server that can share Parquet files stored in Ozone via pre-signed URLs.

**Scope:**
- New Maven module: `hadoop-ozone/delta-sharing` (JAX-RS based HTTP service).
- REST API implementation for: List Shares, Get Share, List Schemas, List Tables, Query Table Version, Query Table Metadata, Read Table Data.
- Bearer token authentication.
- Pre-signed URL generation for file access.
- File-based share configuration (YAML).
- Support for plain Parquet tables (schema inferred from Parquet footer).
- Unit tests for all API endpoints.
- Integration test with the official `delta-sharing-client` Python library.

**Deliverables:**
- Ozone Delta Sharing Gateway service runnable as a standalone process.
- Profile file generation CLI command (`ozone delta-sharing create-profile`).
- Documentation for setting up sharing.

**Milestone criteria:**
- A Pandas client with `delta-sharing` Python package can list shares, read table metadata, and read data from an Ozone-hosted Parquet dataset.

---

### Phase 2: Delta Lake Support

**Goal:** Full support for Delta Lake tables including versioning and change data feed.

**Scope:**
- Delta log parsing (read `_delta_log/*.json` and checkpoint Parquet files from Ozone).
- Table versioning support (query by version, query by timestamp).
- Change data feed (CDF) API.
- Partition pruning using predicate hints.
- File statistics in responses (min/max values, record count from Delta log stats).
- `delta-sharing-capabilities` header support (responseformat=parquet).

**Deliverables:**
- Time-travel queries work (read table at a specific version).
- CDF queries work for tables with `enableChangeDataFeed=true`.
- Integration test with Spark + `delta-sharing-spark` connector.

**Milestone criteria:**
- A Spark job using `delta-sharing-spark` can read a Delta table from Ozone, including time-travel and CDF queries.

---

### Phase 3: Directory-Based Access and STS

**Goal:** Support the `dir` access mode by issuing temporary S3 credentials scoped to the table prefix.

**Scope:**
- Integration with Ozone's STS/token service for temporary credential generation.
- `temporary-table-credentials` API endpoint.
- Credential scoping (read-only, prefix-restricted).
- `accessModes` field in metadata responses.
- Delta Kernel compatibility testing (clients reading Delta log directly from Ozone S3).

**Deliverables:**
- Clients with Delta Kernel can read tables directly using temporary credentials.
- Both `url` and `dir` access modes available per table.

**Milestone criteria:**
- A Delta Kernel client can read a table from Ozone using credentials obtained via the Delta Sharing protocol.

---

### Phase 4: Dynamic Configuration and OM Integration

**Goal:** Move share configuration from static YAML into Ozone Manager metadata for dynamic management.

**Scope:**
- Admin CLI for managing shares, schemas, tables, and recipients (`ozone sh delta-sharing ...`).
- OM-side metadata tables for share configuration.
- ACL integration: map share permissions to Ozone ACLs.
- Audit logging for all Delta Sharing operations.
- Recipient management (create, revoke, rotate tokens).
- Optional: Recon UI dashboard for Delta Sharing activity.

**Deliverables:**
- Admins can create/update/delete shares without restarting the gateway.
- Full audit trail of who accessed what data.

---

### Phase 5: Advanced Features

**Goal:** Support advanced protocol features and production hardening.

**Scope:**
- `responseformat=delta` support for advanced Delta features (deletion vectors, column mapping).
- Streaming support (Spark Structured Streaming via Delta Sharing).
- Rate limiting and quota management per recipient.
- HA deployment (multiple gateway instances behind a load balancer).
- Pagination for large table file listings (`EndStreamAction` with `nextPageToken`).
- URL refresh token support (`refreshToken` in `EndStreamAction`).
- Metrics (request count, latency, bytes shared per recipient).

---

## 5. Future Work: Beyond S3 and Parquet

The initial phases focus on sharing Delta and Parquet data through the S3 Gateway (pre-signed URLs and STS tokens). However, Ozone stores many types of data and supports multiple access protocols. This section describes how we plan to extend the Delta Sharing Gateway to cover a broader range of use cases.

### 5.1 Apache Iceberg Table Support

[Apache Iceberg](https://iceberg.apache.org/) is another widely adopted open table format. Many Ozone users store Iceberg tables (used by Trino, Flink, Spark, Hive, etc.), and those tables should also be shareable via the Delta Sharing protocol.

#### How It Works

Iceberg tables store metadata in a set of JSON and Avro manifest files under a `metadata/` directory. The data files are Parquet (or ORC/Avro). The gateway can read Iceberg metadata to discover the current snapshot, schema, partition spec, and data file list — then expose that through the same Delta Sharing REST API.

From the Delta Sharing client's perspective, the response looks the same: protocol + metadata + file URLs. The client does not need to know that the source is Iceberg rather than Delta.

#### Concept Mapping

| Iceberg Concept | Delta Sharing Mapping |
|:----------------|:----------------------|
| Catalog / Namespace | Share / Schema |
| Table | Table |
| Snapshot ID | Table version |
| Manifest entries | File actions in response |
| Partition spec | `partitionColumns` in metadata |
| Schema (Avro-based) | Converted to Delta Sharing's JSON schema format |

#### Implementation Approach

| Option | Description | Pros | Cons |
|:-------|:------------|:-----|:-----|
| **A) Iceberg Java library** | Use the `iceberg-core` and `iceberg-ozone` (or `iceberg-aws` with S3a) libraries to read table metadata | Full feature support; snapshot listing, time travel, schema evolution handled by the library | Adds dependencies; needs catalog configuration |
| **B) Minimal Iceberg metadata parser** | Read `metadata/version-hint.text`, parse the latest `metadata/*.json`, and read manifest lists/manifests | Fewer dependencies; we only need file listing and schema | May miss edge cases; must keep up with Iceberg spec changes |

**Recommendation:** Option A. Iceberg already has good Java library support and a well-defined catalog API. Using it avoids reimplementing spec details.

#### Table Configuration Example

```yaml
tables:
  - name: "web_events"
    location: "o3://vol1/iceberg-bucket/warehouse/web_events"
    format: "iceberg"
    icebergCatalog: "hadoop"  # or "hive", "rest"
```

#### Limitations and Considerations

- Iceberg schema uses Avro-style types. The gateway must translate these to Delta Sharing's Spark-SQL-style JSON schema.
- Iceberg supports ORC and Avro data files in addition to Parquet. When the data format is ORC or Avro, see [Section 5.2](#52-additional-data-formats-orc-avro-csv-json) for how the gateway handles non-Parquet files.
- Iceberg's `position-delete` and `equality-delete` files need special handling. In Phase 1 of Iceberg support, we can exclude tables with pending deletes or materialize the view (return only live files after applying deletes).

---

### 5.2 Additional Data Formats (ORC, Avro, CSV, JSON)

The Delta Sharing protocol was originally designed around Parquet, but many datasets in Ozone are stored in other formats. We can extend support to these formats in two ways:

#### 5.2.1 Approach A: Transparent Pass-Through

Share the files as-is and let the client handle the format. The metadata response declares the format, and the client uses its own reader.

This works when:
- The client library supports the format (for example, Spark can read ORC, Avro, CSV, and JSON natively).
- The protocol's `format.provider` field is extended beyond `"parquet"`.

```json
{
  "metaData": {
    "format": {"provider": "orc"},
    "schemaString": "{...}"
  }
}
```

**Limitation:** The current Delta Sharing spec only defines `"parquet"` as a valid format provider. Clients that strictly follow the spec may reject other values. This approach needs coordination with the Delta Sharing community or a custom extension.

#### 5.2.2 Approach B: Server-Side Conversion to Parquet

The gateway reads the source files (ORC, Avro, CSV, JSON) and converts them to Parquet on the fly or in a batch process. The client always sees Parquet files.

**How it works:**
1. Admin registers a table with format `"orc"` or `"csv"`.
2. On first query (or on a schedule), the gateway converts the source files to Parquet and caches them in a staging area in Ozone.
3. The response points to the converted Parquet files.
4. The gateway tracks source file modification times to invalidate the cache.

**Trade-offs:**

| Aspect | Pass-Through (A) | Conversion (B) |
|:-------|:-----------------|:----------------|
| Client compatibility | Requires client support for the format | Works with all existing Delta Sharing clients |
| Latency | No overhead | Conversion time (can be done ahead of time) |
| Storage | No extra storage | Needs staging space for converted Parquet files |
| Freshness | Always live data | May serve slightly stale converted files |
| Complexity | Simple | Needs conversion pipeline and cache management |

#### Format-Specific Notes

| Format | Schema Discovery | Partitioning | Considerations |
|:-------|:-----------------|:-------------|:---------------|
| **ORC** | From ORC file footer (similar to Parquet) | Hive-style directory partitioning | Well-structured; close to Parquet in metadata richness |
| **Avro** | From Avro schema in file header | Typically not partitioned | Schema is self-describing; may need type mapping for complex unions |
| **CSV** | Must be user-specified or inferred (first row as header) | Directory-based if partitioned | No embedded schema; need configuration for delimiter, quoting, null values |
| **JSON (newline-delimited)** | Inferred by sampling or user-specified | Directory-based if partitioned | Schema may vary across files; needs validation |

**Recommendation:** Start with Approach A for ORC (since it is structurally similar to Parquet and many clients already support it). Use Approach B for CSV and JSON where clients are unlikely to handle them natively. Avro can go either way depending on client support.

---

### 5.3 Raw Data Sharing from Ozone

Not all data in Ozone is organized as tables. Users may want to share:
- Arbitrary files (images, PDFs, model weights, logs).
- Directories of mixed-format files.
- Data that does not have a schema or table structure.

#### Extension: File-Level Sharing

We can extend the Delta Sharing Gateway with a custom endpoint (outside the standard Delta Sharing spec) that allows sharing raw Ozone keys:

```
GET /delta-sharing/shares/{share}/schemas/{schema}/files
GET /delta-sharing/shares/{share}/schemas/{schema}/files/{path}
```

This would return pre-signed URLs for arbitrary Ozone objects, grouped under the share/schema hierarchy. The response format would list files with their size and content type but without table metadata (no schema, no partition columns).

Example response:
```json
{
  "files": [
    {
      "path": "models/v2/weights.bin",
      "url": "https://ozone-s3:9878/ml-bucket/models/v2/weights.bin?X-Amz-Signature=...",
      "size": 524288000,
      "contentType": "application/octet-stream",
      "lastModified": "2026-05-10T14:32:00Z"
    },
    {
      "path": "models/v2/config.json",
      "url": "https://ozone-s3:9878/ml-bucket/models/v2/config.json?X-Amz-Signature=...",
      "size": 2048,
      "contentType": "application/json",
      "lastModified": "2026-05-10T14:32:01Z"
    }
  ]
}
```

#### Use Cases

| Use Case | Example | How Gateway Helps |
|:---------|:--------|:------------------|
| ML model sharing | Share trained model artifacts | Recipients download model files via pre-signed URLs without needing direct Ozone credentials |
| Log sharing | Share application or audit logs | Recipients can pull logs for analysis without full bucket access |
| Media sharing | Share images/videos for annotation | Annotators access files through their tools using standard HTTPS URLs |
| Document sharing | Share reports, PDFs | External partners access documents without VPN |

#### Compatibility Note

Raw file sharing goes beyond the Delta Sharing specification. To keep things interoperable:
- Table-based endpoints remain fully spec-compliant.
- Raw file endpoints use a separate URL namespace (`/files` instead of `/query`).
- Clients that only understand the standard Delta Sharing protocol will not use the raw file endpoints. Custom clients or a thin wrapper library would be needed.

---

### 5.4 Native Ozone Protocol Access (Beyond S3)

The initial design uses Ozone's S3 Gateway for pre-signed URL generation and data access. However, Ozone also supports:
- **ofs:// (OzoneFS)**: HDFS-compatible filesystem interface.
- **o3:// (Ozone native)**: The native Ozone client protocol.
- **gRPC-based datanode access**: Direct container-level reads.

#### Why Go Beyond S3?

| Reason | Details |
|:-------|:--------|
| Performance | Native Ozone reads can bypass the S3 Gateway translation layer; direct datanode access avoids an extra hop |
| FSO bucket access | FSO buckets with deep directory hierarchies are better served via the native FS interface than S3's flat namespace |
| Non-S3 volumes | Ozone volumes/buckets that are not configured for S3 access (no S3 mapping) cannot be shared via pre-signed S3 URLs |
| Encryption zones | Some Ozone encryption configurations may work differently via native vs. S3 paths |

#### Approach: Pluggable Data Access Layer

The gateway's file-access layer should be pluggable:

```
                    +---------------------------+
                    |  Delta Sharing Gateway    |
                    +---------------------------+
                    |  DataAccessProvider (SPI) |
                    +------+--------+-----------+
                           |        |
              +------------+        +------------+
              |                                  |
              v                                  v
    +-------------------+            +-------------------+
    | S3DataAccess      |            | NativeDataAccess  |
    | (pre-signed URLs) |            | (Ozone RPC/gRPC)  |
    +-------------------+            +-------------------+
```

**S3DataAccess (Phase 1-3):**
- Generates pre-signed S3 URLs.
- Client downloads via HTTPS to S3 Gateway.
- Works with any S3-compatible client.

**NativeDataAccess (Future):**
- Could generate time-limited Ozone tokens for direct ofs:// access.
- Would require the client to have Ozone client libraries (Hadoop OzoneFS jar).
- Useful for Spark/Hive/Trino workloads that already run in the Ozone cluster.

**Hybrid Mode:**
- For a given table, the gateway could advertise both access modes.
- Clients inside the cluster use native access (faster, no S3 Gateway overhead).
- Clients outside the cluster use pre-signed S3 URLs (universally accessible).

#### Native Access Protocol Details

For clients inside the cluster (or with VPN access), the gateway could:
1. Return `ofs://` paths instead of pre-signed HTTPS URLs.
2. Issue a short-lived Ozone delegation token that the client presents when reading.
3. The client uses OzoneFS (Hadoop FileSystem implementation) to read data directly from datanodes.

This is analogous to how Hadoop's WebHDFS provides delegation tokens for direct HDFS access.

Example response for native-access-capable clients:
```json
{
  "file": {
    "nativePath": "ofs://ozone1/vol1/sales-bucket/tables/transactions/part-00000.parquet",
    "delegationToken": "HDDS-delegation-token...",
    "url": "https://ozone-s3:9878/sales-bucket/tables/transactions/part-00000.parquet?X-Amz-Signature=...",
    "id": "abc123",
    "size": 104857600
  }
}
```

Clients that understand the `nativePath` field use it; others fall back to `url`.

---

### 5.5 Cross-Cluster and Federation

In larger organizations, data may live across multiple Ozone clusters. Future work could include:

#### Multi-Cluster Sharing

A single Delta Sharing Gateway could serve data from multiple Ozone clusters:

```yaml
clusters:
  - name: "us-east"
    omAddress: "om1.us-east.internal:9862"
    s3Endpoint: "https://ozone-s3.us-east.internal:9878"
  - name: "eu-west"
    omAddress: "om1.eu-west.internal:9862"
    s3Endpoint: "https://ozone-s3.eu-west.internal:9878"

shares:
  - name: "global_analytics"
    schemas:
      - name: "us_sales"
        tables:
          - name: "orders"
            cluster: "us-east"
            location: "o3://vol1/sales/tables/orders"
      - name: "eu_sales"
        tables:
          - name: "orders"
            cluster: "eu-west"
            location: "o3://vol1/sales/tables/orders"
```

#### Federated Sharing

Multiple Delta Sharing Gateways (one per cluster) could be federated behind a coordinator that routes requests based on the share/table location. This is similar to how Ozone's federation works at the OM level.

#### Data Locality Awareness

When a table is replicated across clusters, the gateway could direct the client to the closest replica based on:
- Client IP geolocation.
- Explicit region hints in the request.
- Latency-based routing.

---

## 6. Open Questions

### Q1: Deployment Model — New Service vs. S3 Gateway Extension?

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) New standalone service** (recommended) | Clean separation of concerns; independent lifecycle and scaling; does not risk breaking S3 compatibility | Additional service to deploy and monitor; one more port to configure |
| **B) Embedded in S3 Gateway** | Fewer services to manage; reuses existing HTTP infrastructure | Mixes two different APIs in one service; Delta Sharing uses bearer tokens while S3 uses SigV4; harder to upgrade independently |

---

### Q2: Share Configuration Storage

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) YAML config file** (Phase 1) | Simple to implement; easy to version in source control; no OM changes needed | Requires restart on changes; does not scale to thousands of shares |
| **B) OM metadata** (Phase 4) | Dynamic; supports CLI management; scalable; transactional | Requires OM schema changes; more implementation effort |

---

### Q3: Service Identity vs. Recipient Impersonation

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Single service identity** | Simpler; gateway uses one principal for all Ozone access | All accesses look the same in Ozone audit logs; hard to trace back to a specific recipient |
| **B) Impersonation (proxy user)** | Audit logs show actual recipient; Ozone ACLs apply per recipient | More complex; requires proxy-user setup; may need Ozone ACL changes per recipient |
| **C) Service identity + gateway-level audit log** | Simple Ozone setup; gateway logs recipient details in its own audit log | Two audit systems to check; no Ozone-native per-recipient ACL enforcement |

---

### Q4: Table Format Support

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Delta + plain Parquet** | Covers the main Delta Sharing use case plus simple data sharing | Need to implement Delta log parsing |
| **B) Delta only** | Simpler; only one code path | Excludes users who just have Parquet files without a Delta log |
| **C) Delta + Parquet + Iceberg** (future) | Most flexible; covers all lakehouse formats | Iceberg adds significant complexity; could be a separate protocol in the future |

---

### Q5: Delta Log Parsing Library

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Delta Kernel (Java)** | Official library; maintained by Databricks/Delta Lake project; supports all Delta features | Adds a new dependency; may have version conflicts with existing Hadoop libraries |
| **B) Custom minimal parser** | No new dependencies; we only parse what we need | Harder to maintain; risk of missing edge cases in the Delta protocol |
| **C) Delta Standalone (legacy)** | Proven in the reference delta-sharing server; Java API | Deprecated in favor of Delta Kernel; may not support newer Delta features |

---

### Q6: Where Should the Gateway Run?

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Co-located with OM** | Direct access to OM metadata; no extra network hop for namespace operations | Ties gateway lifecycle to OM; adds load to OM process |
| **B) Separate process, same node** | Independent lifecycle; can scale/restart independently; shares local network with OM | One more process to manage |
| **C) Separate node(s)** | Full isolation; can be load-balanced; no impact on OM/SCM nodes | Additional network hop; needs remote access to OM |

---

### Q7: Raw File Sharing — Part of Gateway or Separate Service?

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Part of Delta Sharing Gateway** | Single service to manage; shares authentication and share-configuration infrastructure; natural extension of the sharing concept | Mixes table-sharing with general file-sharing; may confuse the API surface |
| **B) Separate "Ozone File Sharing" service** | Clean separation; can evolve independently; could use a different protocol entirely (for example, simple signed-URL endpoint) | Another service to deploy; duplicates auth and config management |
| **C) Gateway with a distinct API prefix** | One deployment; clear separation in API paths (`/delta-sharing/` for tables, `/file-sharing/` for raw files); shared infrastructure | API surface is larger; documentation needs to distinguish the two |

---

### Q8: Iceberg Catalog Integration

When sharing Iceberg tables, the gateway needs to know where the Iceberg metadata lives. This depends on the catalog type.

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Hadoop catalog (filesystem-based)** | No external dependencies; reads `metadata/` directory from Ozone directly | Does not support concurrent writers well; limited catalog features |
| **B) Hive Metastore catalog** | Full-featured; commonly used in production Iceberg deployments | Requires access to HMS; adds a dependency outside Ozone |
| **C) REST catalog (Iceberg REST spec)** | Modern approach; decoupled from storage; supports access control at catalog level | Requires a REST catalog service (for example, Nessie, Polaris, or Unity Catalog) |
| **D) Configurable per table** | Most flexible; each table entry specifies its own catalog type and parameters | More complex configuration; multiple code paths |

---

### Q9: Non-Parquet Format Handling Strategy

| Option | Pros | Cons |
|:-------|:-----|:-----|
| **A) Always convert to Parquet** | Maximum compatibility with all Delta Sharing clients; simple client experience | Storage overhead for converted files; conversion latency; cache invalidation complexity |
| **B) Pass-through with format declaration** | No overhead; serves data as-is; simpler server implementation | Requires client support; not all Delta Sharing clients handle non-Parquet; may break strict spec-compliance |
| **C) Hybrid (pass-through for rich formats, convert for text formats)** | Balanced: ORC/Avro pass-through (structured, similar to Parquet); CSV/JSON get converted | Two code paths; need to decide the boundary per format |
