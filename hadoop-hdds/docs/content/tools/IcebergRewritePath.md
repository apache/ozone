---
title: "Iceberg Table Path Rewrite"
date: 2026-06-05
summary: Rewrite Iceberg table metadata paths from OFS to S3-compatible paths without copying data files.
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

## Introduction

`ozone iceberg rewrite-path` is an Ozone CLI tool that rewrites absolute path references within an existing
Apache Iceberg table's metadata hierarchy stored in Apache Ozone.
The command replaces a source path prefix (for example, ofs://...) with a target path prefix (for example, s3://...)
by updating the internal metadata references.

The operation is metadata-only while keeping the actual data files in their original location.
It serves as Ozone's native implementation of Iceberg's RewriteTablePath action.

(Epic: [HDDS-14937](https://issues.apache.org/jira/browse/HDDS-14937)).

### Requirements:

* Apache Iceberg 1.8.0 or higher (Ozone implementation aligned with Iceberg 1.10.1)
* Ozone build with JDK 11+ (`ozone iceberg` is not available on Java 8 builds)

## Purpose of the tool

Existing Iceberg tables in Ozone are often created using OFS (ofs://) paths. Iceberg stores these paths as absolute references throughout the table’s metadata hierarchy. Because of these embedded absolute references, the tables cannot be accessed through S3-compatible systems such as REST catalogs (for example, Apache Polaris) or engines operating through S3.

The purpose of this migration is to enable S3-based access to an existing Iceberg table in Ozone that was originally created using the OFS protocol, without copying the underlying data files.

> **This is not a storage migration.** Only metadata path references are rewritten.
> Data files remain in their original location.


## How it works
The tool:
1. Loads the table via Iceberg's `HadoopTables`
2. Rewrites metadata files, manifest lists, manifest files, and position delete files
3. Writes all rewritten files to a **staging directory** under the table's metadata directory by default or the specified staging directory.
4. Produces a **`file-list`** CSV mapping source paths to target paths

Original files are **not modified**. We need to copy or move staged files to their final
locations in a separate step.

>**Note:** Avoid writes to the table from the old catalog during rewrite to avoid inconsistency.

## Step-by-step workflow

### Step 1: Create an S3 bucket link

Ozone has a dedicated volume called /s3v which can be accessed by S3 protocol.
We need to create a bucket link in s3v volume pointing to the source bucket in the Ozone volume which contains the Iceberg table data and metadata. This enables S3 access to Ozone volume data. A bucket link is a logical mapping to an existing Ozone bucket, not a physical copy. Therefore, no data file copying is required.


To create bucket link in /s3v we need to do the following:

```bash
ozone sh bucket link /<volume>/<bucket> /s3v/<bucket-link>
```
### Step 2: Identify the latest metadata file for the table
Locate the latest `*.metadata.json` file in the table's metadata directory under `<db-name>/<table-name>/metadata/`
### Step 3: Run the rewrite path command

```bash
ozone iceberg rewrite-path \
  --table-location "ofs://<omserviceid>/<volume>/<bucket>/<db-name>/<table-name>/metadata/<metadata-file>" \
  --source-prefix "ofs://<omserviceid>/<volume>/<bucket>/<db-name>/<table-name>" \
  --target-prefix "s3://<bucket-link>/<db-name>/<table-name>"
```

**Required options:**

| Option | Description                                        |
|--------|----------------------------------------------------|
| `-l`, `--table-location` | Absolute path to the latest `*.metadata.json` file |
| `-s`, `--source-prefix` | Prefix to replace                                  |
| `-t`, `--target-prefix` | Target prefix                                      |

**Additional options:**

| Option | Description                                                                                            |
|--------|--------------------------------------------------------------------------------------------------------|
| `--staging` | Custom staging directory (default: `copy-table-staging-<uuid>/` under metadata directory of the table) |
| `--start-version` | Start metadata version for incremental rewrite (will not be included for rewrite)                      |
| `--end-version` | End metadata version or defaults to current (will be included for rewrite)                                                           |
| `--threads` | Parallel thread count (default: 10)                                                                    |

**Rewrite modes:**

* **Full rewrite** (default): rewrites all reachable metadata files, manifest lists, manifest files, and position delete files.
* **Incremental rewrite**: provide `--start-version` and `--end-version` to limit scope.
  Only snapshots added after the start version are used to scope manifest/manifest-list/delete-file rewriting.

**Example:**

```bash
ozone iceberg rewrite-path \
  --table-location "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/v2.metadata.json" \
  --source-prefix "ofs://omserviceid/vol1/buck1/my_db/test_table" \
  --target-prefix "s3://buck1link/my_db/test_table"
```
Output:
```text
Starting Iceberg table path rewrite
Table location: ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/v2.metadata.json
Source prefix: ofs://omserviceid/vol1/buck1/my_db/test_table
Target prefix: s3://buck1link/my_db/test_table
Table loaded: ofs://omserviceid/vol1/buck1/my_db/test_table
Threads: 10

Rewrite completed successfully
  Latest version: v2.metadata.json
  Staging location: ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/copy-table-staging-123/

Next step: Copy files from source to target using the file list
  File list location: ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/copy-table-staging-123/file-list
```

### Step 4: Copy rewritten files to their target locations
After running the above command, a staging directory is created under the table's existing metadata directory in the bucket. Alternatively, if the `--staging` option is specified, the provided staging directory is used.

The staging directory contains all files rewritten with the `--target-prefix`. The original files that still reference the source prefix remain unchanged in the table's metadata directory. However, once the rewritten files are copied from the staging location to their final destinations, they replace the original metadata files, which are no longer accessible.


A `file-list` CSV is also generated in the staging directory and serves as the copy plan.

Each line follows the format `source_path,target_path`.

Example entries:

```text
ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/copy-table-staging-123/metadata/v0.metadata.json,s3://buck1link/my_db/test_table/metadata/v0.metadata.json
...
ofs://omserviceid/vol1/buck1/my_db/test_table/data/00001-1-7a28411b-069b-43b8-a25c-9d7f1810a719-0-00001.parquet,s3://buck1link/my_db/test_table/data/00001-1-7a28411b-069b-43b8-a25c-9d7f1810a719-0-00001.parquet
...
ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/copy-table-staging-123/metadata/snap-1753351619419365870-1-5ac51133-8cbf-4327-bbf8-0559b463e1f9.avro,s3://buck1link/my_db/test_table/metadata/snap-1753351619419365870-1-5ac51133-8cbf-4327-bbf8-0559b463e1f9.avro
```

* **Metadata files, manifest lists, and manifest files** should be copied from the staging directory to the table's `/metadata` directory.
* **Position delete files** should be copied from the staging directory to the target delete-file location under the table's `/data` directory.
* **Data files** are included in the CSV only for path-mapping purposes. When using an S3 bucket link, no physical data-file copy is required.

After this step, the rewritten files are in their target locations and are ready to be registered with the new catalog.

> **NOTE:** The tool only generates rewritten files in the staging directory and a file-list mapping. Copying those files to their final destinations must be performed separately (for example, using DistCp or another file-copy mechanism).
### Step 5: Register the table with the new catalog

Use your engine's catalog registration procedure. For Spark with Iceberg:

```sql
CALL catalog_name.system.register_table(
  table => '<db>.<table>',
  metadata_file => '<metadata-file-path>'
);
```

Then **unregister the table from the old catalog** before further operations.
If the old catalog is a Hadoop catalog, avoid using it entirely after rewrite.

## What changes (before and after)

### Metadata file

**Before:**

```json lines
{
  "format-version": 2,
  "table-uuid": "9b791462-d257-45e5-92f8-435302d2c335",
  "location": "ofs://omserviceid/vol1/buck1/my_db/test_table",
  ...
  "snapshots": [
    ...
      "manifest-list": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/snap-1753351619419365870-1-5ac51133-8cbf-4327-bbf8-0559b463e1f9.avro",
      "schema-id": 0
    },
    ...
      "manifest-list": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/snap-176890185746044789-1-5061c816-61b1-43e4-84e8-0ad689c2ea86.avro",
      "schema-id": 0
    }
  ],
  ...
  "metadata-log": [
    {
      "timestamp-ms": 1774448474465,
      "metadata-file": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/v0.metadata.json"
    },
    {
      "timestamp-ms": 1774448493051,
      "metadata-file": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/v1.metadata.json"
    }
  ]
}
```

**After (in staging):**

```json lines
{
  "format-version": 2,
  "table-uuid": "9b791462-d257-45e5-92f8-435302d2c335",
  "location": "s3://buck1link/my_db/test_table",
  ...
  "snapshots": [
    ...
      "manifest-list": "s3://buck1link/my_db/test_table/metadata/snap-1753351619419365870-1-5ac51133-8cbf-4327-bbf8-0559b463e1f9.avro",
      "schema-id": 0
    },
    ...
      "manifest-list": "s3://buck1link/my_db/test_table/metadata/snap-176890185746044789-1-5061c816-61b1-43e4-84e8-0ad689c2ea86.avro",
      "schema-id": 0
    }
  ],
  ...
  "metadata-log": [
    {
      "timestamp-ms": 1774448474465,
      "metadata-file": "s3://buck1link/my_db/test_table/metadata/v0.metadata.json"
    },
    {
      "timestamp-ms": 1774448493051,
      "metadata-file": "s3://buck1link/my_db/test_table/metadata/v1.metadata.json"
    }
  ]
}
```
### Manifest list
**Before:**
```json lines
{
  "manifest_path": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/5061c816-61b1-43e4-84e8-0ad689c2ea86-m0.avro",
  ...
}
{
  "manifest_path": "ofs://omserviceid/vol1/buck1/my_db/test_table/metadata/5ac51133-8cbf-4327-bbf8-0559b463e1f9-m0.avro",
  ...
}
```

**After (in staging):**
```json lines
{
  "manifest_path": "s3://buck1link/my_db/test_table/metadata/5061c816-61b1-43e4-84e8-0ad689c2ea86-m0.avro",
  ...
}
{
  "manifest_path": "s3://buck1link/my_db/test_table/metadata/5ac51133-8cbf-4327-bbf8-0559b463e1f9-m0.avro",
  ...
}
```
### Manifest file
**Before:**
```json lines
{
  "status": 1,
  "snapshot_id": {
    "long": 176890185746044789
  },
  ...
  "data_file": {
    "content": 0,
    "file_path": "ofs://omserviceid/vol1/buck1/my_db/test_table/data/00000-2-7dd217f9-4a26-47f2-bd07-5a54ca129645-0-00001.parquet",
    ...
}
{
  "status": 1,
  "snapshot_id": {
    "long": 176890185746044789
  },
  ...
  "data_file": {
    "content": 0,
    "file_path": "ofs://omserviceid/vol1/buck1/my_db/test_table/data/00001-3-7dd217f9-4a26-47f2-bd07-5a54ca129645-0-00001.parquet",
    ...
}
```

**After (in staging):**
```json lines
{
  "status": 1,
  "snapshot_id": {
    "long": 176890185746044789
  },
  ...
  "data_file": {
    "content": 0,
    "file_path": "s3://buck1link/my_db/test_table/data/00000-2-7dd217f9-4a26-47f2-bd07-5a54ca129645-0-00001.parquet",
    ...
}
{
  "status": 1,
  "snapshot_id": {
    "long": 176890185746044789
  },
  ...
  "data_file": {
    "content": 0,
    "file_path": "s3://buck1link/my_db/test_table/data/00001-3-7dd217f9-4a26-47f2-bd07-5a54ca129645-0-00001.parquet",
    ...
}
```


## Important Considerations and Limitations

> **Read this before rewriting production tables.**


| File                                           | What happens                                                                                                                                 |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| Metadata files, manifest lists, manifest files | Rewritten to the staging directory with all internal path references updated. Must be copied to their target location as part of the rewrite |
| Data files                                     | Path references in respective manifests are updated, the files themselves are not moved (access is provided through the bucket link)         |
| Position delete files                          | Rewritten to the staging directory and must be copied to their target location as part of the rewrite                                        |
| Equality delete files                          | Path references in respective manifests are updated                                                                                          |
| Partition statistics files                     | **Not supported** — the rewrite operation fails if these files are present                                                                   |
| Deletion vectors                               | **Not supported**                                                                                                                            |

### Position delete file read failure (known issue)

When a table uses merge-on-read deletes (`write.delete.mode = merge-on-read`) and
has position delete files.
The rewrite updates embedded data-file paths inside those delete files, which changes their size. Manifests are written **before** those delete files are updated, so `file_size_in_bytes` in the rewritten manifest can be **stale**. Some catalogs (especially REST catalogs) may then fail to read the position delete files.

```text
RuntimeException: ... is not a Parquet file. Expected magic number at tail, but found [...]
```

This is a known limitation tracked in
[apache/iceberg#15470](https://github.com/apache/iceberg/pull/15470).
The Ozone implementation will be updated once the Iceberg fix lands.

**Workaround:** Avoid rewriting tables with existing position delete files until
the fix is available, because the position delete file reads may fail with certain catalogs.

### Dual-catalog risk

After rewrite, the table remains registered in the old catalog. Only the newly registered catalog receives metadata updates. Continuing to use both catalogs causes
inconsistency and risks data corruption or accidental purge.

* Unregister from the old catalog immediately after registering with the new one
* If the old catalog is a Hadoop catalog, **do not use it** after the rewrite operation

