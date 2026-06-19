# Recon API Guide

## 1. Global Routing Rules (CRITICAL)
Before selecting an endpoint, you must disambiguate the user's intent according to these rules:

* **Aggregation vs. Enumeration (The `du` vs `ls` rule):**
  * If the user asks for "totals", "size", "disk usage", or "how much space", ALWAYS use `/namespace/usage`. Do NOT use `/keys/listKeys` to calculate totals.
  * If the user asks to "list files", "find files", or filter files by size/date, ALWAYS use `/keys/listKeys`. Do NOT use `/namespace/usage` as it only shows top-level directories.
* **Open vs. Committed Keys:**
  * If the user asks about "open", "in-progress", or "uncommitted" files, use `/keys/open`.
  * If the user asks to list normal/committed files, use `/keys/listKeys`.
* **Missing vs. Deleted Containers:**
  * If the user asks about containers that are "missing" or "lost", use `/containers/missing`.
  * If the user asks about containers that were "deleted", use `/containers/deleted`.

## 2. Module Index
* **Containers:** `/containers`, `/containers/missing`, `/containers/unhealthy`, `/containers/deleted`
* **Volumes & Buckets:** `/volumes`, `/buckets`
* **Keys (Files):** `/keys/listKeys`, `/keys/open`, `/keys/deletePending`
* **Namespace & Usage:** `/namespace/usage`, `/namespace/summary`, `/namespace/quota`, `/utilization/filesize`
* **Cluster Health:** `/clusterState`, `/datanodes`, `/pipelines`, `/task/status`

---

## Module: Containers

**Category Purpose:**

Retrieve container-level metadata, health, and reconciliation status from Recon. Used to identify unhealthy, missing, mismatched, or deleted containers, as well as to trace replica history across DataNodes.

---

### **Endpoint:** `/containers`

**Intent Keywords:** all containers, list containers, container summary, container info

**Purpose:** Fetch metadata for all containers known to Recon.

**Method:** `GET`

**Input Parameters:** None

**Response Fields:**

- `ContainerID`: unique identifier
- `NumberOfKeys`: number of keys within the container
- `pipelines`: associated pipeline, if any

  **Sample Outputs:** Total containers, container counts per pipeline, container–key mapping.

  **Example Queries:**

- "List all containers."
- "How many containers exist in the cluster?"
- "Show number of keys per container."

  **Related Endpoints:** `/containers/unhealthy`, `/containers/missing`


---

### **Endpoint:** `/containers/deleted`

**Intent Keywords:** deleted containers, removed containers, scm deleted

**Purpose:** Retrieve all containers marked deleted in SCM.

**Method:** `GET`

**Response Fields:**

- `containerId`, `pipelineId`, `containerState`, `stateEnterTime`, `lastUsed`, `replicationConfig`

  **Example Queries:**

- "Show deleted containers."
- "Which containers were deleted recently?"
- "Get replication type of deleted containers."

  **Relationships:** Linked to `/containers/mismatch/deleted`.


---

### **Endpoint:** `/containers/missing`

**Intent Keywords:** missing containers, lost containers, containers not found

**Purpose:** List containers missing in SCM metadata or heartbeats.

**Method:** `GET`

**Parameters:**

- `limit` (integer, default 1000)

  **Response Fields:**

- `containerID`, `missingSince`, `pipelineID`, `replicas`, `keys`

  **Example Queries:**

- "Which containers are missing?"
- "List missing containers with pipeline IDs."
- "How many containers went missing this week?"

  **Relationships:** Related to `/containers/unhealthy/MISSING`.


---

### **Endpoint:** `/containers/{id}/replicaHistory`

**Intent Keywords:** replica history, container replicas, container timeline

**Purpose:** Show replica-level history for a specific container across DataNodes.

**Method:** `GET`

**Path Variable:** `id` = container ID

**Response Fields:**

- `datanodeUuid`, `datanodeHost`, `firstSeenTime`, `lastSeenTime`, `state`

  **Example Queries:**

- "Show replica history for container 12."
- "Which DataNodes hosted container 54?"
- "When was container 101 last seen healthy?"

  **Relationships:** Connects `/containers` and `/datanodes`.


---

### **Endpoint:** `/containers/unhealthy`

**Intent Keywords:** unhealthy containers, bad containers, under replicated, mis replicated

**Purpose:** Return metadata for all unhealthy containers.

**Method:** `GET`

**Parameters:**

- `batchNum` (integer, optional)
- `limit` (integer, default 1000)

  **Response Fields:**

- `missingCount`, `underReplicatedCount`, `overReplicatedCount`, `misReplicatedCount`
- `containers[].containerID`, `containers[].containerState`, `containers[].unhealthySince`, `replicaDeltaCount`

  **Example Queries:**

- "List all unhealthy containers."
- "How many under-replicated containers exist?"
- "Which containers are mis-replicated?"

  **Relationships:** `/containers/unhealthy/{state}`, `/containers/missing`


---

### **Endpoint:** `/containers/unhealthy/{state}`

**Intent Keywords:** missing containers, under replicated, over replicated, mis replicated

**Purpose:** Filter unhealthy containers by state.

**Method:** `GET`

**Path Variable:** `state` = `MISSING`, `MIS_REPLICATED`, `UNDER_REPLICATED`, or `OVER_REPLICATED`

**Parameters:**

- `batchNum`, `limit`

  **Response Fields:** Same as `/containers/unhealthy`.

  **Example Queries:**

- "Show missing containers."
- "List under-replicated containers."
- "Which containers are over-replicated?"

  **Relationships:** Child of `/containers/unhealthy`.


---

### **Endpoint:** `/containers/mismatch`

**Intent Keywords:** mismatch containers, inconsistent containers, om scm mismatch

**Purpose:** Return containers that exist in one metadata source (OM/SCM) but not the other.

**Method:** `GET`

**Parameters:**

- `prevKey` (integer)
- `limit` (integer, default 1000)
- `missingIn` (string, `OM` or `SCM`)

  **Response Fields:**

- `containerId`, `existsAt`, `numberOfKeys`, `pipelines[].replicationConfig`

  **Example Queries:**

- "Which containers are mismatched between OM and SCM?"
- "Show containers missing in OM."
- "Find mismatched containers by replication type."

  **Relationships:** `/containers/mismatch/deleted`, `/containers/deleted`.


---

### **Endpoint:** `/containers/mismatch/deleted`

**Intent Keywords:** deleted in scm but present in om, deleted mismatch, stale containers

**Purpose:** Identify containers deleted in SCM but still recorded in OM.

**Method:** `GET`

**Parameters:**

- `prevKey`, `limit`

  **Response Fields:**

- `containerId`, `existsAt`, `replicationConfig`, `numberOfKeys`

  **Example Queries:**

- "Which containers are deleted in SCM but not in OM?"
- "List deleted mismatched containers."
- "Find stale deleted containers still visible to OM."

  **Relationships:** `/containers/deleted`, `/containers/mismatch`.


---

### **Routing Guide (for this module)**

**When user asks about:**

- “unhealthy containers” → Call `/containers/unhealthy`
- “missing containers” → Call `/containers/missing`
- “deleted containers” → Call `/containers/deleted`
- “mismatched containers” → Call `/containers/mismatch`
- “replica history” → Call `/containers/{id}/replicaHistory`

  **If the question includes a specific state** (e.g., “under-replicated”), use `/containers/unhealthy/{state}`.

  If no data matches, respond with: *“Recon did not find any containers matching that state or condition.”*


## Module: Volumes

**Category Purpose:**

Fetch metadata about all Ozone volumes tracked by Recon. Each volume represents a logical namespace boundary owned by a user or service. This API allows listing and paginating through all volumes present in the cluster.

---

### **Endpoint:** `/volumes`

**Intent Keywords:** all volumes, list volumes, volume summary, volume info, available volumes

**Purpose:** Returns a list of all volumes known to Recon. Used to understand the current set of namespaces, their owners, and to verify that all expected volumes are visible to Ozone Recon.

**Method:** `GET`

**Parameters:**

- `prevKey` (string, optional): fetch results after a specific key for pagination.
- `limit` (integer, optional, default: 1000): maximum number of results to return.

**Response Fields:**

- `volumeName`: name of the volume.
- `owner`: user or service that owns the volume.
- `creationTime`: timestamp when the volume was created.
- `quotaInBytes`: total quota assigned to the volume.
- `usedBytes`: amount of storage currently used.
- `numBuckets`: total number of buckets under this volume.

**Example Queries:**

- "List all volumes present in the cluster."
- "Show me all volumes owned by a specific user."
- "How many volumes are available in Ozone?"
- "Get details of all volumes with their quota usage."

**Relationships:**

- `/buckets` (for buckets within each volume)
- `/namespace/usage` (for aggregate usage per volume)

---

### **Routing Guide (for this module)**

**When user asks about:**

- “list all volumes” or “show available volumes” → Call `/volumes`.
- “how many volumes exist” → Call `/volumes` and count entries.
- “quota or used space per volume” → Combine `/volumes` with `/namespace/usage` for details.

**If query includes pagination context:**

Use `prevKey` and `limit` parameters to fetch additional pages of results.

If Recon has no recorded volumes, respond with: *“Recon did not find any volumes currently registered in the cluster.”*

## 

## Module: Buckets

**Category Purpose:**

Retrieve metadata about all buckets across all volumes in the Ozone cluster. Each bucket represents a logical container of keys (files) under a specific volume. This API provides full bucket-level information, including quotas, usage, ownership, layout type, and versioning configuration.

---

### **Endpoint:** `/buckets`

**Intent Keywords:** list buckets, bucket info, all buckets, bucket usage, bucket metadata

**Purpose:** Fetch detailed metadata for all buckets known to Recon, with optional filtering by volume name. Used to analyze storage distribution, monitor quotas, and inspect ownership or versioning status.

**Method:** `GET`

**Parameters:**

- `volume` (string, optional): fetch only buckets under a given volume.
- `prevKey` (string, optional): pagination key to fetch results after a specific entry.
- `limit` (integer, optional, default: 1000): maximum number of bucket entries to retrieve.

**Response Fields:**

- `totalCount` *(integer)* – total number of buckets in the response.
- `buckets[]` *(array)* – list of bucket metadata objects containing:
  - `versioningEnabled` *(boolean)* – whether bucket versioning is enabled.
  - `metadata` *(object)* – additional system metadata about the bucket.
  - `name` *(string)* – bucket name.
  - `quotaInBytes` *(integer)* – maximum bytes allowed in the bucket.
  - `quotaInNamespace` *(integer)* – maximum number of namespace objects (keys, directories).
  - `usedNamespace` *(integer)* – current count of namespace objects used.
  - `creationTime` *(integer)* – bucket creation time (epoch milliseconds).
  - `modificationTime` *(integer)* – last modification time (epoch milliseconds).
  - `acls` *(object)* – access control configuration containing:
    - `type` *(string)* – ACL type (USER/GROUP).
    - `name` *(string)* – user or group name.
    - `aclScope` *(string)* – SCOPE (ACCESS or DEFAULT).
    - `aclList[]` *(array of strings)* – permissions list (e.g., READ, WRITE, ALL).
  - `volumeName` *(string)* – parent volume name.
  - `storageType` *(string)* – storage class (e.g., DISK, ARCHIVE).
  - `versioning` *(boolean)* – versioning flag (same as `versioningEnabled`, backward compatible).
  - `usedBytes` *(integer)* – total storage currently used by the bucket.
  - `encryptionInfo` *(object)* – encryption configuration, if enabled:
    - `version` *(string)* – encryption metadata version.
    - `suite` *(string)* – encryption suite used (e.g., AES/CTR/NoPadding).
    - `keyName` *(string)* – KMS key used for encryption.
  - `replicationConfigInfo` *(object, nullable)* – replication configuration of the bucket (e.g., RATIS/EC).
  - `sourceVolume` *(string, nullable)* – source volume if the bucket was cloned or replicated.
  - `sourceBucket` *(string, nullable)* – source bucket name if the bucket was cloned or replicated.
  - `bucketLayout` *(string)* – layout type (FSO, OBS, or LEGACY).
  - `owner` *(string)* – user or service that owns this bucket.

**Example Queries:**

- "List all buckets in the cluster."
- "Show all buckets under volume `sales`."
- "Get bucket size and quota details."
- "Which buckets have versioning enabled?"
- "Show all FSO layout buckets."

**Relationships:**

- `/volumes` (parent namespace)
- `/keys` (for objects inside buckets)
- `/namespace/usage` (to check detailed disk usage)

---

### **Routing Guide (for this module)**

**When user asks about:**

- “buckets in a specific volume” → Call `/buckets` with `volume=<name>`.
- “list all buckets” or “show bucket metadata” → Call `/buckets` without filters.
- “used or available space” → Extract from `usedBytes` and `quotaInBytes`.
- “bucket owner” or “who owns this bucket” → Return `owner`.
- “layout type” → Return `bucketLayout` (FSO, OBS, Legacy).
- “versioning” → Use `versioningEnabled` and `versioning` fields.
- “encryption” → Use `encryptionInfo` object for suite and keyName details.

**If pagination or limit context is mentioned:**

Use `prevKey` and `limit` parameters accordingly.

If Recon has no bucket data, respond with: *“Recon did not find any buckets currently registered in the cluster.”*

---

## **Schema: ContainerMetadata**

**Purpose:**

Represents metadata for all containers tracked by Recon. Used by the `/containers` endpoint to list every container with its total count, pagination key, and key statistics.

---

### **Structure**

**Root Object:**

- **`data`** *(object)* — Wrapper containing metadata details for containers.

### **data fields**

- **`totalCount`** *(integer)* — Total number of containers included in the response.

  *Example:* `3`

- **`prevKey`** *(integer)* — Key offset for pagination. Used to fetch the next set of containers after this key in subsequent queries.

  *Example:* `3019`

- **`containers[]`** *(array of objects)* — List of individual container metadata entries.

---

### **Container Object Fields**

Each container entry contains the following fields:

- **`ContainerID`** *(integer)* — Unique numeric identifier for the container.

  *Example:* `1`

- **`NumberOfKeys`** *(integer)* — Count of keys (objects/files) stored within this container.

  *Example:* `834`

- **`pipelines`** *(string | null)* — Pipeline or replication configuration assigned to the container. May be null if not explicitly associated.

  *Example:* `"RATIS/THREE"` or `null`


---

### **Example Response**

```json
{
  "data": {
    "totalCount": 3,
    "prevKey": 3019,
    "containers": [
      { "ContainerID": 1, "NumberOfKeys": 834, "pipelines": null },
      { "ContainerID": 2, "NumberOfKeys": 833, "pipelines": null },
      { "ContainerID": 3, "NumberOfKeys": 833, "pipelines": null }
    ]
  }
}

```

---

### **Usage Notes**

- Use `totalCount` to summarize the total containers Recon is aware of.
- Use `prevKey` for pagination when fetching large container sets.
- Each container’s `NumberOfKeys` gives a quick density measure of object distribution.
- The `pipelines` field is primarily used to trace replication topologies or identify pipeline failures.

---

### **Example Natural-Language Mappings**

- “How many containers exist?” → return `data.totalCount`
- “List all container IDs.” → iterate over `data.containers[].ContainerID`
- “How many keys are in container 1?” → `NumberOfKeys` where `ContainerID=1`
- “What pipeline is container 5 using?” → `pipelines` for that container

---

## **Schema: DeletedContainers**

**Purpose:**

Represents metadata for containers that have been marked as **DELETED** in the Storage Container Manager (SCM).

Returned by the `/containers/deleted` endpoint to help administrators track cleanup status and historical deletion events.

---

### **Structure**

**Root Type:**

An **array** of deleted container objects. Each object represents one deleted container and its replication configuration at the time of deletion.

---

### **Fields**

Each entry in the array includes:

- **`containerId`** *(integer)* — Unique identifier of the deleted container.

  *Example:* `1015`

- **`pipelineId`** *(object)* — Identifier of the pipeline associated with the container before deletion.
  - **`id`** *(string)* — UUID of the pipeline.

    *Example:* `"9c8a3a15-7e1b-4d92-99f0-83b5d33fcb23"`

- **`containerState`** *(string)* — Lifecycle state of the container at deletion time.

  Possible values: `DELETED`, `CLOSING`, `QUASI_CLOSED`, `OPEN`.

  *Example:* `"DELETED"`

- **`stateEnterTime`** *(integer)* — Epoch timestamp when the container transitioned into its final state.

  *Example:* `1706127000000`

- **`lastUsed`** *(integer)* — Epoch timestamp of the last I/O or replication activity before deletion.

  *Example:* `1706104000000`

- **`replicationConfig`** *(object)* — Replication configuration used when the container was active.
  - **`replicationType`** *(string)* — Replication mechanism (e.g., `RATIS`, `STAND_ALONE`, `EC`).

    *Example:* `"RATIS"`

  - **`replicationFactor`** *(string)* — Replication factor label (e.g., `ONE`, `THREE`).

    *Example:* `"THREE"`

  - **`replicationNodes`** *(integer)* — Number of nodes hosting replicas of this container.

    *Example:* `3`

- **`replicationFactor`** *(string)* — Legacy or flattened field for replication factor (maintained for backward compatibility).

  *Example:* `"THREE"`


---

### **Example Response**

```json
[
  {
    "containerId": 1015,
    "pipelineId": { "id": "9c8a3a15-7e1b-4d92-99f0-83b5d33fcb23" },
    "containerState": "DELETED",
    "stateEnterTime": 1706127000000,
    "lastUsed": 1706104000000,
    "replicationConfig": {
      "replicationType": "RATIS",
      "replicationFactor": "THREE",
      "replicationNodes": 3
    },
    "replicationFactor": "THREE"
  }
]

```

---

### **Usage Notes**

- Used by **Recon admins** to identify deleted containers that may still exist in SCM metadata.
- Useful for cross-verifying cleanup between SCM and OM.
- `lastUsed` helps identify inactive or orphaned containers prior to deletion.
- `replicationConfig` assists in analyzing deletion behavior across replication schemes.

---

### **Natural-Language Query Mappings**

- “Show all deleted containers.” → list of `containerId` from array.
- “When was container 1015 deleted?” → `stateEnterTime` for that container.
- “Which replication type was used for deleted containers?” → `replicationConfig.replicationType`.
- “List deleted containers last used before yesterday.” → filter by `lastUsed` timestamp.
- “How many replicas did deleted container 1015 have?” → `replicationConfig.replicationNodes`.

---

## **Schema: KeyMetadata**

**Purpose:**

Represents metadata for all keys (files or objects) tracked by Recon. Used by `/keys`-related endpoints to display stored keys, their locations, versions, and associated block details. Enables file-level visibility into Ozone’s object namespace.

---

### **Structure**

**Root Object:**

- **`totalCount`** *(integer)* — Total number of keys returned in this response.

  *Example:* `7`

- **`lastKey`** *(string)* — The key name or path marking the last entry in the current result page (used for pagination).

  *Example:* `\"/vol1/buck1/file1\"`

- **`keys[]`** *(array)* — List of key metadata objects representing individual files.

---

### **Key Object Fields**

Each key entry includes:

- **`Volume`** *(string)* — Name of the volume containing the key.

  *Example:* `vol-1-73141`

- **`Bucket`** *(string)* — Name of the bucket containing the key.

  *Example:* `bucket-3-35816`

- **`Key`** *(string)* — Internal identifier of the key within the bucket.

  *Example:* `key-0-43637`

- **`CompletePath`** *(string)* — Full path of the key (including nested directories for FSO layouts).

  *Example:* `/vol1/buck1/dir1/dir2/file1`

- **`DataSize`** *(integer)* — Logical data size of the key (unreplicated).

  *Example:* `1000`

- **`Versions`** *(array of integers)* — List of version numbers associated with this key (for versioned buckets).

  *Example:* `[0]`

- **`Blocks`** *(object)* — Mapping of version → block list, representing how the key’s data is distributed across containers.

  Example structure:

    ```json
    {
      "0": [
        { "containerID": 1, "localID": 105232659753992201 }
      ]
    }
    
    ```

  - **`containerID`** *(integer)* — Container that stores this block.
  - **`localID`** *(number)* — Local block ID within the container.
- **`CreationTime`** *(string, date-time)* — ISO-8601 timestamp when the key was first created.

  *Example:* `2020-11-18T18:09:17.722Z`

- **`ModificationTime`** *(string, date-time)* — ISO-8601 timestamp of the key’s most recent modification.

  *Example:* `2020-11-18T18:09:30.405Z`


---

### **Example Response**

```json
{
  "totalCount": 7,
  "lastKey": "/vol1/buck1/file1",
  "keys": [
    {
      "Volume": "vol-1-73141",
      "Bucket": "bucket-3-35816",
      "Key": "key-0-43637",
      "CompletePath": "/vol1/buck1/dir1/dir2/file1",
      "DataSize": 1000,
      "Versions": [0],
      "Blocks": {
        "0": [
          { "containerID": 1, "localID": 105232659753992201 }
        ]
      },
      "CreationTime": "2020-11-18T18:09:17.722Z",
      "ModificationTime": "2020-11-18T18:09:30.405Z"
    }
  ]
}

```

---

### **Usage Notes**

- Each key maps to one or more data blocks stored in different containers.
- `Blocks` field allows correlating file-level data to container-level diagnostics.
- `Versions` field is critical for versioned buckets.
- `DataSize` reports logical size; physical usage can be higher due to replication.

---

### **Natural-Language Query Mappings**

- “List all keys in a bucket.” → iterate over `keys[].Key`.
- “Show key paths under volume X.” → use `CompletePath`.
- “How big is file1?” → `DataSize`.
- “When was this file last modified?” → `ModificationTime`.
- “Which containers store this file?” → `Blocks[].containerID`.
- “How many versions exist for key Y?” → count of `Versions`.

---

## **Schema: ReplicaHistory**

**Purpose:**

Tracks per-container replica history across Datanodes. Used by `/containers/{id}/replicaHistory` endpoint to analyze replica movement, node participation, and health over time.

---

### **Structure**

**Root Object Fields:**

- **`containerID`** *(integer)* — Identifier of the container being tracked.

  *Example:* `1`

- **`datanodeUuid`** *(string)* — UUID of the Datanode that hosted this container replica.

  *Example:* `841be80f-0454-47df-b676`

- **`datanodeHost`** *(string)* — Hostname of the Datanode.

  *Example:* `localhost-1`

- **`firstSeenTime`** *(number)* — Epoch timestamp when this replica was first detected by Recon.

  *Example:* `1605724047057`

- **`lastSeenTime`** *(number)* — Epoch timestamp when this replica was last confirmed present.

  *Example:* `1605731201301`

- **`lastBcsId`** *(integer)* — Last known Block Commit Sequence ID for this replica.

  *Example:* `123`

- **`state`** *(string)* — Replica state, e.g. `OPEN`, `CLOSED`, `QUASI_CLOSED`, `UNHEALTHY`.

  *Example:* `OPEN`


---

### **Example Response**

```json
{
  "containerID": 1,
  "datanodeUuid": "841be80f-0454-47df-b676",
  "datanodeHost": "localhost-1",
  "firstSeenTime": 1605724047057,
  "lastSeenTime": 1605731201301,
  "lastBcsId": 123,
  "state": "OPEN"
}

```

---

### **Usage Notes**

- Used primarily for **historical audit** of container replica states.
- `firstSeenTime` and `lastSeenTime` help detect lost or stale replicas.
- `lastBcsId` tracks replication synchronization progress between replicas.
- Can be correlated with `/containers/unhealthy` for failure diagnosis.

---

### **Natural-Language Query Mappings**

- “Show replica history for container 5.” → `/containers/5/replicaHistory`.
- “Which Datanodes held container 2?” → list of `datanodeHost`.
- “When was this replica last seen?” → `lastSeenTime`.
- “Which replicas are in OPEN state?” → filter by `state`.
- “Find replicas that disappeared recently.” → compare `lastSeenTime` to current time.

---

## **Schema: ReplicaHistory**

**Purpose:**

Describes the replica lifecycle of a specific container on each Datanode. Used by the `/containers/{id}/replicaHistory` endpoint to audit how container replicas moved or changed state over time.

---

### **Fields**

- **`containerID`** *(integer)* — Unique identifier of the container being tracked.

  *Example:* `1`

- **`datanodeUuid`** *(string)* — Unique UUID of the Datanode hosting the replica.

  *Example:* `"841be80f-0454-47df-b676"`

- **`datanodeHost`** *(string)* — Hostname of the Datanode.

  *Example:* `"localhost-1"`

- **`firstSeenTime`** *(number)* — Epoch timestamp when the replica was first observed by Recon.

  *Example:* `1605724047057`

- **`lastSeenTime`** *(number)* — Epoch timestamp when the replica was last detected.

  *Example:* `1605731201301`

- **`lastBcsId`** *(integer)* — Last known Block Commit Sequence ID (used for replication sync tracking).

  *Example:* `123`

- **`state`** *(string)* — Replica state; values may include `OPEN`, `CLOSED`, `QUASI_CLOSED`, or `UNHEALTHY`.

  *Example:* `"OPEN"`


---

### **Usage Notes**

- Tracks the *availability timeline* of a container replica on a given node.
- Can detect replicas that have disappeared, reappeared, or stayed stale.
- Used for correlating with unhealthy container reports.

---

### **Natural-Language Mappings**

- “Which Datanodes hosted container 5?” → list of `datanodeHost`.
- “When was the replica last seen?” → `lastSeenTime`.
- “What is the replica state of container 10?” → `state`.
- “Show all replicas for container 3.” → use `/containers/3/replicaHistory`.

---

### **Example Response**

```json
{
  "containerID": 1,
  "datanodeUuid": "841be80f-0454-47df-b676",
  "datanodeHost": "localhost-1",
  "firstSeenTime": 1605724047057,
  "lastSeenTime": 1605731201301,
  "lastBcsId": 123,
  "state": "OPEN"
}

```

---

## **Schema: MissingContainerMetadata**

**Purpose:**

Represents containers currently **missing from the expected replication topology**. Returned by `/containers/missing` to identify containers whose replicas cannot be located across Datanodes.

---

### **Fields**

- **`totalCount`** *(integer)* — Total number of missing containers returned in the response.

  *Example:* `26`

- **`containers[]`** *(array)* — List of individual missing container records, each representing one missing container.

---

### **Container Object Fields**

- **`containerID`** *(integer)* — ID of the missing container.

  *Example:* `1`

- **`missingSince`** *(number)* — Epoch timestamp indicating when Recon first marked the container as missing.

  *Example:* `1605731029145`

- **`keys`** *(integer)* — Number of keys that belong to this container. Useful for estimating impact.

  *Example:* `7`

- **`pipelineID`** *(string)* — UUID of the pipeline that originally managed this container.

  *Example:* `"88646d32-a1aa-4e1a"`

- **`replicas[]`** *(array of `ReplicaHistory` objects)* — Historical replica data per Datanode, showing where the container was last seen and its past states.

  Each item follows the same structure as the `ReplicaHistory` schema above.


---

### **Usage Notes**

- A container appears here if Recon cannot confirm sufficient healthy replicas via SCM reports.
- `missingSince` can be used to monitor how long data has been unavailable.
- `replicas` provide forensic insight into the last known hosts and states before disappearance.
- Often cross-referenced with `/containers/unhealthy` or `/datanodes` for diagnosis.

---

### **Natural-Language Mappings**

- “List missing containers.” → iterate over `containers[].containerID`.
- “When did container 12 go missing?” → `missingSince`.
- “Which pipeline was container 15 in?” → `pipelineID`.
- “Show last known replicas for container 5.” → entries under `replicas[]`.
- “How many keys were affected by missing containers?” → sum of `keys`.

---

### **Example Response**

```json
{
  "totalCount": 26,
  "containers": [
    {
      "containerID": 1,
      "missingSince": 1605731029145,
      "keys": 7,
      "pipelineID": "88646d32-a1aa-4e1a",
      "replicas": [
        {
          "containerID": 1,
          "datanodeUuid": "841be80f-0454-47df-b676",
          "datanodeHost": "localhost-1",
          "firstSeenTime": 1605724047057,
          "lastSeenTime": 1605731201301,
          "lastBcsId": 123,
          "state": "OPEN"
        }
      ]
    }
  ]
}

```

---

### **Routing Notes**

- When the user asks for *“missing containers”*, use `/containers/missing`.
- If the user requests *“when a container went missing”*, extract `missingSince`.
- For *replica history of missing containers*, read nested `replicas[]`.
- Combine `keys` with `totalCount` for aggregate impact summaries.
- If no containers are missing, respond: *“All containers are currently accounted for; no missing entries found.”*

---

## **Schema: UnhealthyContainerMetadata**

**Purpose:**

Represents all containers in an **unhealthy state**, including missing, under-replicated, over-replicated, and mis-replicated containers.

Used by `/containers/unhealthy` and `/containers/unhealthy/{state}` endpoints to summarize container-level health issues in the Ozone cluster.

---

### **Top-Level Fields**

- **`missingCount`** *(integer)* — Number of containers currently in the **MISSING** state.

  *Example:* `2`

- **`underReplicatedCount`** *(integer)* — Number of containers that have fewer replicas than expected.

  *Example:* `0`

- **`overReplicatedCount`** *(integer)* — Number of containers that have more replicas than expected.

  *Example:* `0`

- **`misReplicatedCount`** *(integer)* — Number of containers that are misaligned with the expected replication policy (e.g., data placement violation).

  *Example:* `0`

- **`containers[]`** *(array)* — Detailed information for each container identified as unhealthy.

---

### **Container Object Fields**

Each entry in the `containers[]` array describes one unhealthy container:

- **`containerID`** *(integer)* — Unique ID of the unhealthy container.

  *Example:* `1`

- **`containerState`** *(string)* — Health category of the container:

  Possible values: `MISSING`, `UNDER_REPLICATED`, `OVER_REPLICATED`, `MIS_REPLICATED`.

  *Example:* `"MISSING"`

- **`unhealthySince`** *(number)* — Epoch timestamp when Recon first identified the container as unhealthy.

  *Example:* `1605731029145`

- **`expectedReplicaCount`** *(integer)* — Number of replicas expected based on the container’s replication configuration.

  *Example:* `3`

- **`actualReplicaCount`** *(integer)* — Number of replicas currently detected across all Datanodes.

  *Example:* `0`

- **`replicaDeltaCount`** *(integer)* — Difference between expected and actual replicas.

  Positive values mean missing replicas; negative values mean excess replicas.

  *Example:* `3`

- **`reason`** *(string)* — Explanation of why the container is unhealthy (optional; may be null).

  *Example:* `"Missing replicas detected"`

- **`keys`** *(integer)* — Number of keys associated with the container. Indicates how much user data may be impacted.

  *Example:* `7`

- **`pipelineID`** *(string)* — UUID of the pipeline that originally hosted this container.

  *Example:* `"88646d32-a1aa-4e1a"`

- **`replicas[]`** *(array of `ReplicaHistory` objects)* — List of replica history entries showing last known replica details (host, timestamps, state, etc.) for diagnosis.

  See `ReplicaHistory` schema for structure.


---

### **Example Response**

```json
{
  "missingCount": 2,
  "underReplicatedCount": 0,
  "overReplicatedCount": 0,
  "misReplicatedCount": 0,
  "containers": [
    {
      "containerID": 1,
      "containerState": "MISSING",
      "unhealthySince": 1605731029145,
      "expectedReplicaCount": 3,
      "actualReplicaCount": 0,
      "replicaDeltaCount": 3,
      "reason": null,
      "keys": 7,
      "pipelineID": "88646d32-a1aa-4e1a",
      "replicas": [
        {
          "containerID": 1,
          "datanodeUuid": "841be80f-0454-47df-b676",
          "datanodeHost": "localhost-1",
          "firstSeenTime": 1605724047057,
          "lastSeenTime": 1605731201301,
          "lastBcsId": 123,
          "state": "OPEN"
        }
      ]
    }
  ]
}

```

---

### **Usage Notes**

- Each unhealthy category (`MISSING`, `UNDER_REPLICATED`, `OVER_REPLICATED`, `MIS_REPLICATED`) can be queried separately using `/containers/unhealthy/{state}`.
- `unhealthySince` helps track the duration of container unavailability.
- `replicaDeltaCount` quantifies severity: higher values indicate more missing replicas.
- The nested `replicas[]` list provides historical context for the Datanodes previously hosting the container.
- This schema is useful for identifying root causes of cluster imbalance or data risk.

---

### **Natural-Language Query Mappings**

| **User Query Example** | **Relevant Field(s)** | **Action / Endpoint** |
| --- | --- | --- |
| “List all unhealthy containers.” | `containers[].containerID` | `/containers/unhealthy` |
| “How many missing containers are there?” | `missingCount` | `/containers/unhealthy` |
| “Show all under-replicated containers.” | `underReplicatedCount` + `containers[]` | `/containers/unhealthy/UNDER_REPLICATED` |
| “When did container 10 become unhealthy?” | `unhealthySince` | `/containers/unhealthy` |
| “What’s the replication difference for container 15?” | `replicaDeltaCount` | `/containers/unhealthy` |
| “Which Datanodes last hosted container 5?” | `replicas[].datanodeHost` | `/containers/unhealthy` |

---

### **Routing Guide**

- Use `/containers/unhealthy` when the query includes generic phrases like *“unhealthy containers,” “replication issues,” “missing data,”* or *“replica imbalance.”*
- Use `/containers/unhealthy/{state}` when the query specifies *missing, under-replicated, over-replicated,* or *mis-replicated.*
- If the user asks *“Why is this container unhealthy?”* → check `reason` or infer from `expectedReplicaCount` vs `actualReplicaCount`.
- For *impact analysis*, use `keys` (how many objects are affected).
- When no unhealthy containers are found, reply:

  *“All containers are currently healthy; no unhealthy entries detected by Recon.”*


---

Every field, sub-object, and logical relationship is explicitly covered to ensure complete understanding and reliable natural-language mapping.

---

## **Schema: MismatchedContainers**

**Purpose:**

Describes discrepancies between **OM (Ozone Manager)** and **SCM (Storage Container Manager)** regarding container existence or metadata.

Used by the `/containers/mismatch` endpoint to identify containers that exist in one component but not the other, or whose replication configuration or state is inconsistent.

---

### **Top-Level Fields**

- **`lastKey`** *(integer)* — Marker used for pagination to retrieve the next set of mismatch records.

  *Example:* `21`

- **`containerDiscrepancyInfo[]`** *(array)* — List of objects representing each mismatched container and its details.

---

### **Container Discrepancy Object Fields**

Each object in `containerDiscrepancyInfo` represents one container that exhibits an inconsistency between OM and SCM:

### **Container Information**

- **`containerId`** *(integer)* — Unique identifier of the container showing mismatch.

  *Example:* `11`

- **`numberOfKeys`** *(integer)* — Number of keys (objects) associated with this container, as known to OM.

  *Example:* `1`


### **Pipeline Information**

- **`pipelines[]`** *(array)* — List of pipeline objects linked to this container.

  A container can have multiple associated pipelines if replication states changed over time.


Each pipeline entry includes:

- **`id`** *(object)* — Pipeline identifier.
  - **`id`** *(string)* — UUID representing the specific pipeline.

    *Example:* `"1202e6bb-b7c1-4a85-8067-61374b069adb"`

- **`replicationConfig`** *(object)* — Describes replication parameters used by the pipeline.
  - **`replicationFactor`** *(string)* — Number of replica copies configured (e.g., `ONE`, `TWO`, `THREE`).

    *Example:* `"THREE"`

  - **`requiredNodes`** *(integer)* — Number of datanodes expected to hold replicas.

    *Example:* `3`

  - **`replicationType`** *(string)* — Type of replication method (e.g., `RATIS`, `STAND_ALONE`, `EC`).

    *Example:* `"RATIS"`

- **`healthy`** *(boolean)* — Indicates if the pipeline was considered healthy when the mismatch was detected.

  *Example:* `true`


### **Existence Information**

- **`existsAt`** *(string)* — Location of the mismatch; identifies where the container exists but should not.

  Possible values:

  - `"OM"` → container exists in OM metadata but missing in SCM.
  - `"SCM"` → container exists in SCM but missing in OM.

    *Example:* `"OM"`


---

### **Example Response**

```json
{
  "lastKey": 21,
  "containerDiscrepancyInfo": [
    {
      "containerId": 2,
      "numberOfKeys": 2,
      "pipelines": [
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-61374b069adb" },
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "RATIS"
          },
          "healthy": true
        }
      ],
      "existsAt": "OM"
    },
    {
      "containerId": 11,
      "numberOfKeys": 2,
      "pipelines": [
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-61374b069adb" },
          "replicationConfig": {
            "replicationFactor": "TWO",
            "requiredNodes": 2,
            "replicationType": "RATIS"
          },
          "healthy": true
        },
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-613724nn" },
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "RATIS"
          },
          "healthy": true
        }
      ],
      "existsAt": "SCM"
    }
  ]
}

```

---

### **Usage Notes**

- These mismatches often occur when container state updates fail to sync between OM and SCM.
- `existsAt` determines which subsystem has the extra entry.
- `pipelines` help identify whether replication topology differences contributed to the mismatch.
- Healthy pipelines (`healthy: true`) indicate that container mismatch is likely due to metadata, not physical corruption.
- Used mainly for **reconciliation audits** between OM and SCM databases.

---

### **Natural-Language Mappings**

| **User Query Example** | **Relevant Field(s)** | **Recommended Endpoint** |
| --- | --- | --- |
| “Show all containers that exist in OM but not in SCM.” | `existsAt: OM` | `/containers/mismatch?missingIn=SCM` |
| “Which containers are mismatched between OM and SCM?” | `containerDiscrepancyInfo[]` | `/containers/mismatch` |
| “How many keys are stored in mismatched container 11?” | `numberOfKeys` | `/containers/mismatch` |
| “Show pipeline details for container 2.” | `pipelines[]` | `/containers/mismatch` |
| “Which mismatched containers use RATIS replication?” | `replicationConfig.replicationType` | `/containers/mismatch` |

---

### **Routing Guide**

- Use `/containers/mismatch` for queries involving **OM–SCM mismatches** or **metadata inconsistencies**.
- If the user mentions *“containers missing in SCM”* → filter where `existsAt = "OM"`.
- If the user mentions *“containers missing in OM”* → filter where `existsAt = "SCM"`.
- When analyzing replication or data integrity, extract from `replicationConfig` and `healthy`.
- For multi-page results, use `lastKey` for pagination.
- If no mismatches exist, respond with:

  *“All containers are consistent between OM and SCM; no mismatches found.”*


---

## **Schema: DeletedMismatchedContainers**

**Purpose:**

Represents containers that are **deleted in SCM** but **still present in OM metadata**.

Used by `/containers/mismatch/deleted` endpoint to find orphaned entries that should be purged or reconciled.

---

### **Top-Level Fields**

- **`lastKey`** *(integer)* — Pagination marker for retrieving additional results.

  *Example:* `21`

- **`containerDiscrepancyInfo[]`** *(array)* — List of container discrepancy records (same structure as in `MismatchedContainers`).

---

### **Container Discrepancy Object Fields**

- **`containerId`** *(integer)* — ID of the container found deleted in SCM but still existing in OM.

  *Example:* `11`

- **`numberOfKeys`** *(integer)* — Number of keys inside the container as known to OM.

  *Example:* `1`

- **`pipelines[]`** *(array)* — List of associated pipeline records, same structure as in `MismatchedContainers`:
  - **`id.id`** *(string)* — Pipeline UUID.
  - **`replicationConfig`** *(object)* with:
    - `replicationFactor` *(string)* — e.g., `ONE`, `TWO`, `THREE`.
    - `requiredNodes` *(integer)* — Expected replica node count.
    - `replicationType` *(string)* — e.g., `RATIS`, `STAND_ALONE`, `EC`.
  - **`healthy`** *(boolean)* — Indicates pipeline health.
- **`existsAt`** *(string)* *(optional in this schema)* — Often omitted, implicitly `"OM"` since these mismatches are OM-side remnants.

---

### **Example Response**

```json
{
  "lastKey": 21,
  "containerDiscrepancyInfo": [
    {
      "containerId": 2,
      "numberOfKeys": 2,
      "pipelines": [
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-61374b069adb" },
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "RATIS"
          },
          "healthy": true
        }
      ]
    },
    {
      "containerId": 11,
      "numberOfKeys": 2,
      "pipelines": [
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-61374b069adb" },
          "replicationConfig": {
            "replicationFactor": "TWO",
            "requiredNodes": 2,
            "replicationType": "RATIS"
          },
          "healthy": true
        },
        {
          "id": { "id": "1202e6bb-b7c1-4a85-8067-613724nn" },
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "RATIS"
          },
          "healthy": true
        }
      ]
    }
  ]
}

```

---

### **Usage Notes**

- Indicates **residual metadata** in OM that was not cleaned up after SCM deletion.
- Typically occurs during partial or failed container deletion workflows.
- Used by Recon to guide **cleanup and reconciliation jobs**.
- Since all entries here exist only in OM, the `existsAt` field is optional but implied.
- Can be cross-checked with `/containers/deleted` (active SCM deletions).

---

### **Natural-Language Mappings**

| **User Query Example** | **Relevant Field(s)** | **Recommended Endpoint** |
| --- | --- | --- |
| “Show containers deleted in SCM but still visible in OM.” | `containerDiscrepancyInfo[]` | `/containers/mismatch/deleted` |
| “List orphaned containers in OM.” | `containerId` | `/containers/mismatch/deleted` |
| “Which replication type do these deleted containers use?” | `replicationConfig.replicationType` | `/containers/mismatch/deleted` |
| “How many keys remain in deleted containers?” | `numberOfKeys` | `/containers/mismatch/deleted` |

---

### **Routing Guide**

- Use `/containers/mismatch/deleted` when queries mention *deleted containers still appearing in OM* or *inconsistent deletion*.
- Combine with `/containers/mismatch` when user requests *all types of container mismatches*.
- Use pipeline information to determine whether mismatch is purely metadata or linked to replication.
- If none found, respond:

  *“No deleted containers remain in OM; SCM and OM container metadata are consistent.”*


---

## **Schema: OpenKeysSummary**

**Purpose:**

Provides aggregated statistics about **all open keys** (in-progress or uncommitted files) in the Ozone cluster.

Returned by the `/keys/open/summary` endpoint.

---

### **Fields**

- **`totalUnreplicatedDataSize`** *(integer)* — Total size (in bytes) of all open keys **before replication**. Represents actual user data written but not yet closed.

  *Example:* `4608`

- **`totalReplicatedDataSize`** *(integer)* — Total size (in bytes) of all open keys **after applying replication factor**. Represents how much cluster capacity is occupied.

  *Example:* `13824`

- **`totalOpenKeys`** *(integer)* — Number of open keys currently tracked by Recon.

  *Example:* `57`


---

### **Usage Notes**

- Used to quickly assess cluster-level **write workload** or **pending uploads**.
- Helpful for detecting long-standing open files that may cause space leaks.
- Often correlated with `/keys/open` for detailed per-file information.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “How many open keys are there?” | `totalOpenKeys` |
| “What is the total unreplicated data size?” | `totalUnreplicatedDataSize` |
| “How much cluster space is used by open keys?” | `totalReplicatedDataSize` |

---

## **Schema: OpenKeys**

**Purpose:**

Provides a **detailed listing of all open keys**, including FSO (File System Optimized) and non-FSO layouts, their sizes, replication metadata, and timestamps.

Returned by `/keys/open`.

---

### **Required Fields**

- `lastKey`
- `replicatedDataSize`
- `unreplicatedDataSize`
- `status`

---

### **Fields**

### **Top-Level**

- **`lastKey`** *(string)* — The final key path in the current response page; used for pagination.

  *Example:* `/vol1/fso-bucket/dir1/dir2/file2`

- **`replicatedDataSize`** *(integer)* — Total replicated data size for the keys returned in this batch.

  *Example:* `13824`

- **`unreplicatedDataSize`** *(integer)* — Total unreplicated (logical) data size.

  *Example:* `4608`

- **`status`** *(string)* — Operation status (e.g., `"SUCCESS"`, `"PARTIAL"`, `"ERROR"`).

  *Example:* `"SUCCESS"`


---

### **FSO Array**

- **`fso[]`** *(array)* — List of open keys under **File System Optimized (FSO)** buckets.

  Each entry includes:

  - **`path`** *(string)* — Full hierarchical path of the key.
  - **`key`** *(string)* — Internal key name identifier.
  - **`inStateSince`** *(number)* — Epoch timestamp since the key entered the “open” state.
  - **`size`** *(integer)* — Logical file size in bytes.
  - **`replicatedSize`** *(integer)* — Physical size after replication.
  - **`replicationInfo`** *(object)* — Replication metadata:
    - **`replicationFactor`** *(string)* — e.g., `THREE`, `ONE`.
    - **`requiredNodes`** *(integer)* — Number of replicas expected.
    - **`replicationType`** *(string)* — e.g., `RATIS`, `EC`.
  - **`creationTime`** *(integer)* — Epoch timestamp when the key was created.
  - **`modificationTime`** *(integer)* — Epoch timestamp when it was last modified.
  - **`isKey`** *(boolean)* — Whether the record represents an actual file (true) or directory (false).

---

### **Non-FSO Array**

- **`nonFSO[]`** *(array)* — List of open keys under **non-FSO (Legacy or OBS)** buckets.

  Same structure as `fso[]` with identical subfields.


---

### **Example Response**

```json
{
  "lastKey": "/vol1/fso-bucket/dir1/dir2/file2",
  "replicatedDataSize": 13824,
  "unreplicatedDataSize": 4608,
  "status": "SUCCESS",
  "fso": [
    {
      "path": "/vol1/fso-bucket/dir1/dir2/file2",
      "key": "file2",
      "inStateSince": 1713700000000,
      "size": 2048,
      "replicatedSize": 6144,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1713600000000,
      "modificationTime": 1713705000000,
      "isKey": true
    }
  ],
  "nonFSO": []
}

```

---

### **Usage Notes**

- Tracks all files currently open (i.e., not yet committed/closed).
- Distinguishes between FSO and legacy buckets.
- Supports incremental fetching using `lastKey`.
- Used for debugging upload failures or incomplete multipart uploads.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “List all open files.” | `fso[].path` + `nonFSO[].path` |
| “Show total size of open files.” | `replicatedDataSize` / `unreplicatedDataSize` |
| “Which files are stuck open in FSO buckets?” | `fso[]` |
| “When did a key become open?” | `inStateSince` |

---

## **Schema: OMKeyInfoList**

**Purpose:**

Represents full metadata for **all keys known to the Ozone Manager (OM)**.

Returned by internal Recon APIs that expose OM database content for diagnostics and debugging.

---

### **Type:** `array` (of key metadata objects)

Each object contains:

- **`metadata`** *(object)* — Arbitrary system metadata key-value pairs.
- **`objectID`** *(number)* — Unique identifier of the key object.
- **`updateID`** *(number)* — Version or update sequence ID.
- **`parentObjectID`** *(number)* — Identifier of the parent directory or object (for hierarchical storage).
- **`volumeName`** *(string)* — Volume the key belongs to.
- **`bucketName`** *(string)* — Bucket that contains this key.
- **`keyName`** *(string)* — Key’s logical name (file identifier).
- **`dataSize`** *(number)* — Logical file size in bytes.
- **`keyLocationVersions[]`** *(array)* — List of **VersionLocation** objects (see below).
- **`creationTime`** *(number)* — Epoch timestamp of creation.
- **`modificationTime`** *(number)* — Epoch timestamp of last modification.
- **`replicationConfig`** *(object)* — Replication settings:
  - **`replicationFactor`** *(string)* — e.g., `THREE`, `ONE`.
  - **`requiredNodes`** *(integer)* — Node count for replication.
  - **`replicationType`** *(string)* — e.g., `RATIS`, `EC`.
- **`fileChecksum`** *(number, nullable)* — Optional file checksum (if enabled).
- **`fileName`** *(string)* — File name.
- **`ownerName`** *(string)* — Owner of the key.
- **`acls`** *(object)* — Access control list (via ACL schema).
- **`tags`** *(object)* — User-defined metadata tags.
- **`expectedDataGeneration`** *(string, nullable)* — Expected data generation marker.
- **`file`** *(boolean)* — Indicates if this entry is a file (`true`) or directory (`false`).
- **`path`** *(string)* — Full path to the file.
- **`generation`** *(integer)* — Generation counter for this object.
- **`replicatedSize`** *(number)* — Physical size after replication.
- **`fileEncryptionInfo`** *(string, nullable)* — File encryption metadata (if encryption enabled).
- **`objectInfo`** *(string)* — Serialized object information (used internally).
- **`latestVersionLocations`** *(object)* — Single `VersionLocation` entry for the latest version.
- **`hsync`** *(boolean)* — Whether the file is synced to disk (`hsync` flag).

---

### **Example (Simplified)**

```json
[
  {
    "volumeName": "vol1",
    "bucketName": "buck1",
    "keyName": "file1",
    "dataSize": 2048,
    "replicationConfig": {
      "replicationFactor": "THREE",
      "requiredNodes": 3,
      "replicationType": "RATIS"
    },
    "ownerName": "ozone",
    "path": "/vol1/buck1/file1",
    "file": true,
    "replicatedSize": 6144,
    "hsync": true
  }
]

```

---

## **Schema: VersionLocation**

**Purpose:**

Represents the **block-level versioning layout** for a key.

Each key may have one or more versions (for versioned buckets).

---

### **Fields**

- **`version`** *(integer)* — Version number of the key.
- **`locationVersionMap`** *(object)* — Maps version identifiers to location lists.
  - Key: version index (e.g., `0`), Value: `LocationList`.
- **`multipartKey`** *(boolean)* — Indicates if this version belongs to a multipart upload.
- **`blocksLatestVersionOnly`** *(LocationList)* — Blocks belonging only to the latest version.
- **`locationListCount`** *(integer)* — Number of location lists for this key version.
- **`locationLists[]`** *(array)* — Array of `LocationList` objects for different blocks.
- **`locationList`** *(LocationList)* — Single list of blocks for this version.

---

## **Schema: LocationList**

**Purpose:**

Represents the list of **physical block locations** for a specific key or version.

Used to map from logical file data to actual container IDs and offsets.

---

### **Type:** `array` (of block objects)

Each block object includes:

- **`blockID`** *(object)* — Identifies the block uniquely.
  - **`containerBlockID`** *(object)* — Nested identifier:
    - **`containerID`** *(integer)* — Container hosting the block.
    - **`localID`** *(integer)* — Local block identifier.
  - **`blockCommitSequenceID`** *(integer)* — Commit sequence identifier.
  - **`replicaIndex`** *(integer, nullable)* — Index of replica (if multiple replicas).
  - **`containerID`** *(integer)* — Container ID (redundant for quick access).
  - **`localID`** *(integer)* — Local block ID (redundant for quick access).
- **`length`** *(integer)* — Length of the data block in bytes.
- **`offset`** *(integer)* — Starting offset of this block within the key’s total data stream.
- **`token`** *(string, nullable)* — Access token for secure block reads (if applicable).
- **`createVersion`** *(integer)* — Version when this block was created.
- **`pipeline`** *(string, nullable)* — Pipeline identifier assigned during block creation.
- **`partNumber`** *(integer)* — For multipart uploads, denotes which part the block belongs to.
- **`underConstruction`** *(boolean)* — Indicates whether the block is still being written.
- **`blockCommitSequenceId`** *(integer)* — Latest committed sequence ID for this block.
- **`containerID`** *(integer)* — Container ID (duplicate of blockID.containerBlockID.containerID).
- **`localID`** *(integer)* — Local ID (duplicate of blockID.containerBlockID.localID).

---

### **Example (Condensed)**

```json
[
  {
    "blockID": {
      "containerBlockID": { "containerID": 1, "localID": 105232659753992201 },
      "blockCommitSequenceID": 100,
      "replicaIndex": 0,
      "containerID": 1,
      "localID": 105232659753992201
    },
    "length": 1048576,
    "offset": 0,
    "createVersion": 1,
    "pipeline": "pipeline-abc",
    "partNumber": 1,
    "underConstruction": false,
    "blockCommitSequenceId": 100,
    "containerID": 1,
    "localID": 105232659753992201
  }
]

```

---

### **Usage Notes**

- Enables detailed tracing of **where key data physically resides**.
- Essential for debugging block corruption, replication issues, and incomplete multipart uploads.
- `underConstruction` helps detect partially written blocks.
- Fields are often nested inside higher-level structures (`VersionLocation` → `LocationList`).

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “Show physical blocks for a key.” | `locationLists[].blockID.containerBlockID` |
| “Where is version 3 of this key stored?” | `version` + `locationVersionMap` |
| “How big is each block?” | `length` |
| “Which container hosts this block?” | `containerID` |
| “Are there blocks still under construction?” | `underConstruction` |

---

### **Routing Guide**

- Use `OpenKeys` and `OpenKeysSummary` for **active/open file tracking**.
- Use `OMKeyInfoList` to access **static metadata** for stored or versioned keys.
- Traverse `VersionLocation → LocationList → blockID` to resolve **data lineage** and **physical storage mapping**.
- Respond with hierarchical clarity when users ask “where”, “how big”, or “how replicated” questions.
- If `nonFSO` is empty, clarify that only FSO-based open keys exist.
- For incomplete uploads or multipart debugging, check `multipartKey` and `underConstruction`.

---

`DeletePendingKeys`, `DeletePendingSummary`, `DeletePendingDirs`, `DeletePendingBlocks`, and `ACL`.

All parameters and sub-fields are included, with full structural, contextual, and reasoning details.

---

## **Schema: DeletePendingKeys**

**Purpose:**

Represents **keys (files or objects)** that are pending deletion in Ozone.

Returned by `/keys/deletePending` endpoint to show files marked for removal but not yet physically purged from the cluster.

---

### **Top-Level Fields**

- **`lastKey`** *(string)* — The final key name in the current result page; used for pagination.

  *Example:* `"sampleVol/bucketOne/key_one"`

- **`replicatedDataSize`** *(number)* — Total replicated data size (in bytes) of keys pending deletion in this page.

  *Example:* `300000000`

- **`unreplicatedDataSize`** *(number)* — Total unreplicated (logical) size of pending keys.

  *Example:* `100000000`

- **`deletedKeyInfo[]`** *(array)* — List of pending-deletion key groups. Each element represents one or more OM keys with their cumulative size.
- **`status`** *(string)* — Request result status (e.g., `"OK"`, `"FAILED"`).

  *Example:* `"OK"`


---

### **deletedKeyInfo Object Fields**

- **`omKeyInfoList`** *(array)* — Reference to the full OM key metadata (`OMKeyInfoList` schema).

  Each entry includes key path, replication config, ownership, ACLs, etc.

- **`totalSize`** *(object)* — Map of **replication index → size (bytes)**.

  Example structure:

    ```json
    { "63": 189 }
    
    ```

  - **Key (`63`)** — Internal block or node index.
  - **Value (`189`)** — Size in bytes of pending deletion data.

---

### **Example Response**

```json
{
  "lastKey": "sampleVol/bucketOne/key_one",
  "replicatedDataSize": 300000000,
  "unreplicatedDataSize": 100000000,
  "deletedKeyInfo": [
    {
      "omKeyInfoList": [
        {
          "volumeName": "sampleVol",
          "bucketName": "bucketOne",
          "keyName": "key_one",
          "dataSize": 1024
        }
      ],
      "totalSize": { "63": 189 }
    }
  ],
  "status": "OK"
}

```

---

### **Usage Notes**

- Displays **logical vs replicated** deletion size to estimate cleanup impact.
- `totalSize` is often used internally to group by deletion batches.
- Data remains visible here until background cleanup (OM/Recon) removes physical blocks.

---

### **Natural-Language Mappings**

| User Query | Relevant Field |
| --- | --- |
| “List all keys pending deletion.” | `deletedKeyInfo[].omKeyInfoList[].keyName` |
| “Total size of pending deletions.” | `replicatedDataSize` / `unreplicatedDataSize` |
| “Which volumes still have undeleted keys?” | `omKeyInfoList[].volumeName` |
| “How large is deletion batch 63?” | `totalSize["63"]` |

---

## **Schema: DeletePendingSummary**

**Purpose:**

Provides **aggregated statistics** for all delete-pending keys cluster-wide.

Returned by `/keys/deletePending/summary`.

---

### **Fields**

- **`totalUnreplicatedDataSize`** *(integer)* — Logical total size of all pending deletions.
- **`totalReplicatedDataSize`** *(integer)* — Physical total (with replication).
- **`totalDeletedKeys`** *(integer)* — Number of keys pending deletion.

---

### **Usage Notes**

Used to evaluate **storage reclaim backlog**.

High replicated data size relative to unreplicated indicates heavy replication overhead.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “How many keys are waiting to be deleted?” | `totalDeletedKeys` |
| “Total bytes occupied by undeleted keys?” | `totalReplicatedDataSize` |
| “Raw user data still marked for deletion?” | `totalUnreplicatedDataSize` |

---

## **Schema: DeletePendingDirs**

**Purpose:**

Lists **directories** pending deletion.

Returned by `/dirs/deletePending` to show uncleaned directory paths in FSO (File System Optimized) layouts.

---

### **Top-Level Fields**

- **`lastKey`** *(string)* — The last directory key in this response page.

  *Example:* `"vol1/bucket1/bucket1/dir1"`

- **`replicatedDataSize`** *(integer)* — Total replicated size of directory data pending deletion.

  *Example:* `13824`

- **`unreplicatedDataSize`** *(integer)* — Logical total size before replication.

  *Example:* `4608`

- **`deletedDirInfo[]`** *(array)* — List of directory entries awaiting deletion.
- **`status`** *(string)* — Operation status.

  *Example:* `"OK"`


---

### **deletedDirInfo Object Fields**

Each entry represents one directory record:

- **`path`** *(string)* — Full directory path.
- **`key`** *(string)* — Directory key identifier.
- **`inStateSince`** *(number)* — Epoch time when deletion was initiated.
- **`size`** *(integer)* — Logical size of data inside this directory.
- **`replicatedSize`** *(integer)* — Physical size (replication applied).
- **`replicationInfo`** *(object)* — Replication configuration:
  - **`replicationFactor`** *(string)* — e.g., `THREE`, `ONE`.
  - **`requiredNodes`** *(integer)* — e.g., `3`.
  - **`replicationType`** *(string)* — e.g., `RATIS`, `EC`.
- **`creationTime`** *(integer)* — Directory creation epoch.
- **`modificationTime`** *(integer)* — Last modified epoch.
- **`isKey`** *(boolean)* — Indicates if entry represents a file instead of a directory (true = file).

---

### **Example Response**

```json
{
  "lastKey": "vol1/bucket1/bucket1/dir1",
  "replicatedDataSize": 13824,
  "unreplicatedDataSize": 4608,
  "deletedDirInfo": [
    {
      "path": "/vol1/bucket1/dir1",
      "key": "dir1",
      "inStateSince": 1713710000000,
      "size": 2048,
      "replicatedSize": 6144,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1713600000000,
      "modificationTime": 1713700000000,
      "isKey": false
    }
  ],
  "status": "OK"
}

```

---

### **Usage Notes**

- Tracks FSO directories queued for deletion.
- Useful for identifying incomplete directory cleanup after large file removals.
- `inStateSince` → detects aging deletions.
- `isKey = true` indicates mis-categorized file entries under directory cleanup.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “List all directories pending deletion.” | `deletedDirInfo[].path` |
| “When did deletion start for dir1?” | `inStateSince` |
| “What’s the total size of pending directory deletions?” | `replicatedDataSize` |

---

## **Schema: DeletePendingBlocks**

**Purpose:**

Represents **data blocks** pending deletion across containers.

Returned by `/blocks/deletePending` endpoint for low-level cleanup visibility.

---

### **Fields**

Each property under this schema represents a container state or block category (e.g., `"OPEN"`, `"CLOSED"`).

For example, `OPEN` → array of block deletion entries.

Each element inside such arrays contains:

- **`containerId`** *(number)* — ID of the container holding these pending blocks.

  *Example:* `100`

- **`localIDList[]`** *(array of integers)* — List of local block IDs pending deletion.

  *Example:* `[1, 2, 3, 4]`

- **`localIDCount`** *(integer)* — Number of local IDs in this deletion batch.

  *Example:* `4`

- **`txID`** *(number)* — Transaction ID for the deletion event or batch.

  *Example:* `1`


---

### **Example Response**

```json
{
  "OPEN": [
    {
      "containerId": 100,
      "localIDList": [1, 2, 3, 4],
      "localIDCount": 4,
      "txID": 1
    }
  ]
}

```

---

### **Usage Notes**

- Identifies un-cleaned blocks even after key deletion.
- `txID` ties the pending operation to SCM or OM transaction logs.
- Can be grouped by container to monitor cleanup backlog.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “Show blocks pending deletion in container 100.” | `OPEN[].localIDList` |
| “How many block IDs remain?” | `localIDCount` |
| “What transaction triggered these deletions?” | `txID` |

---

## **Schema: ACL**

**Purpose:**

Defines the **Access Control List** structure applied to volumes, buckets, and keys across all Recon objects.

---

### **Fields**

- **`type`** *(string)* — Principal type (e.g., `"USER"`, `"GROUP"`).
- **`name`** *(string)* — Principal name (user or group).
- **`aclScope`** *(string)* — Scope of ACL: `"ACCESS"` or `"DEFAULT"`.
- **`aclList[]`** *(array of strings)* — Permission list, such as `"READ"`, `"WRITE"`, `"ALL"`.

---

### **Example**

```json
{
  "type": "USER",
  "name": "ozone",
  "aclScope": "ACCESS",
  "aclList": ["READ", "WRITE"]
}

```

---

### **Usage Notes**

- Appears within `OMKeyInfoList`, `Buckets`, and directory structures.
- Used to answer “who can access what” queries.
- Supports multi-entry lists for different users/groups.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “Who owns this key?” | `name` |
| “What permissions does user ozone have?” | `aclList` |
| “Show all ACLs on volume vol1.” | object’s embedded `ACL` list |

---

### **Routing Guide (Summary)**

| User Intent | Recommended Endpoint | Key Fields |
| --- | --- | --- |
| “Pending key deletions” | `/keys/deletePending` | `deletedKeyInfo[]`, `replicatedDataSize` |
| “List or filter keys/files in a bucket” | `/keys/listKeys` | `keys[]`, `startPrefix`, `replicationType`, `keySize` |
| “Pending directory deletions” | `/dirs/deletePending` | `deletedDirInfo[]` |
| “Pending block deletions” | `/blocks/deletePending` | `OPEN[].localIDList` |
| “Deletion statistics summary” | `/keys/deletePending/summary` | `totalDeletedKeys` |
| “Access control or ownership info” | (any schema with `ACL`) | `type`, `name`, `aclList` |

---

This section now exhaustively documents **every parameter and sub-object** under the Delete-Pending and ACL-related schemas, in full depth and consistent structure with your Recon API specification.


`NamespaceMetadataResponse`, `MetadataDiskUsage`, `MetadataQuota`, and `MetadataSpaceDist`.

All nested fields, array elements, and intended use-cases are explicitly covered.

---

## **Schema: NamespaceMetadataResponse**

**Purpose:**

Provides a **summary of namespace composition** — number of volumes, buckets, directories, and keys under a specific hierarchy in Ozone Recon.

Returned by `/namespace/metadata` endpoint.

---

### **Fields**

| Field | Type | Description | Example |
| --- | --- | --- | --- |
| **`status`** | `string` | Result status of the request (`OK`, `ERROR`, etc.). | `"OK"` |
| **`type`** | `string` | Type of the queried namespace level — one of `"VOLUME"`, `"BUCKET"`, `"DIRECTORY"`, or `"KEY"`. | `"BUCKET"` |
| **`numVolume`** | `number` | Total number of volumes under the queried scope. May be `-1` if query is below volume level. | `-1` |
| **`numBucket`** | `integer` | Number of buckets in the scope. | `100` |
| **`numDir`** | `number` | Number of directories found (FSO layout only). | `50` |
| **`numKey`** | `number` | Total number of keys (files) within this namespace path. | `400` |

---

### **Example**

```json
{
  "status": "OK",
  "type": "BUCKET",
  "numVolume": -1,
  "numBucket": 100,
  "numDir": 50,
  "numKey": 400
}

```

---

### **Usage Notes**

- Used in `/namespace/metadata` or `/namespace/summary` APIs to show a **hierarchical object count**.
- `numVolume = -1` indicates the query was scoped below volume level.
- Helps visualize namespace growth or estimate metadata load.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “How many keys are under bucket1?” | `numKey` |
| “Show the number of directories in this bucket.” | `numDir` |
| “List total buckets inside this volume.” | `numBucket` |

---

## **Schema: MetadataDiskUsage**

**Purpose:**

Reports **logical and replicated space usage** for a given path (volume, bucket, or directory).

Returned by `/namespace/usage` or `/namespace/usage?path=<target>`.

---

### **Top-Level Fields**

| Field | Type | Description | Example |
| --- | --- | --- | --- |
| **`status`** | `string` | Operation status (`OK`, `ERROR`). | `"OK"` |
| **`path`** | `string` | The queried path whose usage is computed. | `"/vol1/bucket1"` |
| **`size`** | `number` | Logical size (sum of user bytes) in bytes. | `150000` |
| **`sizeWithReplica`** | `number` | Physical size accounting for replication. | `450000` |
| **`subPathCount`** | `number` | Number of immediate subpaths (directories or keys). | `4` |
| **`subPaths[]`** | `array` | List of direct children (dirs/keys) with individual usage data. | – |
| **`sizeDirectKey`** | `number` | Total size of direct keys under this path (non-recursive). | `10000` |

---

### **subPaths Object Fields**

Each subPath represents one **direct child object** under the queried path.

| Field | Type | Description | Example |
| --- | --- | --- | --- |
| **`key`** | `boolean` | Indicates whether this entry represents a key (true) or directory (false). | `false` |
| **`path`** | `string` | Full path of the subdirectory or key. | `"/vol1/bucket1/dir1-1"` |
| **`size`** | `number` | Logical data size (bytes). | `30000` |
| **`sizeWithReplica`** | `number` | Replicated (physical) size. | `90000` |
| **`isKey`** | `boolean` | Duplicate of `key` for API consistency. | `false` |

---

### **Example**

```json
{
  "status": "OK",
  "path": "/vol1/bucket1",
  "size": 150000,
  "sizeWithReplica": 450000,
  "subPathCount": 4,
  "subPaths": [
    { "key": false, "path": "/vol1/bucket1/dir1-1", "size": 30000, "sizeWithReplica": 90000, "isKey": false },
    { "key": false, "path": "/vol1/bucket1/dir1-2", "size": 30000, "sizeWithReplica": 90000, "isKey": false },
    { "key": false, "path": "/vol1/bucket1/dir1-3", "size": 30000, "sizeWithReplica": 90000, "isKey": false },
    { "key": true,  "path": "/vol1/bucket1/key1-1", "size": 30000, "sizeWithReplica": 90000, "isKey": true }
  ],
  "sizeDirectKey": 10000
}

```

---

### **Usage Notes**

- Mirrors `du` (disk usage) semantics for object storage.
- `size` measures raw data; `sizeWithReplica` reflects replication (e.g., ×3 for RATIS).
- `subPaths` gives per-directory or per-file breakdown.
- `sizeDirectKey` isolates top-level files from recursive totals.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “What is the total disk usage of /vol1/bucket1?” | `size` / `sizeWithReplica` |
| “Show subdirectory usage under bucket1.” | `subPaths[]` |
| “How many child paths are there?” | `subPathCount` |
| “How much space do direct keys use?” | `sizeDirectKey` |

---

## **Schema: MetadataQuota**

**Purpose:**

Displays **quota limits and usage** for a path (volume or bucket).

Returned by `/namespace/quota`.

---

### **Fields**

| Field | Type | Description | Example |
| --- | --- | --- | --- |
| **`status`** | `string` | Request status. | `"OK"` |
| **`allowed`** | `number` | Maximum quota (bytes or objects) configured for this namespace. | `200000` |
| **`used`** | `number` | Current usage within the quota limit. | `160000` |

---

### **Usage Notes**

- Used for quota enforcement dashboards in Recon.
- Quota types may represent **space** (bytes) or **namespace count**, depending on context.
- If `used ≥ allowed`, the path has exceeded its configured limit.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “What’s the quota for bucket1?” | `allowed` |
| “How much of the quota is used?” | `used` |
| “Is this volume near its limit?” | Compare `used` vs `allowed` |

---

## **Schema: MetadataSpaceDist**

**Purpose:**

Represents a **histogram of space distribution** across namespace elements (e.g., directories, keys).

Returned by `/namespace/spaceDist` or integrated into Recon UI visualizations.

---

### **Fields**

| Field | Type | Description | Example |
| --- | --- | --- | --- |
| **`status`** | `string` | Operation result. | `"OK"` |
| **`dist[]`** | `array(integer)` | Ordered list of space usage buckets, typically representing ranges (e.g., key size distribution). | `[0, 0, 10, 20, 0, 30, 0, 100, 40]` |

---

### **Example**

```json
{
  "status": "OK",
  "dist": [0, 0, 10, 20, 0, 30, 0, 100, 40]
}

```

---

### **Usage Notes**

- Used for plotting **key size histograms** or **space distribution graphs**.
- Each position in `dist` corresponds to a size bucket (e.g., 0–1 KB, 1–10 KB, 10–100 KB, etc.).
- Helps visualize data skew across directories or buckets.
- Commonly paired with `MetadataDiskUsage` for per-bucket dashboards.

---

### **Natural-Language Mappings**

| Query | Field |
| --- | --- |
| “Show size distribution of objects under bucket1.” | `dist[]` |
| “Which buckets contribute most to storage usage?” | Analyze non-zero indices of `dist[]` |
| “Plot the histogram of key sizes.” | Use `dist[]` values as y-axis counts |

---

## **Routing Guide (Cross-Schema)**

| Intent | Schema | Key Fields |
| --- | --- | --- |
| Count objects at any namespace level | `NamespaceMetadataResponse` | `numVolume`, `numBucket`, `numDir`, `numKey` |
| Check space used vs replicated | `MetadataDiskUsage` | `size`, `sizeWithReplica` |
| List per-directory usage breakdown | `MetadataDiskUsage.subPaths[]` | `path`, `size` |
| Inspect quota limits | `MetadataQuota` | `allowed`, `used` |
| Visualize space distribution | `MetadataSpaceDist` | `dist[]` |

---

This documentation now covers **every property and nested element** across the four metadata schemas, with clear field definitions, examples, usage context, and natural-language query mappings.


All fields are expanded, typed, and semantically linked so the model can map user intent to exact parameters and metrics.

---

## **Schema: StorageReport**

**Purpose:**

Represents per-node storage metrics summarizing total capacity, used space, and utilization types (Ozone vs non-Ozone).

Used inside multiple APIs such as `/clusterState`, `/datanodes`, and `/pipelines`.

**Fields**

- **capacity** *(number)* – Total raw disk capacity on the DataNode in bytes.

  *Example:* `270429917184`

- **used** *(number)* – Total space used by Ozone data blocks.

  *Example:* `358805504`

- **remaining** *(number)* – Free space available for new data.

  *Example:* `270071111680`

- **committed** *(number)* – Space already reserved for in-flight writes but not yet finalized.

  *Example:* `27007111`

- **nonOzoneUsed** *(number)* – Space used by files not managed by Ozone (HDFS, system logs, or local data).

  Useful for queries about **"non-ozone used space"**.

  *Example:* `150000000`


**Usage Notes**

- Aggregated across all DataNodes to compute total cluster utilization.
- Helps detect imbalance or external data occupying Ozone disks.
- Commonly nested in `ClusterState` or `DatanodesSummary`.

**Typical Question Mappings**

- “How much total storage is available in the cluster?” → `capacity`
- “What portion of space is used by non-Ozone data?” → `nonOzoneUsed`
- “Show remaining vs committed space per DataNode.” → `remaining`, `committed`

---

## **Schema: ClusterState**

**Purpose:**

Global summary of cluster health and topology.

Returned by `/clusterState` endpoint.

**Fields**

- **deletedDirs** *(integer)* – Number of directories deleted by background services.
- **missingContainers** *(integer)* – Containers reported missing by SCM.
- **openContainers** *(integer)* – Containers currently writable.
- **deletedContainers** *(integer)* – Containers fully deleted.
- **keysPendingDeletion** *(integer)* – Keys marked for deletion but not yet removed.
- **scmServiceId** *(string)* – SCM service identifier.
- **omServiceId** *(string)* – OM service identifier.
- **pipelines** *(integer)* – Active replication pipelines.

  *Example:* `5`

- **totalDatanodes** *(integer)* – Total number of registered DataNodes.

  *Example:* `4`

- **healthyDatanodes** *(integer)* – Count of currently healthy DataNodes.

  *Example:* `4`

- **storageReport** *(StorageReport)* – Aggregated cluster-wide storage metrics.
- **containers** *(integer)* – Total containers in SCM metadata.

  *Example:* `26`

- **volumes** *(integer)* – Total Ozone volumes.

  *Example:* `6`

- **buckets** *(integer)* – Total buckets across all volumes.

  *Example:* `26`

- **keys** *(integer)* – Total key objects stored.

  *Example:* `25`


**Usage Notes**

- Used by Recon dashboard to represent **cluster overview** (health, capacity, object counts).
- Combines logical object metadata with physical DataNode metrics.
- `missingContainers` and `keysPendingDeletion` help identify cleanup or replication backlog.

**Typical Questions**

- “How many healthy DataNodes are in the cluster?” → `healthyDatanodes`
- “What’s the total container count?” → `containers`
- “Show the current non-ozone usage.” → `storageReport.nonOzoneUsed`

---

## **Schema: DatanodesSummary**

**Purpose:**

Lists all DataNodes along with build, health, and storage information.

Returned by `/datanodes` endpoint.

**Fields**

- **totalCount** *(integer)* – Number of DataNodes in the response.

  *Example:* `4`

- **datanodes[]** *(array)* – Detailed per-node metadata.

Each **datanode object** includes:

- **buildDate** *(string)* – Software build timestamp.
- **layoutVersion** *(integer)* – On-disk layout version.
- **networkLocation** *(string)* – Rack or topology location.
- **opState** *(string)* – Operational state (e.g., `IN_SERVICE`).
- **revision** *(string)* – Code revision identifier.
- **setupTime** *(integer)* – Epoch time when the node was initialized.
- **version** *(string)* – Software version.
- **uuid** *(string)* – Unique identifier for the DataNode.

  *Example:* `"f8f8cb45-3ab2-4123"`

- **hostname** *(string)* – Hostname of the DataNode.

  *Example:* `"localhost-1"`

- **state** *(string)* – Health state (`HEALTHY`, `STALE`, etc.).
- **lastHeartbeat** *(number)* – Timestamp of the latest heartbeat.

  *Example:* `1605738400544`

- **storageReport** *(StorageReport)* – Node-specific storage usage.
- **pipelines[]** *(array)* – Pipelines this node participates in, each containing:
  - **pipelineID** *(string)*
  - **replicationType** *(string)* – e.g., `RATIS`, `STAND_ALONE`.
  - **replicationFactor** *(integer)* – Expected replicas (e.g., 3).
  - **leaderNode** *(string)* – Hostname of pipeline leader.

    *Example:*


    ```json
    [
      { "pipelineID": "b9415b20-b9bd-4225", "replicationType": "RATIS", "replicationFactor": 3, "leaderNode": "localhost-2" },
      { "pipelineID": "3bf4a9e9-69cc-4d20", "replicationType": "RATIS", "replicationFactor": 1, "leaderNode": "localhost-1" }
    ]
    
    ```

- **containers** *(integer)* – Containers hosted on this DataNode.
- **leaderCount** *(integer)* – Number of pipelines where this node acts as leader.

**Usage Notes**

- Used for **per-node diagnostics**, capacity distribution, and leadership visualization.
- `lastHeartbeat` helps detect stale or dead nodes.
- `leaderCount` indicates how much write traffic a node handles.

**Typical Questions**

- “List all DataNodes and their health.” → `datanodes[].state`
- “Show which node is leading the most pipelines.” → `leaderCount`
- “How much space is used on localhost-1?” → `storageReport.used`

---

## **Schema: RemovedDatanodesResponse**

**Purpose:**

Reports DataNodes that were removed or decommissioned from the cluster.

Returned by `/datanodes/remove`.

**Fields**

- **datanodesResponseMap.removedDatanodes.totalCount** *(integer)* – Number of removed nodes.
- **datanodesResponseMap.removedDatanodes.datanodes[]** *(array)* – List of removed DataNode entries.

Each **removed datanode** includes:

- **uuid** *(string)* – Node identifier.
- **hostname** *(string)* – Hostname of the removed node.
- **state** *(string)* – State before removal (`DECOMMISSIONED`, `DEAD`).
- **pipelines** *(string, nullable)* – Pipelines last associated with this node (optional).

**Usage Notes**

- Helps trace removed nodes and ensure decommission completion.
- Used to audit SCM node removal actions.

**Typical Questions**

- “Which DataNodes were recently removed?” → `removedDatanodes.datanodes[].hostname`
- “How many nodes were decommissioned?” → `totalCount`

---

## **Schema: DatanodesDecommissionInfo**

**Purpose:**

Details current decommissioning progress for each DataNode.

Returned by `/datanodes/decommission/info`.

**Fields**

- **DatanodesDecommissionInfo[]** *(array)* – List of decommission status objects.

Each **decommission object** contains:

- **containers** *(object)* – Placeholder for container list/details being processed.
- **metrics** *(object, nullable)* – Contains numeric progress indicators:
  - **decommissionStartTime** *(string)* – Timestamp when decommission began.
  - **numOfUnclosedContainers** *(integer)* – Containers not yet closed.
  - **numOfUnclosedPipelines** *(integer)* – Pipelines still active.
  - **numOfUnderReplicatedContainers** *(integer)* – Containers awaiting replication.
- **datanodeDetails** *(DatanodeDetails)* – Metadata for the node being decommissioned.

**Usage Notes**

- Used by admins to monitor **decommission progress** and identify blockers.
- `numOfUnclosedContainers` or `numOfUnderReplicatedContainers` > 0 indicates delay.
- Paired with `RemovedDatanodesResponse` to validate completion.

**Typical Questions**

- “Which nodes are being decommissioned?” → `datanodeDetails.hostname`
- “How many unclosed containers remain?” → `metrics.numOfUnclosedContainers`
- “When did the decommission start?” → `metrics.decommissionStartTime`

---

## **Schema: ByteString**

**Purpose:**

Represents dual string and raw byte data in protocol buffers or internal metadata objects.

Used internally for data encoding and transmission validation.

**Fields**

- **string** *(string)* – Human-readable string representation.
- **bytes** *(object)* – Raw byte information:
  - **validUtf8** *(boolean)* – Indicates whether bytes can be safely decoded as UTF-8.
  - **empty** *(boolean)* – True if the byte array is empty.

**Usage Notes**

- Primarily internal; not used in most Recon user APIs.
- Enables serialization/deserialization consistency for byte-encoded IDs or paths.

**Typical Questions**

- “Is this byte data UTF-8 valid?” → `bytes.validUtf8`
- “Is this string field empty?” → `bytes.empty`

---

### **Routing Guide (Summary)**

- For cluster-level queries → use **ClusterState**.
- For node-level health and capacity → use **DatanodesSummary**.
- For removed or decommissioning nodes → use **RemovedDatanodesResponse** or **DatanodesDecommissionInfo**.
- For raw capacity metrics → use **StorageReport** (nested in multiple schemas).
- For encoding checks → use **ByteString**.

This structure provides both semantic understanding (purpose, usage, relationships) and low-level grounding (exact field names and examples).


`DatanodeDetails` schema. Every parameter is included and concisely explained so the model can interpret, map, and reason over it without ambiguity.

---

## **Schema: DatanodeDetails**

**Purpose:**

Describes full metadata and network topology details of a single Ozone **DataNode**.

Used in APIs like `/datanodes`, `/datanodes/decommission/info`, and internal cluster diagnostics.

---

### **Fields**

- **level** *(integer)* — Hierarchical level of the node within network topology (e.g., rack depth).
- **parent** *(string, nullable)* — Parent node or rack name in the topology tree; null if top-level.
- **cost** *(integer)* — Network or topology cost metric used for replica placement distance.
- **uuid** *(string)* — Unique node identifier (short form).
- **uuidString** *(string)* — Same UUID as string format for serialization consistency.
- **ipAddress** *(string)* — IP address of the DataNode.
- **hostName** *(string)* — Hostname of the DataNode.
- **ports[]** *(array)* — List of named service ports exposed by this node.
  - **name** *(string)* — Port label (e.g., `RATIS`, `STANDALONE`, `HTTP`).
  - **value** *(integer)* — Numeric port value.
- **certSerialId** *(integer)* — Certificate serial ID used for TLS authentication.
- **version** *(string, nullable)* — Software version currently running.
- **setupTime** *(string)* — Timestamp when the node was initialized and registered.
- **revision** *(string, nullable)* — Source control revision hash for the running build.
- **buildDate** *(string, nullable)* — Build timestamp of the deployed binary.
- **persistedOpState** *(string)* — Last persisted operational state (`IN_SERVICE`, `DECOMMISSIONING`, etc.).
- **persistedOpStateExpiryEpochSec** *(integer)* — Expiry time (epoch seconds) of the persisted op-state, if temporary.
- **initialVersion** *(integer)* — Disk layout version at initial startup.
- **currentVersion** *(integer)* — Current layout version after upgrades.
- **decommissioned** *(boolean)* — True if the DataNode has been fully decommissioned.
- **maintenance** *(boolean)* — True if the node is currently under maintenance mode.
- **ipAddressAsByteString** *(ByteString)* — Byte representation of the node’s IP (used internally for serialization).
- **hostNameAsByteString** *(ByteString)* — Byte representation of the hostname.
- **networkName** *(string)* — Short name of the network/rack segment this node belongs to.
- **networkLocation** *(string)* — Rack or topology location string (e.g., `/default-rack`).
- **networkFullPath** *(string)* — Full hierarchical path from root to node within topology (e.g., `/root/region1/rackA/dn1`).
- **numOfLeaves** *(integer)* — Count of leaf nodes under this network path (used for rack balancing).
- **networkNameAsByteString** *(ByteString)* — Byte-encoded form of `networkName`.
- **networkLocationAsByteString** *(ByteString)* — Byte-encoded form of `networkLocation`.

---

### **Example**

```json
{
  "level": 3,
  "parent": "rackA",
  "cost": 10,
  "uuid": "f8f8cb45-3ab2-4123",
  "uuidString": "f8f8cb45-3ab2-4123",
  "ipAddress": "10.0.0.5",
  "hostName": "localhost-1",
  "ports": [
    { "name": "RATIS", "value": 9872 },
    { "name": "HTTP", "value": 9882 }
  ],
  "certSerialId": 12345,
  "version": "1.3.0",
  "setupTime": "1605738400544",
  "revision": "abcd123",
  "buildDate": "2024-09-20",
  "persistedOpState": "IN_SERVICE",
  "persistedOpStateExpiryEpochSec": 1700000000,
  "initialVersion": 1,
  "currentVersion": 2,
  "decommissioned": false,
  "maintenance": false,
  "ipAddressAsByteString": { "string": "10.0.0.5" },
  "hostNameAsByteString": { "string": "localhost-1" },
  "networkName": "rackA",
  "networkLocation": "/default-rack",
  "networkFullPath": "/root/region1/rackA/dn1",
  "numOfLeaves": 1,
  "networkNameAsByteString": { "string": "rackA" },
  "networkLocationAsByteString": { "string": "/default-rack" }
}

```

---

### **Usage Notes**

- Used heavily for **replica placement**, **decommission tracking**, and **rack awareness visualization**.
- `cost` and `level` help Ozone compute network distance for data placement.
- `persistedOpState` and `decommissioned` reveal the node’s current administrative role.
- `networkFullPath` and `numOfLeaves` are useful for topology map generation in Recon.
- The various `AsByteString` fields exist for consistent protobuf serialization but can usually be ignored in user queries.

---

### **Natural-Language Query Mappings**

| Example Query | Map To |
| --- | --- |
| “Where is DataNode dn1 located in the network?” | `networkLocation`, `networkFullPath` |
| “What is the IP and port for DataNode localhost-1?” | `ipAddress`, `ports[]` |
| “Is this node under maintenance or decommissioned?” | `maintenance`, `decommissioned` |
| “What is the DataNode’s operational state?” | `persistedOpState` |
| “Which rack is this node part of?” | `networkName`, `parent` |
| “When was this DataNode registered?” | `setupTime` |
| “What version and build revision is it running?” | `version`, `revision`, `buildDate` |

---

### **Routing Guide**

- Use `DatanodeDetails` whenever queries involve **specific node identity**, **network placement**, or **state management**.
- Prefer textual fields (`ipAddress`, `hostName`, `networkLocation`) for user-facing responses; the `ByteString` variants exist only for internal matching.
- Combine with `DatanodesDecommissionInfo` when user asks “Which nodes are decommissioning?” or “Show detailed info for node X.”

---

This version includes every field, nested object, and its purpose — with short, clear summaries for structured retrieval and reasoning.


`PipelinesSummary` schema — fully expanded, with every parameter explained concisely and consistently with your `DatanodeDetails` format.

---

## **Schema: PipelinesSummary**

**Purpose:**

Represents the state, configuration, and participants of all active **replication pipelines** in the Ozone cluster.

Used by the `/pipelines` endpoint to show per-pipeline metrics and leadership details.

Each pipeline defines a logical replication channel between multiple DataNodes.

---

### **Fields**

- **totalCount** *(integer)* — Total number of pipelines currently tracked by Recon.

  Indicates how many replication groups exist across the cluster.

  *Example:* `5`

- **pipelines[]** *(array)* — List containing detailed information about each pipeline.

  Each pipeline object describes its ID, replication settings, participating nodes, and health indicators.


---

### **Pipeline Object Fields**

Each element within `pipelines[]` includes:

- **pipelineId** *(string)* — Unique identifier (UUID) for the pipeline.

  Used to correlate container assignments and node participation.

  *Example:* `"b9415b20-b9bd-4225"`

- **status** *(string)* — Current operational state of the pipeline (`OPEN`, `CLOSED`, or `ALLOCATING_CONTAINERS`).

  *Example:* `"OPEN"`

- **leaderNode** *(string)* — Hostname of the node currently acting as the **leader** for this pipeline.

  Responsible for coordination and consensus during writes.

  *Example:* `"localhost-1"`

- **datanodes[]** *(array of DatanodeDetails)* —

  Full details of the DataNodes that form this pipeline, including their IP, network location, and operational state.

  Each entry follows the **DatanodeDetails** schema.

- **lastLeaderElection** *(integer)* — Epoch timestamp (in milliseconds) when the last leader election occurred.

  Zero indicates no election since creation.

  *Example:* `0`

- **duration** *(number)* — Total lifetime of the pipeline in milliseconds since creation.

  Helps identify short-lived or unstable pipelines.

  *Example:* `23166128`

- **leaderElections** *(integer)* — Number of leader election events that have occurred for this pipeline.

  Frequent elections may signal instability or node churn.

  *Example:* `0`

- **replicationType** *(string)* — Mechanism used for replication (`RATIS` or `STAND_ALONE`).

  Determines how data blocks are replicated and acknowledged.

  *Example:* `"RATIS"`

- **replicationFactor** *(integer)* — Expected number of replicas participating in the pipeline (e.g., `1`, `3`).

  Matches the replication policy of containers assigned to this pipeline.

  *Example:* `3`

- **containers** *(integer)* — Number of containers currently hosted within this pipeline.

  Indicates how many storage units rely on this replication channel.

  *Example:* `3`


---

### **Example**

```json
{
  "totalCount": 5,
  "pipelines": [
    {
      "pipelineId": "b9415b20-b9bd-4225",
      "status": "OPEN",
      "leaderNode": "localhost-1",
      "datanodes": [
        {
          "uuid": "f8f8cb45-3ab2-4123",
          "hostName": "localhost-1",
          "ipAddress": "10.0.0.5",
          "networkLocation": "/rackA",
          "state": "HEALTHY"
        },
        {
          "uuid": "a9b7d19e-4a77-88f9",
          "hostName": "localhost-2",
          "ipAddress": "10.0.0.6",
          "networkLocation": "/rackA",
          "state": "HEALTHY"
        },
        {
          "uuid": "cd3e21aa-0e45-42ff",
          "hostName": "localhost-3",
          "ipAddress": "10.0.0.7",
          "networkLocation": "/rackB",
          "state": "HEALTHY"
        }
      ],
      "lastLeaderElection": 0,
      "duration": 23166128,
      "leaderElections": 0,
      "replicationType": "RATIS",
      "replicationFactor": 3,
      "containers": 3
    }
  ]
}

```

---

### **Usage Notes**

- A **pipeline** groups DataNodes used for block replication and I/O coordination.
- The **leaderNode** handles write ordering and Raft consensus for RATIS pipelines.
- **duration** and **leaderElections** help identify unstable pipelines that frequently reform.
- **containers** quantifies how much data traffic flows through each pipeline.
- When combined with `DatanodesSummary`, Recon can show pipeline-to-node relationships and leadership distribution.

---

### **Natural-Language Query Mappings**

| Example Query | Maps To |
| --- | --- |
| “List all pipelines in the cluster.” | `pipelines[]` |
| “Show the leader node of each pipeline.” | `leaderNode` |
| “How many pipelines are open?” | `status` |
| “Which pipelines use RATIS replication?” | `replicationType` |
| “What is the replication factor for pipeline b9415b20?” | `replicationFactor` |
| “Show how long each pipeline has been running.” | `duration` |
| “Which pipelines have undergone leader elections?” | `leaderElections`, `lastLeaderElection` |
| “How many containers are assigned per pipeline?” | `containers` |
| “List DataNodes participating in pipeline X.” | `datanodes[]` |

---

### **Routing Guide**

- Use `PipelinesSummary` for all user intents involving **replication groups**, **leaders**, or **container-to-pipeline mappings**.
- When a query includes keywords like “RATIS,” “pipeline,” “replica,” “leader,” or “container group,” this schema is most relevant.
- Combine with `DatanodesSummary` for topology-aware explanations (e.g., “Which rack hosts all nodes of this pipeline?”).
- If `status = CLOSED`, the pipeline should be excluded from write path discussions.

---

## **Schema: TasksStatus**

**Purpose:**

Represents the **latest execution state and progress** of background Recon tasks (such as OM Delta sync, Missing Container scans, or Key Mapping tasks).

Returned by the `/task/status` endpoint to monitor task freshness, completion order, and synchronization cycles.

---

### **Fields**

Each entry in the array corresponds to one background task being tracked by Recon.

- **taskName** *(string)* — Name of the background task or service module reporting status.

  Identifies which component of Recon (e.g., `OmDeltaRequest`, `ContainerKeyMapper`, `FileSizeCountTaskFSO`, etc.) last updated its internal checkpoint.

  *Example:* `"OmDeltaRequest"`

- **lastUpdatedTimestamp** *(number)* — Epoch timestamp (in milliseconds) when this task last successfully ran or synchronized data.

  Used to detect staleness or verify that a task is running on schedule.

  *Example:* `1605724099147`

- **lastUpdatedSeqNumber** *(number)* — Last sequence number or transaction checkpoint processed by the task.

  Indicates how far Recon has ingested data (e.g., OM transaction sequence).

  Higher numbers represent newer sync progress.

  *Example:* `186`


---

### **Example**

```json
[
  {
    "taskName": "OmDeltaRequest",
    "lastUpdatedTimestamp": 1605724099147,
    "lastUpdatedSeqNumber": 186
  },
  {
    "taskName": "OmDeltaRequest",
    "lastUpdatedTimestamp": 1605724103892,
    "lastUpdatedSeqNumber": 188
  }
]

```

---

### **Usage Notes**

- Used by the **Recon Tasks Dashboard** to show when each background service last completed execution.
- Critical for **monitoring data freshness** between Ozone Manager, SCM, and Recon DBs.
- A growing gap between `lastUpdatedSeqNumber` and OM transaction IDs indicates **sync lag**.
- `lastUpdatedTimestamp` allows for quick checks of **task health and scheduling cadence**.
- Useful for diagnosing why Recon data (e.g., container states, key counts) appears outdated.

---

### **Natural-Language Query Mappings**

| Example Query | Maps To |
| --- | --- |
| “When did Recon last sync with OM?” | `lastUpdatedTimestamp` where `taskName = OmDeltaRequest` |
| “Which tasks have not updated recently?” | Compare `lastUpdatedTimestamp` values |
| “What is the current sequence number for OM delta sync?” | `lastUpdatedSeqNumber` |
| “Is Recon lagging behind OM updates?” | Evaluate difference between `lastUpdatedSeqNumber` and OM’s known latest sequence |
| “List all background tasks and their update times.” | Iterate over all `taskName` entries |

---

### **Routing Guide**

- Use this schema when queries involve **Recon sync progress**, **task freshness**, or **lag detection**.
- Keywords like *“last updated,” “task progress,” “delta sync,” “background service,”* or *“status of tasks”* map directly here.
- If timestamps differ greatly across tasks, suggest Recon restart or deeper inspection of lag sources.
- When multiple tasks share the same name but have different timestamps, report the most recent update as the **active instance**.

---

---

## Module: Keys (Advanced Listing)

### **Endpoint:** `/keys/listKeys`

**Intent Keywords:**
list keys, list files, browse bucket, filter keys, large keys, ratis keys, ec keys, keys by date, keys by size, keys under prefix, paginate keys

**Purpose:**
Return committed keys and files under a bucket-scoped prefix with optional filters on replication type, creation date, and minimum size. Supports pagination across large buckets (OBS, LEGACY, and FSO layouts).

Use this endpoint when the user wants to **enumerate or filter stored keys**, not open/in-progress writes (use `/keys/open` for those).

**Method:** `GET`

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **`startPrefix`** | `string` | **Yes (effective)** | `/` | Path prefix to search under. **Must be bucket level or deeper** — at minimum `/<volume>/<bucket>`. Examples: `/volume1/fso-bucket`, `/volume1/obs-bucket/dir1/`. Returns HTTP 400 if missing, empty, or not at least volume+bucket depth. |
| **`replicationType`** | `string` | No | (none) | Filter by replication backend. Common values: `RATIS`, `EC`. Omit to include all replication types. |
| **`creationDate`** | `string` | No | (none) | Return keys created **on or after** this timestamp. Format: `MM-dd-yyyy HH:mm:ss` (server local timezone). Example: `02-10-2026 00:00:00`. |
| **`keySize`** | `integer` | No | `0` | Minimum logical size in bytes. Only keys with `size >= keySize` are returned. Example: `1073741824` for 1 GB. |
| **`prevKey`** | `string` | No | `""` | Pagination cursor. Set to the `lastKey` value from the previous response to fetch the next page. |
| **`limit`** | `integer` | No | `1000` | Maximum number of keys to return in one response. |

**Important constraints for the chatbot:**
- **Always** set `startPrefix` to at least `/<volume>/<bucket>`. Never use `/` alone — that scans the entire cluster and is blocked by Recon chatbot safety policy.
- If the user names a volume and bucket, construct `startPrefix=/volumeName/bucketName`.
- If the user names a directory under a bucket, include it: `/volume1/fso-bucket/dir1/`.
- Use `/keys/open` instead when the question is about **open/uncommitted** files.
- Use `/keys/open/summary` for aggregate open-key statistics without listing individual files.

**Response schema:** `ListKeysResponse` (see below)

**HTTP status notes:**
- `200` — Keys found and returned.
- `204` / empty match — No keys matched the filters under the prefix.
- `400` — Invalid `startPrefix` (not bucket-scoped).
- `503` — Recon OM metadata still initializing (`status: INITIALIZING`).

**Example Queries:**
- "List keys under /volume1/fso-bucket."
- "Show RATIS keys larger than 1 GB in bucket obs-bucket on volume vol1."
- "Find EC keys in /vol1/bucket2 created after 02-10-2026 00:00:00."
- "List the next page of keys in /volume1/obs-bucket using pagination."

**Example Requests:**
- `/api/v1/keys/listKeys?startPrefix=/volume1/fso-bucket&limit=100`
- `/api/v1/keys/listKeys?startPrefix=/volume1/fso-bucket&limit=100&replicationType=RATIS&keySize=1048576`
- `/api/v1/keys/listKeys?startPrefix=/volume1/obs-bucket&prevKey=/volume1/obs-bucket/key6&limit=100`

**Related Endpoints:**
- `/keys/open` — keys currently open (not yet committed)
- `/keys/open/summary` — aggregate stats for open keys
- `/keys/deletePending` — keys marked for deletion
- `/keys/deletePending/summary` — deletion summary counts
- `/namespace/summary` — namespace counts without listing individual keys

---

## **Schema: ListKeysResponse**

**Purpose:**

Paginated listing of committed keys/files under a prefix, with optional filters applied. Returned by `/keys/listKeys`.

Unlike `/keys/open`, this endpoint searches **committed** keys across LEGACY, FSO, and OBS bucket layouts.

---

### **Top-Level Fields**

- **`status`** *(string)* — Result status. Common values: `OK`, `INITIALIZING`.

  *Example:* `"OK"`

- **`path`** *(string)* — Echo of the requested `startPrefix`.

  *Example:* `"/volume1/fso-bucket"`

- **`replicatedDataSize`** *(integer)* — Sum of `replicatedSize` for all keys in this response page (bytes after replication).

  *Example:* `188743680`

- **`unReplicatedDataSize`** *(integer)* — Sum of logical `size` for all keys in this response page.

  *Example:* `62914560`

- **`lastKey`** *(string)* — Internal key identifier of the last entry in `keys[]`. Pass this value as `prevKey` to fetch the next page.

  *Example:* `"/volume1/obs-bucket/key6"`

- **`keys[]`** *(array)* — List of matching key/file entries (see below).

---

### **`keys[]` Entry Fields**

Each element describes one key or directory prefix:

- **`key`** *(string)* — Internal RocksDB/table key used for pagination. FSO buckets may show numeric object IDs here; use `path` for human-readable names.

  *Example:* `"/volume1/obs-bucket/key1"`

- **`path`** *(string)* — Human-readable path relative to the Ozone namespace (`volume/bucket/...`).

  *Example:* `"volume1/fso-bucket/dir1/file1"`

- **`size`** *(integer)* — Logical data size in bytes (before replication).

  *Example:* `10485760`

- **`replicatedSize`** *(integer)* — Physical size after applying the replication factor.

  *Example:* `31457280`

- **`replicationInfo`** *(object)* — Replication configuration:
  - **`replicationType`** *(string)* — `RATIS` or `EC`.
  - **`replicationFactor`** *(string)* — e.g. `ONE`, `THREE`.
  - **`requiredNodes`** *(integer)* — Number of replicas/nodes required.

- **`creationTime`** *(integer)* — Epoch milliseconds when the key was created.

- **`modificationTime`** *(integer)* — Epoch milliseconds when the key was last modified.

- **`isKey`** *(boolean)* — `true` for a file, `false` for a directory entry.

---

### **Example Response**

```json
{
  "status": "OK",
  "path": "/volume1/obs-bucket",
  "replicatedDataSize": 62914560,
  "unReplicatedDataSize": 62914560,
  "lastKey": "/volume1/obs-bucket/key6",
  "keys": [
    {
      "key": "/volume1/obs-bucket/key1",
      "path": "volume1/obs-bucket/key1",
      "size": 10485760,
      "replicatedSize": 10485760,
      "replicationInfo": {
        "replicationFactor": "ONE",
        "requiredNodes": 1,
        "replicationType": "RATIS"
      },
      "creationTime": 1715781418742,
      "modificationTime": 1715781419762,
      "isKey": true
    }
  ]
}
```

---

### **Usage Notes**

- Combines results from LEGACY and FSO key tables; OBS keys appear under non-FSO paths.
- Filters (`replicationType`, `creationDate`, `keySize`) are applied while scanning; omit them to list all keys under the prefix.
- For large buckets, keep `limit` modest (e.g. 100–200) and paginate with `prevKey`/`lastKey`.
- `creationDate` filter is inclusive of keys created at or after the parsed timestamp.

---

### **Natural-Language Mappings**

| Query | Parameter / Field |
| --- | --- |
| "List keys in bucket X on volume Y" | `startPrefix=/Y/X` |
| "Show only RATIS keys" | `replicationType=RATIS` |
| "Keys larger than 1 GB" | `keySize=1073741824` |
| "Keys created after Feb 10 2026" | `creationDate=02-10-2026 00:00:00` |
| "Next page of results" | `prevKey=<lastKey from prior response>` |
| "How much data do these keys use?" | `replicatedDataSize`, `unReplicatedDataSize` |
| "Is this a file or directory?" | `isKey` |

---

### **Routing Guide**

- Choose `/keys/listKeys` when the user asks to **list, browse, search, or filter committed keys/files** in a bucket or subdirectory.
- **Require** a bucket-scoped `startPrefix` before calling. If the user only names a volume, ask which bucket to scope to.
- Do **not** use `/keys/listKeys` for open/in-progress uploads — route those to `/keys/open`.
- Do **not** use `/keys/listKeys` for deletion candidates — route those to `/keys/deletePending`.
- When combining filters, include every filter the user mentioned (`replicationType`, `keySize`, `creationDate`).
- If the response includes `lastKey` and the user wants more results, suggest pagination with `prevKey`.
- If `status` is `INITIALIZING`, tell the user Recon is still syncing OM metadata and to retry shortly.

---

## **Schema: FileSizeUtilization**

**Purpose:**

Represents the **distribution of files across volumes and buckets by size category and count**.

Returned by the `/utilization/filesize` endpoint in Recon to show how many files exist of specific sizes within each bucket.

Used for analyzing **storage utilization trends**, **file size skew**, and **capacity consumption patterns**.

---

### **Fields**

Each entry in the array describes a unique combination of volume, bucket, and file-size grouping.

- **volume** *(string)* — Name of the Ozone **volume** containing the files.

  Identifies the logical namespace root under which the bucket resides.

  *Example:* `"vol-2-04168"`

- **bucket** *(string)* — Name of the **bucket** under the specified volume.

  Represents the immediate container grouping for files (keys) of this size class.

  *Example:* `"bucket-0-11685"`

- **fileSize** *(number)* — Size (in bytes) of the file or file group represented by this record.

  Each record aggregates all files of the same size under a given volume/bucket pair.

  *Example:* `1024`

- **count** *(integer)* — Number of files (keys) found with the exact or approximate file size defined in `fileSize`.

  Indicates how many files contribute to that utilization point.

  *Example:* `1`


---

### **Example**

```json
[
  { "volume": "vol-2-04168", "bucket": "bucket-0-11685", "fileSize": 1024, "count": 1 },
  { "volume": "vol-2-04168", "bucket": "bucket-1-41795", "fileSize": 1024, "count": 1 },
  { "volume": "vol-2-04168", "bucket": "bucket-2-93377", "fileSize": 1024, "count": 1 },
  { "volume": "vol-2-04168", "bucket": "bucket-3-50336", "fileSize": 1024, "count": 2 }
]

```

---

### **Usage Notes**

- Used in Recon to **quantify data distribution** by file size across volumes and buckets.
- Each record represents aggregated counts of files with identical or rounded sizes.
- Helps identify **hot buckets** (those with many small files) or **storage inefficiency** (many tiny keys inflating metadata).
- Supports **capacity planning** by correlating `fileSize × count` for total storage consumption per bucket.
- May be combined with `MetadataDiskUsage` or `MetadataSpaceDist` for richer cluster utilization analytics.

---

### **Interpretation Example**

If the dataset shows many entries with `fileSize = 1024` and high `count` values across multiple buckets,

it implies heavy use of small files — common in workloads with metadata-intensive operations or frequent small writes.

---

### **Natural-Language Query Mappings**

| Example Query | Maps To |
| --- | --- |
| “Show how many files exist per bucket by size.” | `volume`, `bucket`, `fileSize`, `count` |
| “Which buckets have the most small files?” | Filter where `fileSize` < threshold, sort by `count` |
| “What is the total number of 1 KB files?” | Aggregate all entries where `fileSize = 1024`, sum `count` |
| “List buckets under vol-2-04168 with large files.” | Filter by `volume`, sort by descending `fileSize` |
| “How is file size distributed across volumes?” | Group by `volume`, aggregate `fileSize × count` |

---

### **Routing Guide**

- Use this schema for **data size analytics**, **file count summaries**, and **storage optimization queries**.
- When the query includes phrases like *“file size utilization,” “file count by bucket,” “how many small files,”* or *“storage distribution,”* this schema applies.
- For broader space usage (including replicas), correlate with `MetadataDiskUsage`.
- If user asks for totals or averages, aggregate across `count` and `fileSize` fields.
- If `fileSize` appears constant across many buckets, highlight uneven data spread as a cluster optimization insight.

---

## **Schema: ContainerUtilization**

**Purpose:**

Represents the **distribution of container sizes and counts** across the Ozone cluster.

Returned by the `/utilization/containers` endpoint in Recon.

Used to analyze **how many containers exist at specific size levels**, identify imbalance, and assist in capacity planning.

---

### **Fields**

Each record in the array corresponds to one container size category and the number of containers that fall into it.

- **containerSize** *(number)* — The size (in bytes) of containers within this utilization group.

  Reflects total data stored in each container class.

  Often reported as powers of two (e.g., 1 GB, 2 GB).

  *Example:* `2147483648`

- **count** *(number)* — Number of containers that have the specified `containerSize`.

  Indicates the frequency or volume distribution of containers by size.

  *Example:* `9`


---

### **Example**

```json
[
  { "containerSize": 2147483648, "count": 9 },
  { "containerSize": 1073741824, "count": 3 }
]

```

---

### **Usage Notes**

- Used to **analyze space allocation patterns** among Ozone containers.
- Helps detect uneven data distribution across pipelines or DataNodes.
- A large number of smaller containers can imply fragmented writes or high namespace churn.
- Larger container groups indicate bulk or aggregated data usage patterns.
- Useful for **capacity diagnostics**, **container balancing**, and **replication efficiency monitoring** in Recon dashboards.

---

### **Interpretation Example**

If `containerSize = 2 GB` has a higher count than `1 GB`, the cluster stores most data in full-sized containers.

If smaller containers dominate, it may indicate premature container closures or frequent small writes.

---

### **Natural-Language Query Mappings**

| Example Query | Maps To |
| --- | --- |
| “How many containers are 2 GB in size?” | Filter where `containerSize = 2147483648`, read `count` |
| “List all container sizes and their counts.” | Iterate through `containerSize` and `count` |
| “What is the most common container size in the cluster?” | Highest `count` value |
| “Show container size distribution.” | Aggregate full array of `containerSize` vs `count` |
| “Are most containers small or large?” | Compare counts between lower and higher size ranges |

---

### **Routing Guide**

- Use `ContainerUtilization` when queries mention *“container size,” “container distribution,” “storage utilization per container,”* or *“how many containers of size X.”*
- When user queries require percentage or trend analysis, compute relative proportions of `count` for each `containerSize`.
- For total capacity estimation, multiply `containerSize × count` and sum across entries.
- Integrate with `FileSizeUtilization` for combined container-to-file size analytics.
- If no containers are listed, infer that Recon’s container scan hasn’t completed or that all containers are currently empty.

---

