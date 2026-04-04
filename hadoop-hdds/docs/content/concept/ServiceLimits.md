---
title: Service Limits
description: An overview of the scalability and resource limits within Apache Ozone.
---

This document provides a compact overview of the scalability and resource limits within Apache Ozone. It is divided into two main sections: **Configured Limits**, which can be tuned via properties in `ozone-site.xml`, and **Architectural Limits**, which are inherent to the system's design.

## Configured Limits

These limits are explicitly defined by configuration properties.

### Namespace and Metadata Limits
| Limit Description | Configuration Property | Default Value |
|---|---|---|
| Max Buckets (Cluster-wide) | `ozone.om.max.buckets` | `100,000` |
| Max Snapshots (Cluster-wide) | `ozone.om.fs.snapshot.max.limit` | `10,000` |

### Data and Storage Limits
| Limit Description | Configuration Property | Default Value |
|---|---|---|
| Default Block Size | `ozone.scm.block.size` | `256MB` |
| Minimum DN Free Space | `hdds.datanode.volume.free-space.min` | `10GB` |

### Operational Limits
| Limit Description | Configuration Property | Default Value | Notes |
|---|---|---|---|
| Pipelines per DataNode | `ozone.scm.datanode.pipeline.limit` | `2` | A value of `0` bases the limit on the number of metadata volumes. |
| Max Concurrent Snapshot Diff Jobs | `ozone.om.snapshot.diff.thread.pool.size`| `10` | |
| Max Key Changes per Snapshot Diff | `ozone.om.snapshot.diff.max.allowed.keys.changed.per.job` | `10,000,000` | A job will fail if it exceeds this threshold. |

### Security Limits
| Limit Description | Configuration Property | Default Value |
|---|---|---|
| Delegation Token Max Lifetime | `ozone.manager.delegation.token.max-lifetime` | `7d` |

## Architectural Limits

These limits are practical maximums based on the system's design and resources, rather than a specific configuration property.

### Object and Naming
| Limit | Boundary | Notes |
|---|---|---|
| Total Keys/Objects (Cluster-wide) | **Ozone Manager Heap Size** | This is the primary scaling limit. The OM holds all key metadata in memory. |
| Total Volumes / Containers | **SCM & OM Heap Size** | The SCM and OM must track metadata for all volumes and containers. |
| Keys per Volume / Bucket | **OM Performance** | No configured limit. Performance degrades as a single bucket grows very large. |
| Max Object Size | **Practically Unlimited** | Determined by `(Number of Blocks) x (Block Size)`. The number of blocks is not limited. |
| Bucket/Volume Name Length | Min: 3, Max: 63 chars | Must be DNS-compliant. |
| Key Name Length | **No hard limit** | Practical limits are imposed by metadata storage. |
| Max Quota Value | `Long.MAX_VALUE` (~8 EB) | Applies to both namespace and storage space quotas. |

### Component and Scalability
| Limit | Boundary | Notes |
|---|---|---|
| Number of DataNodes | **OM/SCM Performance** | No configured limit. Can scale to thousands of nodes. |
| Number of Pipelines | **Dynamically Managed by SCM** | The SCM creates and destroys pipelines as needed based on cluster state. |
| Number of S3 / HTTPFS Gateways | **Unlimited** | These components are stateless and can be scaled out horizontally. |
| Number of OM/SCM nodes (HA) | Typically **3 or 5** | Requires an odd number for Raft quorum. |
| Number of Recon nodes | Typically **1** | Recon is a centralized service and not designed for HA. |
| Number of S3 Tenants | **Apache Ranger Performance** | The practical limit is determined by the scalability of the external Ranger service. |

### Replication
| Limit | Boundary | Notes |
|---|---|---|
| RATIS Replication Factor | **1 or 3** | These are the only supported values for standard replication. |
| EC `RS-k-m` Policy | Requires **k+m** nodes | To write new data, the cluster must have at least `k` (data) + `m` (parity) DataNodes. |

### Hardware and Resources
| Limit | Boundary | Notes |
|---|---|---|
| Total Storage per DataNode | **Host OS / Hardware** | Limited by the number of disks and their capacity as supported by the host. |
| Data Directories per DataNode | **Host OS** | No configured limit. Set via a comma-separated list in `hdds.datanode.dir`. |
| Component JVM Heap Size | **Host Physical RAM** | See the verbose document version for specific component recommendations. |
