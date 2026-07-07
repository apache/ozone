# Ozone project map

Where things live, service boundaries, and local environment for Apache Ozone. Read on
demand; this file is not loaded into agent context at startup.

## Tech stack

- Java 8 bytecode with JDK 21 runtime compatibility (see the `[21,]` profile in `pom.xml`)
- Maven build
- Hadoop RPC and gRPC over Protobuf
- RocksDB for persistent metadata
- Apache Ratis for replicated state
- JUnit 5 for tests

## Aggregators

- `hadoop-hdds/`: storage layer and shared infrastructure. Key submodules: `server-scm`,
  `container-service`, `framework`, `managed-rocksdb`, `interface-{admin,client,server}`.
- `hadoop-ozone/`: Ozone services and clients. Key submodules: `ozone-manager`, `s3gateway`,
  `recon`, `datanode`, `dist`, `integration-test*`, `ozonefs*`.

## Service boundaries

1. SCM manages containers, pipelines, and replication metadata.
2. OM manages namespace, keys, buckets, volumes, snapshots, and most user-visible metadata.
3. Datanodes serve container data and participate in Ratis pipelines.
4. Recon provides observability and derived metadata views.
5. S3 Gateway and OzoneFS expose external APIs on top of OM and HDDS services.

Cross-cutting changes often span multiple layers: a feature or bug fix may need updates in
`hadoop-hdds/interface-*`, server-side handling, client translation code, and integration
tests.

## Key paths

- `hadoop-hdds/interface-*`: Protobuf definitions and protocol-facing interfaces
- `hadoop-hdds/server-scm`: SCM server behavior
- `hadoop-hdds/container-service`: datanode-side container handling
- `hadoop-hdds/framework`: shared service infrastructure
- `hadoop-hdds/managed-rocksdb`: RocksDB wrappers and helpers
- `hadoop-ozone/ozone-manager`: OM request handling and namespace logic
- `hadoop-ozone/s3gateway`: S3-compatible gateway
- `hadoop-ozone/recon`: Recon backend and UI
- `hadoop-ozone/datanode`: Ozone datanode service pieces outside HDDS container-service
- `hadoop-ozone/integration-test*`: Mini-cluster and integration coverage
- `hadoop-ozone/dist`: distribution assembly and compose definitions
- `hadoop-ozone/dev-support/checks`: scripts that mirror CI checks
- `.run/`: IDE launch configurations for local services and HA topologies

## Local environment

- Use a JDK 21 runtime locally; source and target compatibility remain Java 8.
- Build requirements: Unix system, JDK 8+ (build with JDK 21), Maven 3.6+, internet on first
  build, and standard tools (`make`, `gcc`).
- Formatting conventions are shared through `.editorconfig`.
- If Maven behaves unexpectedly, check `java -version` and `mvn -version` first.

## Test taxonomy

Pick the narrowest useful type:

- unit — JUnit / Java, local logic
- integration — single-JVM "mini cluster" (`MiniOzoneCluster`)
- acceptance — docker + Robot Framework
- blockade — python-based fault/partition tests
- performance / load — `ozone freon` generators

## Reference resource

- [DeepWiki](https://deepwiki.com/apache/ozone) — AI-queryable index of the codebase, useful
  for semantic Q&A during exploration.
