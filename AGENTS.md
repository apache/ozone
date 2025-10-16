# Project: Apache Ozone Distributed Object Store

## General Instructions:

- When contributing to Apache Ozone, please follow the existing Java coding style and module structure.
- All public classes and methods must include proper Javadoc comments.
- Unit and integration tests are mandatory for all new features or behavior changes.
- Ensure compatibility with Java 8+ (Ozone 1.x) or Java 11+ (Ozone 2.x).
- All code must pass `mvn clean install -DskipTests=false`.
- Before commit and push, run scripts hadoop-ozone/dev-support/checks/findbugs.sh and fix any code style problems; subsequently, run hadoop-ozone/dev-support/checks/checkstyle.sh and fix any bugs.

## Coding Style:

- Use 2 spaces for indentation.
- Follow [Apache Hadoop Java Code Conventions](https://wiki.apache.org/hadoop/CodeReviewChecklist).
- Class names should be camel case and descriptive (e.g., `KeyManagerImpl`).
- Private members should be prefixed with `this.` when accessed internally.
- Avoid static utility classes unless functionally necessary.
- Use `Objects.requireNonNull()` and `Preconditions.checkArgument()` where appropriate.

## Specific Component: `hadoop-ozone/ozone-manager`

- This module contains Ozone Manager (OM), the metadata service that manages volumes, buckets, and keys.
- Follow the existing `OMRequestHandler` dispatch pattern and transaction logging conventions.
- For any new operation types, extend the appropriate `OMClientRequest` and update `OMMetrics`.

## Specific Component: `hadoop-ozone/s3gateway`

- This module provides a RESTful S3-compatible API layer on top of Ozone FS.
- When adding new S3 API endpoints:
  - Follow the pattern used in `org.apache.hadoop.ozone.s3.endpoint.*`.
  - Ensure authentication, signature validation (AWS v2/v4), and error codes match S3 expectations.
  - Write Robot-based acceptance tests in `hadoop-ozone/dist/src/test/compose`.

## Specific Component: `hadoop-hdds/container-service`

- This module is responsible for the physical storage of containers and block data.
- Any changes here must preserve compatibility with **Ratis pipelines**, **container replication**, and **HDDS volume layout**.
- When modifying container state transitions, update state machine logic and ensure the consistency of container metadata.

## Regarding Dependencies:

- Avoid introducing new dependencies unless they provide substantial architectural benefit.
- All dependencies must be Apache License 2.0 compatible and should be declared in the relevant `pom.xml`.
- For new shaded dependencies (e.g., AWS SDK, Jetty), isolate them via module-level shading rules.
- If a new dependency is required, document the reason and impact in the associated JIRA.

## Documentation:
- User documents are written in Markdown format, and hosted by Hugo under hadoop-hdds/docs/content directory.
- Read the README.md under the hadoop-hdds/docs directory to learn more about docs.


## Common Build Commands:

- Always use JDK8.
- Full build: `mvn clean install -DskipTests -Pdist -Dmaven.javadoc.skip=true -DskipShade -DskipRecon`
- Run unit tests: `mvn test`
- Run acceptance tests: `dev-support/checks/acceptance.sh s3a`
- Start dev cluster: `docker-compose -f ./compose/ozone/ozone-up.yaml up`

## Notes:

- For any changes to Ozone on Kubernetes, update Helm charts under `hadoop-ozone/dist/helm/ozone`.
- For documentation updates, modify Markdown files under `hadoop-ozone/site/content/docs/`.
- All commits must reference a JIRA ticket (e.g., `HDDS-4567`).

---

