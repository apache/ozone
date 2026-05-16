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

# Managed Local S3 Access Keys Phase Status

## Phase 1 - Completed

Scope completed:
- Added managed local S3 access-key configuration keys and defaults.
- Added `ozone-default.xml` entries for the Phase 1 keys.
- Added a pure config parsing and scalar validation helper.
- Added OM startup validation for unsafe non-secure managed-key mode.
- Added focused tests for defaults, parsing, scalar bounds, and startup gates.

Files changed:
- `hadoop-hdds/common/src/main/java/org/apache/hadoop/ozone/OzoneConfigKeys.java`
- `hadoop-hdds/common/src/main/resources/ozone-default.xml`
- `hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/security/ManagedS3AccessKeyConfig.java`
- `hadoop-ozone/common/src/test/java/org/apache/hadoop/ozone/security/TestManagedS3AccessKeyConfig.java`
- `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java`
- `hadoop-ozone/ozone-manager/src/test/java/org/apache/hadoop/ozone/om/TestOzoneManagerManagedS3AccessKeyStartup.java`
- `PHASE_STATUS.md`

Tests and checks run:
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/common -am -Dtest=TestManagedS3AccessKeyConfig test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/ozone-manager -am -Dtest=TestOzoneManagerManagedS3AccessKeyStartup test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/integration-test -am -Dtest=TestOzoneConfigurationFields test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-hdds/common,hadoop-ozone/common,hadoop-ozone/ozone-manager -DskipTests checkstyle:check` - passed after blocker fix.
- `git diff --check` - passed.

Blockers found and fixed:
- Fixed an Ozone checkstyle hidden-field violation in `OzoneManager.validateManagedS3AccessKeyStartup(...)` by renaming the new boolean parameter.

Remaining follow-ups:
- Phase 2 must add the proto/model/table and layout/checkpoint awareness.
- Later phases must preserve STS precedence, legacy S3 secret compatibility,
  non-secure fail-closed behavior, HA freshness, and no plaintext secret
  leakage.
- Runtime credential validation, policy evaluation, CLI commands, KMS/encryption
  lifecycle, and S3 request-path behavior remain intentionally out of scope for
  Phase 1.

## Phase 1b - Completed

Scope completed:
- Added managed access-key encryption key-name configuration with an empty
  disabled-mode default.
- Added one-time retrieval-handle TTL and max-entry configuration for the
  accepted Option B plaintext transport model.
- Extended the config helper to validate encryption key-name requirements and
  retrieval-handle bounds.
- Added OM startup validation for enabled-mode KMS prerequisites: usable
  durable Hadoop KeyProviderCryptoExtension, existing configured key, and
  current key version availability.
- Updated the design doc to use the two-step retrieval-handle model.

Files changed:
- `hadoop-hdds/common/src/main/java/org/apache/hadoop/ozone/OzoneConfigKeys.java`
- `hadoop-hdds/common/src/main/resources/ozone-default.xml`
- `hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/security/ManagedS3AccessKeyConfig.java`
- `hadoop-ozone/common/src/test/java/org/apache/hadoop/ozone/security/TestManagedS3AccessKeyConfig.java`
- `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java`
- `hadoop-ozone/ozone-manager/src/test/java/org/apache/hadoop/ozone/om/TestOzoneManagerManagedS3AccessKeyStartup.java`
- `hadoop-hdds/docs/content/design/managed-local-s3-access-keys.md`
- `PHASE_STATUS.md`

Tests and checks run:
- `git diff --check` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/common -am -Dtest=TestManagedS3AccessKeyConfig test` - passed after blocker fix.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/ozone-manager -am -Dtest=TestOzoneManagerManagedS3AccessKeyStartup test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/integration-test -am -Dtest=TestOzoneConfigurationFields test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-hdds/common,hadoop-ozone/common,hadoop-ozone/ozone-manager -DskipTests checkstyle:check` - passed.

Blockers found and fixed:
- Fixed `TestManagedS3AccessKeyConfig.xmlDefaultsMatchConstants` to assert the
  effective empty/default value for `<value/>`, because Hadoop configuration
  returns `null` for empty XML values.

Remaining follow-ups:
- Phase 3/4 must validate operation-level envelope generation/decryption
  behavior before create/rotate; Phase 1b only verifies provider initialization,
  durable provider status, key existence, and current key-version availability.
- `RetrieveManagedS3AccessKeySecret` and retrieval-handle map behavior remain
  intentionally unimplemented until the combined Phase 3/4 lifecycle work.
- S3 request-path validation, LocalJsonPolicyEvaluator, CLI commands, and
  HDDS-15273 / STS behavior remain intentionally untouched.

## Phase 2 - Completed

Scope completed:
- Added the storage proto/model for managed local S3 access-key metadata.
- Registered the `s3ManagedAccessKeyTable` OM metadata table.
- Added the typed metadata-manager table accessor and initialization.
- Added a layout-version marker without enabling credential validation.
- Added compatibility handling for old checkpoints that do not yet contain the
  new optional table.
- Added focused model, metadata-manager, layout, checkpoint, and Recon tests.

Files changed:
- `hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto`
- `hadoop-ozone/interface-client/src/main/resources/proto.lock`
- `hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/helpers/S3ManagedAccessKeyInfo.java`
- `hadoop-ozone/common/src/test/java/org/apache/hadoop/ozone/om/helpers/TestS3ManagedAccessKeyInfoCodec.java`
- `hadoop-ozone/interface-storage/src/main/java/org/apache/hadoop/ozone/om/OMMetadataManager.java`
- `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OmMetadataManagerImpl.java`
- `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/codec/OMDBDefinition.java`
- `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutFeature.java`
- `hadoop-ozone/ozone-manager/src/test/java/org/apache/hadoop/ozone/om/TestOmMetadataManager.java`
- `hadoop-ozone/ozone-manager/src/test/java/org/apache/hadoop/ozone/om/upgrade/TestOMVersionManager.java`
- `hadoop-ozone/recon/src/test/java/org/apache/hadoop/ozone/recon/recovery/TestReconOmMetadataManagerImpl.java`
- `PHASE_STATUS.md`

Tests and checks run:
- `python3 -c 'import json; json.load(open("hadoop-ozone/interface-client/src/main/resources/proto.lock")); print("proto.lock JSON OK")'` - passed.
- `python3 -m json.tool hadoop-ozone/interface-client/src/main/resources/proto.lock >/tmp/proto.lock.check` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/interface-client,hadoop-ozone/common,hadoop-ozone/interface-storage,hadoop-ozone/ozone-manager -am -DskipTests compile` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/common -am -Dtest=TestS3ManagedAccessKeyInfoCodec test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/ozone-manager -am -Dtest=TestOMDBDefinition,TestOmMetadataManager,TestOMVersionManager test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/recon -am -Dtest=TestReconOmMetadataManagerImpl test` - passed.
- `mvn -Dmaven.repo.local=/tmp/m2-ozone -pl hadoop-ozone/common,hadoop-ozone/interface-storage,hadoop-ozone/ozone-manager,hadoop-ozone/recon -DskipTests checkstyle:check` - passed.
- `git diff --check` - passed.

Blockers found and fixed:
- None.

Remaining follow-ups:
- Future write-capable phases must gate managed-key behavior on the
  `MANAGED_LOCAL_S3_ACCESS_KEYS` layout feature until finalization.
- Phase 2 intentionally adds no OM admin lifecycle, S3G credential validation,
  secret generation/encryption lifecycle, policy evaluator, CLI commands, or
  STS/legacy compatibility behavior.
- A negative test for old checkpoints missing a required OM column family would
  further lock in that only the new optional table is repaired.
- The old read-only checkpoint compatibility path creates the missing optional
  column family before reopening read-only; physically read-only legacy
  checkpoint directories remain an integration gap.
