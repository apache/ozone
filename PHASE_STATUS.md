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
