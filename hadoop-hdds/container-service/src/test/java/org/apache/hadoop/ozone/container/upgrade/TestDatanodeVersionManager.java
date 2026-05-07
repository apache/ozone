/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.upgrade;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeAction;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DatanodeStorage;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Tests for {@link DatanodeVersionManager} using on-disk {@link DatanodeStorage} under a JUnit temp directory.
 */
class TestDatanodeVersionManager extends AbstractComponentVersionManagerTest {

  @TempDir
  private Path tempDir;

  private static final List<ComponentVersion> ALL_VERSIONS;

  static {
    ALL_VERSIONS = new ArrayList<>(Arrays.asList(HDDSLayoutFeature.values()));

    for (HDDSVersion version : HDDSVersion.values()) {
      // Add all defined versions after and including ZDU to get the complete version list.
      if (HDDSVersion.ZDU.isSupportedBy(version) && version != HDDSVersion.FUTURE_VERSION) {
        ALL_VERSIONS.add(version);
      }
    }
  }

  public static Stream<Arguments> preFinalizedVersionArgs() {
    return ALL_VERSIONS.stream()
        .limit(ALL_VERSIONS.size() - 1)
        .map(Arguments::of);
  }

  @Override
  protected ComponentVersionManager createManager(int serializedApparentVersion) throws IOException {
    Path storageRoot = Files.createTempDirectory(tempDir, "dn-version-manager-");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageRoot.toString());
    DatanodeStorage storage = new DatanodeStorage(conf, UUID.randomUUID().toString(),
        serializedApparentVersion);
    storage.setApparentVersion(serializedApparentVersion);
    storage.getCurrentDir().mkdirs();
    storage.persistCurrentState();
    return new DatanodeVersionManager(storage, null);
  }

  @Override
  protected List<ComponentVersion> allVersionsInOrder() {
    return ALL_VERSIONS;
  }

  @Override
  protected ComponentVersion expectedSoftwareVersion() {
    return HDDSVersion.SOFTWARE_VERSION;
  }

  @Test
  void testUpgradeActionsLoaded() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());

    DatanodeStorage storage = new DatanodeStorage(conf, UUID.randomUUID().toString(),
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    storage.initialize();

    try (DatanodeVersionManager versionManager = new DatanodeVersionManager(storage, null)) {
      Map<ComponentVersion, DatanodeUpgradeAction> actions =
          versionManager.getUpgradeActionsForTesting();
      assertTrue(actions.containsKey(HDDSLayoutFeature.DATANODE_SCHEMA_V2));
    }
  }
}
