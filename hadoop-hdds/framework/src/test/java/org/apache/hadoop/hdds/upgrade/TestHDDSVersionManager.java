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

package org.apache.hadoop.hdds.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Tests for {@link HDDSVersionManager} using on-disk {@link Storage} under a JUnit temp directory.
 */
class TestHDDSVersionManager extends AbstractComponentVersionManagerTest {

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
    Path storageRoot = Files.createTempDirectory(tempDir, "hdds-version-manager-");
    Storage storage = new TestStorage(storageRoot.toFile(), serializedApparentVersion);
    storage.setApparentVersion(serializedApparentVersion);
    storage.getCurrentDir().mkdirs();
    storage.persistCurrentState();
    return new HDDSVersionManager(storage);
  }

  @Override
  protected List<ComponentVersion> allVersionsInOrder() {
    return ALL_VERSIONS;
  }

  @Override
  protected ComponentVersion expectedSoftwareVersion() {
    return HDDSVersion.SOFTWARE_VERSION;
  }

  private static final class TestStorage extends Storage {
    TestStorage(File root, int defaultLayoutVersion) throws IOException {
      super(NodeType.DATANODE, root, "hdds-test", defaultLayoutVersion);
    }

    @Override
    protected Properties getNodeProperties() {
      return new Properties();
    }
  }
}
