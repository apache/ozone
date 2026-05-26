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

package org.apache.hadoop.hdds.scm.upgrade;

import static org.apache.hadoop.hdds.HDDSVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_HA;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures SCM does not start when the VERSION file records an apparent version
 * that is larger than the software version.
 */
public class TestScmStartupInvalidApparentVersion {

  @TempDir
  private Path folder;

  @Test
  public void testStartupFailsWhenApparentVersionBetweenLastLayoutFeatureAndZdu() throws Exception {
    assertStartupFailsWithComponentVersionMessage(
        HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION.layoutVersion() + 1);
  }

  @Test
  public void testStartupFailsWhenApparentVersionBeyondLastKnownComponentVersion() throws Exception {
    assertStartupFailsWithComponentVersionMessage(SOFTWARE_VERSION.serialize() + 1);
  }

  private void assertStartupFailsWithComponentVersionMessage(int serializedApparentVersion)
      throws Exception {
    File scmSubdir = folder.resolve("scm").resolve("current").toFile();
    assertTrue(scmSubdir.mkdirs());

    File ratisDir = folder.resolve("scm.ratis").toFile();
    File snapshotDir = folder.resolve("scm.ratis.snapshot").toFile();
    assertTrue(ratisDir.mkdirs());
    assertTrue(snapshotDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, folder.toAbsolutePath().toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.toAbsolutePath().toString());

    Properties properties = new Properties();
    properties.setProperty(SCM_ID, "scm");
    properties.setProperty(SCM_HA, "true");

    UpgradeTestUtils.createVersionFile(scmSubdir, HddsProtos.NodeType.SCM, serializedApparentVersion, properties);

    String expectedMessage =
        "Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion
            + " for software version " + HDDSVersion.SOFTWARE_VERSION + ". Make sure this component was not" +
            " downgraded after finalization";

    IOException ioException =
        assertThrows(IOException.class, () -> new StorageContainerManager(conf));
    assertEquals(expectedMessage, ioException.getMessage());
  }
}
