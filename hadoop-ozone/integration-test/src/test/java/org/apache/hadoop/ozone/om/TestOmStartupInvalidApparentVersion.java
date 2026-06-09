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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneManagerVersion.SOFTWARE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures OM does not start when the VERSION file records an apparent version newer than the software version
 * baked into this process ({@link org.apache.hadoop.ozone.OzoneManagerVersion#SOFTWARE_VERSION}).
 */
public class TestOmStartupInvalidApparentVersion {

  @TempDir
  private Path folder;

  @Test
  public void testStartupFailsWhenApparentVersionBetweenLastLayoutFeatureAndZdu()
      throws Exception {
    assertStartupFailsWithComponentVersionMessage(OMLayoutFeature.SNAPSHOT_DEFRAG.layoutVersion() + 1);
  }

  @Test
  public void testStartupFailsWhenApparentVersionBeyondLastKnownComponentVersion()
      throws Exception {
    assertStartupFailsWithComponentVersionMessage(SOFTWARE_VERSION.serialize() + 1);
  }

  private void assertStartupFailsWithComponentVersionMessage(int serializedApparentVersion)
      throws Exception {
    String subDir = folder.toAbsolutePath() + "/om/current";
    File omSubdir = Files.createDirectories(Paths.get(subDir)).toFile();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.toAbsolutePath().toString());

    UpgradeTestUtils.createVersionFile(omSubdir, HddsProtos.NodeType.OM, serializedApparentVersion);

    MiniOzoneCluster.Builder clusterBuilder = MiniOzoneCluster.newBuilder(conf).withoutDatanodes();

    String expectedMessage =
        "Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion
            + " for software version " + SOFTWARE_VERSION + ". Make sure OM was not downgraded after"
            + " finalization";

    GenericTestUtils.withLogDisabled(MiniOzoneClusterImpl.class, () -> {
      IOException ioException = assertThrows(IOException.class, clusterBuilder::build);
      assertEquals(expectedMessage, ioException.getMessage());
    });
  }
}
