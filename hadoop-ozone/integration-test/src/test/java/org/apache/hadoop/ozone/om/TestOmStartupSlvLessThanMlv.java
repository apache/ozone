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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test that the ozone manager will not start when it loads a VERSION file
 * indicating a metadata layout version larger than its software layout version.
 */
public class TestOmStartupSlvLessThanMlv {

  @TempDir
  private Path folder;

  @Test
  public void testStartupSlvLessThanMlv() throws Exception {
    // Add subdirectories under the temporary folder where the version file
    // will be placed.
    String subDir = folder.toAbsolutePath() + "/om/current";
    File omSubdir = Files.createDirectories(Paths.get(subDir)).toFile();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());

    // Set metadata layout version larger than software layout version.
    int largestSlv = 0;
    for (LayoutFeature f: OMLayoutFeature.values()) {
      largestSlv = Math.max(largestSlv, f.layoutVersion());
    }
    int mlv = largestSlv + 1;

    // Create version file with MLV > SLV, which should fail the cluster build.
    UpgradeTestUtils.createVersionFile(omSubdir, HddsProtos.NodeType.OM, mlv);

    MiniOzoneCluster.Builder clusterBuilder = MiniOzoneCluster.newBuilder(conf);

    GenericTestUtils.withLogDisabled(MiniOzoneClusterImpl.class, () -> {
      OMException omException = assertThrows(OMException.class,
          clusterBuilder::build);
      String expectedMessage = String.format("Cannot initialize " +
          "VersionManager. Metadata layout version (%s) > software layout" +
          " version (%s)", mlv, mlv - 1);
      assertEquals(expectedMessage, omException.getMessage());
    });
  }
}
