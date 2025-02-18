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

import static org.apache.hadoop.ozone.OzoneConsts.SCM_HA;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that SCM will throw an exception on creation when it reads in a
 * VERSION file indicating a metadata layout version larger than its
 * software layout version.
 */
public class TestScmStartupSlvLessThanMlv {

  @Test
  public void testStartupSlvLessThanMlv(@TempDir Path tempDir)
      throws Exception {
    // Add subdirectories under the temporary folder where the version file
    // will be placed.
    File scmSubdir = tempDir.resolve("scm").resolve("current").toFile();
    assertTrue(scmSubdir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS,
        tempDir.toAbsolutePath().toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toAbsolutePath().toString());

    // Set metadata layout version larger then software layout version.
    int largestSlv = 0;
    for (LayoutFeature f: HDDSLayoutFeature.values()) {
      largestSlv = Math.max(largestSlv, f.layoutVersion());
    }
    int mlv = largestSlv + 1;

    Properties properties = new Properties();
    properties.setProperty(SCM_ID, "scm");
    properties.setProperty(SCM_HA, "true");

    // Create version file with MLV > SLV, which should fail the SCM
    // construction.
    UpgradeTestUtils.createVersionFile(scmSubdir, HddsProtos.NodeType.SCM, mlv,
        properties);


    Throwable t = assertThrows(IOException.class,
        () -> new StorageContainerManager(conf));
    String expectedMessage = String.format("Cannot initialize VersionManager." +
            " Metadata layout version (%s) > software layout version (%s)",
        mlv, largestSlv);
    assertEquals(expectedMessage, t.getMessage());
  }
}
