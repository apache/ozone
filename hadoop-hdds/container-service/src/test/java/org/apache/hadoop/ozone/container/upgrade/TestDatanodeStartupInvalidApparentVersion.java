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

import static org.apache.hadoop.hdds.HDDSVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConsts.DATANODE_LAYOUT_VERSION_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures Datanode does not start when the VERSION file records an apparent version newer than
 * {@link org.apache.hadoop.hdds.HDDSVersion#SOFTWARE_VERSION}.
 */
public class TestDatanodeStartupInvalidApparentVersion {
  @TempDir
  private Path tempFolder;

  @Test
  public void testStartupFailsWhenApparentVersionBetweenLastLayoutFeatureAndZdu()
      throws Exception {
    assertStartupFailsWithComponentVersionMessage(
        HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION.serialize() + 1);
  }

  @Test
  public void testStartupFailsWhenApparentVersionBeyondLastKnownComponentVersion()
      throws Exception {
    assertStartupFailsWithComponentVersionMessage(SOFTWARE_VERSION.serialize() + 1);
  }

  private void assertStartupFailsWithComponentVersionMessage(int serializedApparentVersion)
      throws Exception {
    File datanodeSubdir = Files.createDirectory(
        tempFolder.resolve(DATANODE_LAYOUT_VERSION_DIR)).toFile();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFolder.toString());

    UpgradeTestUtils.createVersionFile(datanodeSubdir,
        HddsProtos.NodeType.DATANODE, serializedApparentVersion);

    String expectedMessage =
        "Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion
            + " for software version " + SOFTWARE_VERSION + ". Make sure this component was not downgraded after"
            + " finalization";

    IOException ioException = assertThrows(IOException.class,
        () -> new DatanodeStateMachine(randomDatanodeDetails(), conf));
    assertEquals(expectedMessage, ioException.getMessage());
  }
}
