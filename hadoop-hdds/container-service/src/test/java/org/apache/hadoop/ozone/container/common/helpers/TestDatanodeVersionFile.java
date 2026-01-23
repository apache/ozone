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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.HDDSVolumeLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests {@link DatanodeVersionFile}.
 */
public class TestDatanodeVersionFile {

  private File versionFile;
  private Properties properties;

  private String storageID;
  private String clusterID;
  private String datanodeUUID;
  private long cTime;
  private int lv;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws IOException {
    versionFile = Files.createFile(
        folder.resolve("VersionFile")).toFile();
    storageID = UUID.randomUUID().toString();
    clusterID = UUID.randomUUID().toString();
    datanodeUUID = UUID.randomUUID().toString();
    cTime = Time.now();
    lv = HDDSVolumeLayoutVersion.getLatestVersion().getVersion();

    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, cTime, lv);

    dnVersionFile.createVersionFile(versionFile);

    properties = DatanodeVersionFile.readFrom(versionFile);
  }

  @Test
  public void testCreateAndReadVersionFile() throws IOException {

    //Check VersionFile exists
    assertTrue(versionFile.exists());

    assertEquals(storageID, StorageVolumeUtil.getStorageID(
        properties, versionFile));
    assertEquals(clusterID, StorageVolumeUtil.getClusterID(
        properties, versionFile, clusterID));
    assertEquals(datanodeUUID, StorageVolumeUtil.getDatanodeUUID(
        properties, versionFile, datanodeUUID));
    assertEquals(cTime, StorageVolumeUtil.getCreationTime(
        properties, versionFile));
    assertEquals(lv, StorageVolumeUtil.getLayOutVersion(
        properties, versionFile));
  }

  @Test
  public void testIncorrectClusterId() {
    String randomClusterID = UUID.randomUUID().toString();
    InconsistentStorageStateException exception = assertThrows(InconsistentStorageStateException.class,
        () -> StorageVolumeUtil.getClusterID(properties, versionFile, randomClusterID));
    assertThat(exception).hasMessageContaining("Mismatched ClusterIDs");
  }

  @Test
  public void testVerifyCTime() throws IOException {
    long invalidCTime = -10;
    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, invalidCTime, lv);
    dnVersionFile.createVersionFile(versionFile);
    properties = DatanodeVersionFile.readFrom(versionFile);

    InconsistentStorageStateException exception = assertThrows(InconsistentStorageStateException.class,
        () -> StorageVolumeUtil.getCreationTime(properties, versionFile));
    assertThat(exception).hasMessageContaining("Invalid Creation time in Version File : " + versionFile);
  }

  @Test
  public void testVerifyLayOut() throws IOException {
    int invalidLayOutVersion = 100;
    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, cTime, invalidLayOutVersion);
    dnVersionFile.createVersionFile(versionFile);
    Properties props = DatanodeVersionFile.readFrom(versionFile);
    InconsistentStorageStateException exception = assertThrows(InconsistentStorageStateException.class,
        () -> StorageVolumeUtil.getLayOutVersion(props, versionFile));
    assertThat(exception).hasMessageContaining("Invalid layOutVersion.");
  }
}
