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

package org.apache.hadoop.ozone.debug.om;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Unit test for ContainerToKeyMapping tool.
 */
public class TestContainerToKeyMapping {

  @TempDir
  private Path tempDir;

  private OMMetadataManager omMetadataManager;
  private String dbPath;
  private CommandLine cmd;
  private StringWriter outWriter;

  private static final String VOLUME_NAME = "vol1";
  private static final String FSO_BUCKET_NAME = "fso-bucket";
  private static final String OBS_BUCKET_NAME = "obs-bucket";
  private static final long VOLUME_ID = 100L;
  private static final long FSO_BUCKET_ID = 200L;
  private static final long OBS_BUCKET_ID = 250L;
  private static final long DIR_ID = 300L;
  private static final long FILE_ID = 400L;
  private static final long KEY_ID = 450L;
  private static final long CONTAINER_ID_1 = 1L;
  private static final long CONTAINER_ID_2 = 2L;
  private static final long CONTAINER_ID_3 = 3L;
  private static final long UNREFERENCED_FILE_ID = 500L;
  private static final long MISSING_DIR_ID = 999L;  // Non-existent parent

  @BeforeEach
  public void setup() throws Exception {
    Path dbFile = tempDir.resolve("om.db");
    dbPath = dbFile.toString();
    
    cmd = new OzoneDebug().getCmd();
    outWriter = new StringWriter();
    StringWriter errWriter = new StringWriter();
    cmd.setOut(new PrintWriter(outWriter));
    cmd.setErr(new PrintWriter(errWriter));
    
    OzoneConfiguration conf = new OzoneConfiguration();
    omMetadataManager = new OmMetadataManagerImpl(conf,
        tempDir.toFile(), "om.db", false);

    createTestData();
    
    omMetadataManager.getStore().close();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (omMetadataManager != null && !omMetadataManager.getStore().isClosed()) {
      omMetadataManager.getStore().close();
    }
  }

  @Test
  public void testContainerToKeyMapping() {
    int exitCode = execute("--containers", CONTAINER_ID_1 + "," + CONTAINER_ID_2);
    assertEquals(0, exitCode);

    String output = outWriter.toString();

    // Check FSO key
    assertThat(output).contains("\"" + CONTAINER_ID_1 + "\"");
    assertThat(output).contains("vol1/fso-bucket/dir1/file1");

    // Check OBS key
    assertThat(output).contains("\"" + CONTAINER_ID_2 + "\"");
    assertThat(output).contains("/vol1/obs-bucket/key1");
  }

  @Test
  public void testContainerToKeyMappingWithOnlyFileNames() {
    int exitCode = execute("--containers", CONTAINER_ID_1 + "," + CONTAINER_ID_2, "--onlyFileNames");
    assertEquals(0, exitCode);

    String output = outWriter.toString();

    // Check FSO key - should show only filename
    assertThat(output).contains("\"" + CONTAINER_ID_1 + "\"");
    assertThat(output).contains("file1");
    assertThat(output).doesNotContain("vol1/fso-bucket/dir1/file1");

    // Check OBS key - should also show only key name
    assertThat(output).contains("\"" + CONTAINER_ID_2 + "\"");
    assertThat(output).contains("key1");
    assertThat(output).doesNotContain("/vol1/obs-bucket/key1");
  }

  @Test
  public void testNonExistentContainer() {
    long nonExistentContainerId = 999L;
    
    int exitCode = execute("--containers", String.valueOf(nonExistentContainerId));
    assertEquals(0, exitCode);
    
    String output = outWriter.toString();
    assertThat(output).contains("\"" + nonExistentContainerId + "\"");
    assertThat(output).contains("\"numOfKeys\" : 0");
  }

  @Test
  public void testUnreferencedKeys() {
    int exitCode = execute("--containers", String.valueOf(CONTAINER_ID_3));
    assertEquals(0, exitCode);
    
    String output = outWriter.toString();

    assertThat(output).contains("\"" + CONTAINER_ID_3 + "\"");
    assertThat(output).contains("\"numOfKeys\" : 0");
    assertThat(output).contains("\"unreferencedKeys\" : 1");
  }

  private void createTestData() throws Exception {
    // Create volume
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(VOLUME_NAME)
        .setOwnerName("testUser")
        .setAdminName("admin")
        .setObjectID(VOLUME_ID)
        .build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(VOLUME_NAME), volumeArgs);

    // Create FSO bucket
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(FSO_BUCKET_NAME)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setObjectID(FSO_BUCKET_ID)
        .build();
    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(VOLUME_NAME, FSO_BUCKET_NAME), bucketInfo);

    // Create OBS bucket
    OmBucketInfo obsBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(OBS_BUCKET_NAME)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setObjectID(OBS_BUCKET_ID)
        .build();
    omMetadataManager.getBucketTable().put(
        omMetadataManager.getBucketKey(VOLUME_NAME, OBS_BUCKET_NAME), obsBucketInfo);

    // Create directory
    OmDirectoryInfo dirInfo = OmDirectoryInfo.newBuilder()
        .setName("dir1")
        .setObjectID(DIR_ID)
        .setParentObjectID(FSO_BUCKET_ID)
        .setUpdateID(1)
        .build();
    String dirKey = omMetadataManager.getOzonePathKey(VOLUME_ID, FSO_BUCKET_ID, FSO_BUCKET_ID, "dir1");
    omMetadataManager.getDirectoryTable().put(dirKey, dirInfo);

    // Create FSO file with a block in container 1
    OmKeyInfo keyInfo = createKeyInfo(
        "file1", FILE_ID, DIR_ID, CONTAINER_ID_1);
    String fileKey = omMetadataManager.getOzonePathKey(
        VOLUME_ID, FSO_BUCKET_ID, DIR_ID, "file1");
    omMetadataManager.getFileTable().put(fileKey, keyInfo);

    // Create OBS key with a block in container 2
    OmKeyInfo obsKeyInfo = createOBSKeyInfo(
        "key1", KEY_ID, CONTAINER_ID_2);
    String obsKey = omMetadataManager.getOzoneKey(
        VOLUME_NAME, OBS_BUCKET_NAME, "key1");
    omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).put(obsKey, obsKeyInfo);

    // Create unreferenced file (parent directory doesn't exist)
    OmKeyInfo unreferencedKey = createKeyInfo(
        "unreferencedFile", UNREFERENCED_FILE_ID, MISSING_DIR_ID, CONTAINER_ID_3);
    String unreferencedFileKey = omMetadataManager.getOzonePathKey(
        VOLUME_ID, FSO_BUCKET_ID, MISSING_DIR_ID, "unreferencedFile");
    omMetadataManager.getFileTable().put(unreferencedFileKey, unreferencedKey);
  }

  /**
   * Helper method to create OmKeyInfo with a block in specified container (FSO).
   */
  private OmKeyInfo createKeyInfo(String keyName, long objectId, long parentId, long containerId) {
    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(containerId, 1L))
        .setLength(1024)
        .setOffset(0)
        .build();

    OmKeyLocationInfoGroup locationGroup = new OmKeyLocationInfoGroup(0,
        Collections.singletonList(locationInfo));

    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(FSO_BUCKET_NAME)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(1024)
        .setObjectID(objectId)
        .setParentObjectID(parentId)
        .setUpdateID(1)
        .addOmKeyLocationInfoGroup(locationGroup)
        .build();
  }

  /**
   * Helper method to create OmKeyInfo for OBS keys with a block in specified container.
   */
  private OmKeyInfo createOBSKeyInfo(String keyName, long objectId, long containerId) {
    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(containerId, 1L))
        .setLength(1024)
        .setOffset(0)
        .build();

    OmKeyLocationInfoGroup locationGroup = new OmKeyLocationInfoGroup(0,
        Collections.singletonList(locationInfo));

    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(OBS_BUCKET_NAME)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(1024)
        .setObjectID(objectId)
        .setUpdateID(1)
        .addOmKeyLocationInfoGroup(locationGroup)
        .build();
  }

  private int execute(String... args) {
    List<String> argList = new ArrayList<>(Arrays.asList("om", "container-key-mapping", "--db", dbPath));
    argList.addAll(Arrays.asList(args));

    return cmd.execute(argList.toArray(new String[0]));
  }
}

