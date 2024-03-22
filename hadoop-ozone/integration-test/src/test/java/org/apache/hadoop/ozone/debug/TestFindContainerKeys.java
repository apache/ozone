/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.debug;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.debug.container.ContainerCommands;
import org.apache.hadoop.ozone.debug.container.ContainerKeyInfo;
import org.apache.hadoop.ozone.debug.container.ContainerKeyInfoResponse;
import org.apache.hadoop.ozone.debug.container.FindContainerKeys;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for {@link FindContainerKeys}.
 */
public class TestFindContainerKeys {
  private DBStore dbStore;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private String[] cmdArgs;

  @BeforeEach
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    stdout = new StringWriter();
    pstdout = new PrintWriter(stdout);
    stderr = new StringWriter();
    pstderr = new PrintWriter(stderr);

    cmd = new CommandLine(new OzoneDebug())
        .addSubcommand(new ContainerCommands())
        .addSubcommand(new FindContainerKeys())
        .setOut(pstdout)
        .setErr(pstderr);

    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(tempDir.toPath()).addTable(KEY_TABLE).addTable(FILE_TABLE)
        .addTable(DIRECTORY_TABLE)
        .build();

    cmdArgs =
        new String[]{"find-keys", "--om-db", dbStore.getDbLocation().getAbsolutePath(), "--container-ids", "1 2 3"};
  }

  @AfterEach
  public void shutdown() throws IOException {
    closeDbStore();
    pstderr.close();
    stderr.close();
    pstdout.close();
    stdout.close();
  }

  @Test
  void testFSO() throws Exception {
    /*
      Structure:
      keyName (container id)

      /vol1/bucet1
        - key1 (1)
        - dir1
          - key2 (2)
          - dir2
            - key3 (3)
            - key4 (3)
        - key5 (4)
     */
    long volumeId = -123L;
    long bucketId = -456L;
    long dirObjectId1 = -789L;
    long dirObjectId2 = -788L;
    createDirectory(volumeId, bucketId, bucketId, dirObjectId1, "dir1");
    createDirectory(volumeId, bucketId, dirObjectId1, dirObjectId2, "dir2");
    createFile(volumeId, bucketId, "key1", -987L, bucketId, 1L);
    createFile(volumeId, bucketId, "key2", -986L, dirObjectId1, 2L);
    createFile(volumeId, bucketId, "key3", -985L, dirObjectId2, 3L);
    createFile(volumeId, bucketId, "key4", -984L, dirObjectId2, 3L);
    createFile(volumeId, bucketId, "key5", -983L, dirObjectId2, 4L);

    closeDbStore();

    int exitCode = cmd.execute(cmdArgs);
    assertThat(exitCode).isEqualTo(0);

    // Create expected response
    List<ContainerKeyInfo> expectedKeysForContainer1 = new ArrayList<>();
    expectedKeysForContainer1.add(new ContainerKeyInfo(1L, "vol1", volumeId, "bucket1", bucketId, "key1", bucketId));
    List<ContainerKeyInfo> expectedKeysForContainer2 = new ArrayList<>();
    expectedKeysForContainer2.add(
        new ContainerKeyInfo(2L, "vol1", volumeId, "bucket1", bucketId, "dir1/key2", dirObjectId1));
    List<ContainerKeyInfo> expectedKeysForContainer3 = new ArrayList<>();
    expectedKeysForContainer3.add(
        new ContainerKeyInfo(3L, "vol1", volumeId, "bucket1", bucketId, "dir1/dir2/key3", dirObjectId2));
    expectedKeysForContainer3.add(
        new ContainerKeyInfo(3L, "vol1", volumeId, "bucket1", bucketId, "dir1/dir2/key4", dirObjectId2));
    Map<Long, List<ContainerKeyInfo>> expectedContainerIdToKeyInfos = new HashMap<>();
    expectedContainerIdToKeyInfos.put(1L, expectedKeysForContainer1);
    expectedContainerIdToKeyInfos.put(2L, expectedKeysForContainer2);
    expectedContainerIdToKeyInfos.put(3L, expectedKeysForContainer3);
    ContainerKeyInfoResponse expectedResponse = new ContainerKeyInfoResponse(5, expectedContainerIdToKeyInfos);
    assertThat(GSON.fromJson(stdout.toString(), ContainerKeyInfoResponse.class)).isEqualTo(expectedResponse);

    assertThat(stderr.toString()).isEmpty();
  }

  @Test
  void testNonFSO() throws Exception {
    /*
      Structure:
      keyName (container id)

      /vol1/bucket1
        - key1 (1)
          - dir1/key2 (2)
          - dir1/dir2/key3 (3)
          - dir1/dir2/key4 (3)
        - key5 (4)
     */
    createKey("key1", 1L);
    createKey("dir1/key2", 2L);
    createKey("dir1/dir2/key3", 3L);
    createKey("dir1/dir2/key4", 3L);
    createKey("key5", 4L);

    closeDbStore();

    int exitCode = cmd.execute(cmdArgs);
    assertThat(exitCode).isEqualTo(0);

    // Create expected response
    List<ContainerKeyInfo> expectedKeysForContainer1 = new ArrayList<>();
    expectedKeysForContainer1.add(new ContainerKeyInfo(1L, "vol1", 0, "bucket1", 0, "key1", 0));
    List<ContainerKeyInfo> expectedKeysForContainer2 = new ArrayList<>();
    expectedKeysForContainer2.add(
        new ContainerKeyInfo(2L, "vol1", 0, "bucket1", 0, "dir1/key2", 0));
    List<ContainerKeyInfo> expectedKeysForContainer3 = new ArrayList<>();
    expectedKeysForContainer3.add(
        new ContainerKeyInfo(3L, "vol1", 0, "bucket1", 0, "dir1/dir2/key3", 0));
    expectedKeysForContainer3.add(
        new ContainerKeyInfo(3L, "vol1", 0, "bucket1", 0, "dir1/dir2/key4", 0));
    Map<Long, List<ContainerKeyInfo>> expectedContainerIdToKeyInfos = new HashMap<>();
    expectedContainerIdToKeyInfos.put(1L, expectedKeysForContainer1);
    expectedContainerIdToKeyInfos.put(2L, expectedKeysForContainer2);
    expectedContainerIdToKeyInfos.put(3L, expectedKeysForContainer3);
    ContainerKeyInfoResponse expectedResponse = new ContainerKeyInfoResponse(5, expectedContainerIdToKeyInfos);
    assertThat(GSON.fromJson(stdout.toString(), ContainerKeyInfoResponse.class)).isEqualTo(expectedResponse);

    assertThat(stderr.toString()).isEmpty();
  }

  /**
   * Close db store because of the lock.
   */
  private void closeDbStore() throws IOException {
    if (dbStore != null && !dbStore.isClosed()) {
      dbStore.close();
    }
  }

  @Test
  void testWhenThereAreNoKeysForContainerIds() throws Exception {

    // create keys for tables
    long volumeId = -123L;
    long bucketId = -456L;
    createFile(volumeId, bucketId, "key1", -987L, bucketId, 4L);
    createKey("key2", 5L);
    createKey("key3", 6L);

    closeDbStore();

    int exitCode = cmd.execute(cmdArgs);
    assertThat(exitCode).isEqualTo(0);

    assertThat(stderr.toString()).contains("No keys were found for container IDs: [1, 2, 3]\n" + "Keys processed: 3\n");

    assertThat(stdout.toString()).isEmpty();
  }

  private void createFile(long volumeId, long bucketId, String keyName, long objectId, long parentId, long containerId)
      throws Exception {
    try (Table<byte[], byte[]> table = dbStore.getTable(FILE_TABLE)) {
      // format: /volumeId/bucketId/parentId(bucketId)/keyName
      String key =
          OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX + parentId + OM_KEY_PREFIX + keyName;

      OmKeyInfo value = getOmKeyInfo("vol1", "bucket1", keyName, containerId, objectId, parentId);

      table.put(key.getBytes(UTF_8), value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
    }
  }

  private void createKey(String keyName, long containerId) throws Exception {
    try (Table<byte[], byte[]> table = dbStore.getTable(KEY_TABLE)) {
      String volumeName = "vol1";
      String bucketName = "bucket1";
      // format: /volumeName/bucketName/keyName
      String key = OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName + OM_KEY_PREFIX + keyName;

      // generate table value
      OmKeyInfo value = getOmKeyInfo(volumeName, bucketName, keyName, containerId, 0, 0);

      table.put(key.getBytes(UTF_8), value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
    }
  }

  private void createDirectory(long volumeId, long bucketId, long parentId, long objectId, String keyName)
      throws Exception {
    try (Table<byte[], byte[]> table = dbStore.getTable(DIRECTORY_TABLE)) {

      // format: /volumeId/bucketId/parentId(bucketId)/keyName
      String key =
          OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX + parentId + OM_KEY_PREFIX + keyName;

      OmDirectoryInfo value = OMRequestTestUtils.createOmDirectoryInfo(keyName, objectId, parentId);

      table.put(key.getBytes(UTF_8), value.getProtobuf().toByteArray());
    }
  }

  private static OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                        String keyName, long containerId,
                                        long objectId,
                                        long parentId) {
    return OMRequestTestUtils
        .createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE), objectId, parentId,
            new OmKeyLocationInfoGroup(0L, Collections.singletonList(new OmKeyLocationInfo.Builder()
                .setBlockID(new BlockID(containerId, 1))
                .build())))
        .build();
  }

}
