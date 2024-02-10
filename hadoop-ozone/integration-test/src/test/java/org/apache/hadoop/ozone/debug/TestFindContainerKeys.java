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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.debug.container.ContainerKeyInfo;
import org.apache.hadoop.ozone.debug.container.ContainerKeyInfoResponse;
import org.apache.hadoop.ozone.debug.container.FindContainerKeys;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * This class tests `ozone debug ckscanner` CLI that reads from RocksDB
 * and gets keys for container ids.
 */
public class TestFindContainerKeys {
  private DBStore dbStore;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;
  private static final Gson GSON =
      new GsonBuilder().setPrettyPrinting().create();
  private static final ContainerKeyInfo KEY_ONE =
      new ContainerKeyInfo(1L, "vol1", -123L, "bucket1", -456L, "dir1/key1",
          -789L);
  private static final ContainerKeyInfo KEY_TWO =
      new ContainerKeyInfo(2L, "vol1", 0L, "bucket1", 0L, "key2", 0L);
  private static final ContainerKeyInfo KEY_THREE =
      new ContainerKeyInfo(3L, "vol1", 0L, "bucket1", 0L, "key3", 0L);

  private static final Map<Long, List<ContainerKeyInfo>> CONTAINER_KEYS =
      new HashMap<>();

  static {
    List<ContainerKeyInfo> list1 = new ArrayList<>();
    list1.add(KEY_ONE);
    List<ContainerKeyInfo> list2 = new ArrayList<>();
    list2.add(KEY_TWO);
    List<ContainerKeyInfo> list3 = new ArrayList<>();
    list3.add(KEY_THREE);
    CONTAINER_KEYS.put(1L, list1);
    CONTAINER_KEYS.put(2L, list2);
    CONTAINER_KEYS.put(3L, list3);
  }

  private static final ContainerKeyInfoResponse KEYS_FOUND_OUTPUT =
      new ContainerKeyInfoResponse(3, CONTAINER_KEYS);

  private static final String KEYS_NOT_FOUND_OUTPUT =
      "No keys were found for container IDs: [1, 2, 3]\n" +
          "Keys processed: 3\n";

  @BeforeEach
  public void setup() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    stdout = new StringWriter();
    pstdout = new PrintWriter(stdout);
    stderr = new StringWriter();
    pstderr = new PrintWriter(stderr);

    cmd = new CommandLine(new OzoneDebug())
        .addSubcommand(new FindContainerKeys())
        .setOut(pstdout)
        .setErr(pstderr);

    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(tempDir.toPath()).addTable(KEY_TABLE).addTable(FILE_TABLE)
        .addTable(DIRECTORY_TABLE)
        .build();
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
  void testWhenThereAreKeysForContainerIds() throws IOException {

    // create keys for tables
    long volumeId = -123L;
    long bucketId = -456L;
    long dirObjectId = -789L;
    createDirectory(volumeId, bucketId, bucketId, dirObjectId, "dir1");
    createFile(volumeId, bucketId, "key1", -987L, dirObjectId, 1L);
    createKey("key2", 2L);
    createKey("key3", 3L);

    String[] cmdArgs =
        {"ckscanner", "--om-db", dbStore.getDbLocation().getAbsolutePath(),
            "--container-ids", "1 2 3"};

    closeDbStore();

    int exitCode = cmd.execute(cmdArgs);
    Assertions.assertEquals(0, exitCode);

    Assertions.assertEquals(
        GSON.fromJson(stdout.toString(), ContainerKeyInfoResponse.class),
        KEYS_FOUND_OUTPUT);

    Assertions.assertTrue(stderr.toString().isEmpty());
  }

  /**
   * Close db store because of the lock.
   */
  private void closeDbStore() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  void testWhenThereAreNotKeysForContainerIds() throws IOException {

    // create keys for tables
    long volumeId = -123L;
    long bucketId = -456L;
    createFile(volumeId, bucketId, "key1", -987L, bucketId, 4L);
    createKey("key2", 5L);
    createKey("key3", 6L);

    String[] cmdArgs =
        {"ckscanner", "--om-db", dbStore.getDbLocation().getAbsolutePath(),
            "--container-ids", "1 2 3"};

    closeDbStore();

    int exitCode = cmd.execute(cmdArgs);
    Assertions.assertEquals(0, exitCode);

    Assertions.assertTrue(stderr.toString().contains(KEYS_NOT_FOUND_OUTPUT));

    Assertions.assertTrue(stdout.toString().isEmpty());
  }

  private void createFile(long volumeId, long bucketId, String keyName,
                          long objectId, long parentId, long containerId)
      throws IOException {
    Table<byte[], byte[]> table = dbStore.getTable(FILE_TABLE);

    // format: /volumeId/bucketId/parentId(bucketId)/keyName
    String key =
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX +
            bucketId + OM_KEY_PREFIX + parentId +
            OM_KEY_PREFIX + keyName;

    OmKeyInfo value =
        getOmKeyInfo("vol1", "bucket1", keyName, containerId, objectId,
            parentId);

    table.put(key.getBytes(UTF_8),
        value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
  }

  private void createKey(String keyName, long containerId) throws IOException {
    Table<byte[], byte[]> table = dbStore.getTable(KEY_TABLE);

    String volumeName = "vol1";
    String bucketName = "bucket1";
    // format: /volumeName/bucketName/keyName
    String key = OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName +
        OM_KEY_PREFIX + keyName;

    // generate table value
    OmKeyInfo value =
        getOmKeyInfo(volumeName, bucketName, keyName, containerId, 0, 0);

    table.put(key.getBytes(UTF_8),
        value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
  }

  private void createDirectory(long volumeId, long bucketId, long parentId,
                               long objectId, String keyName)
      throws IOException {
    Table<byte[], byte[]> table = dbStore.getTable(DIRECTORY_TABLE);

    // format: /volumeId/bucketId/parentId(bucketId)/keyName
    String key =
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX +
            parentId + OM_KEY_PREFIX + keyName;

    OmDirectoryInfo value =
        OMRequestTestUtils.createOmDirectoryInfo(keyName, objectId, parentId);

    table.put(key.getBytes(UTF_8), value.getProtobuf().toByteArray());
  }

  private static OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                        String keyName, long containerId,
                                        long objectId,
                                        long parentId) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName,
        keyName, HddsProtos.ReplicationType.STAND_ALONE,
        HddsProtos.ReplicationFactor.ONE, objectId, parentId, 1, 1, 1, false,
        new ArrayList<>(
            Collections.singletonList(
                new OmKeyLocationInfo.Builder().setBlockID(
                    new BlockID(containerId, 1)).build())));
  }

}
