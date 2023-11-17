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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class tests `ozone debug ldb ckscanner` CLI that reads from RocksDB
 * and gets keys for container ids.
 */
public class TestContainerKeyScanner {
  private static final String KEY_TABLE = "keyTable";
  private static final String FILE_TABLE = "fileTable";
  private static final String DIRECTORY_TABLE = "directoryTable";
  private DBStore dbStore;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;

  private static final String KEYS_FOUND_OUTPUT = "{\n" +
      "  \"keysProcessed\": 3,\n" +
      "  \"containerKeys\": {\n" +
      "    \"1\": [\n" +
      "      {\n" +
      "        \"containerID\": 1,\n" +
      "        \"volumeName\": \"vol1\",\n" +
      "        \"volumeId\": -123,\n" +
      "        \"bucketName\": \"bucket1\",\n" +
      "        \"bucketId\": -456,\n" +
      "        \"keyName\": \"dir1/key1\",\n" +
      "        \"parentId\": -789\n" +
      "      }\n" +
      "    ],\n" +
      "    \"2\": [\n" +
      "      {\n" +
      "        \"containerID\": 2,\n" +
      "        \"volumeName\": \"vol1\",\n" +
      "        \"volumeId\": 0,\n" +
      "        \"bucketName\": \"bucket1\",\n" +
      "        \"bucketId\": 0,\n" +
      "        \"keyName\": \"key2\",\n" +
      "        \"parentId\": 0\n" +
      "      }\n" +
      "    ],\n" +
      "    \"3\": [\n" +
      "      {\n" +
      "        \"containerID\": 3,\n" +
      "        \"volumeName\": \"vol1\",\n" +
      "        \"volumeId\": 0,\n" +
      "        \"bucketName\": \"bucket1\",\n" +
      "        \"bucketId\": 0,\n" +
      "        \"keyName\": \"key3\",\n" +
      "        \"parentId\": 0\n" +
      "      }\n" +
      "    ]\n" +
      "  }\n" +
      "}\n";

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

    cmd = new CommandLine(new RDBParser())
        .addSubcommand(new ContainerKeyScanner())
        .setOut(pstdout)
        .setErr(pstderr);

    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(tempDir.toPath()).addTable(KEY_TABLE).addTable(FILE_TABLE)
        .addTable(DIRECTORY_TABLE)
        .build();
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
    pstderr.close();
    stderr.close();
    pstdout.close();
    stdout.close();
  }

  @Test
  void testWhenThereAreKeysForConatainerIds() throws IOException {

    // create keys for tables
    long volumeId = -123L;
    long bucketId = -456L;
    long dirObjectId = -789L;
    createDirectory(volumeId, bucketId, bucketId, dirObjectId, "dir1");
    createFile(volumeId, bucketId, "key1", -987L, dirObjectId, 1L);
    createKey("key2", 2L);
    createKey("key3", 3L);

    String[] cmdArgs =
        {"--db", dbStore.getDbLocation().getAbsolutePath(), "ckscanner",
            "-ids", "1,2,3"};

    int exitCode = cmd.execute(cmdArgs);
    Assertions.assertEquals(0, exitCode);

    Assertions.assertEquals(KEYS_FOUND_OUTPUT, stdout.toString());

    Assertions.assertTrue(stderr.toString().isEmpty());
  }

  @Test
  void testWhenThereAreNotKeysForConatainerIds() throws IOException {

    // create keys for tables
    long volumeId = -123L;
    long bucketId = -456L;
    createFile(volumeId, bucketId, "key1", -987L, bucketId, 4L);
    createKey("key2", 5L);
    createKey("key3", 6L);

    String[] cmdArgs =
        {"--db", dbStore.getDbLocation().getAbsolutePath(), "ckscanner",
            "-ids", "1,2,3"};

    int exitCode = cmd.execute(cmdArgs);
    Assertions.assertEquals(0, exitCode);

    Assertions.assertEquals(KEYS_NOT_FOUND_OUTPUT, stdout.toString());

    Assertions.assertTrue(stderr.toString().isEmpty());
  }

  private void createFile(long volumeId, long bucketId, String keyName,
                          long objectId, long parentId, long containerId)
      throws IOException {
    Table<byte[], byte[]> table = dbStore.getTable(FILE_TABLE);

    // format: /volumeId/bucketId/parentId(bucketId)/keyName
    String key =
        "/" + volumeId + "/" + bucketId + "/" + parentId + "/" + keyName;

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
    String key = "/" + volumeName + "/" + bucketName + "/" + keyName;

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
        "/" + volumeId + "/" + bucketId + "/" + parentId + "/" + keyName;

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
