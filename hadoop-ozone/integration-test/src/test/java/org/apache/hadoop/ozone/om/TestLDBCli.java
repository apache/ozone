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
package org.apache.hadoop.ozone.om;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * This class tests the Debug LDB CLI that reads from rocks db file.
 */
public class TestLDBCli {
  private OzoneConfiguration conf;

  private RDBParser rdbParser;
  private DBScanner dbScanner;
  private DBStore dbStore = null;
  private List<String> keyNames;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    rdbParser = new RDBParser();
    dbScanner = new DBScanner();
    keyNames = new ArrayList<>();
  }

  @After
  public void shutdown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
    // Restore the static fields in DBScanner
    DBScanner.setContainerId(-1);
    DBScanner.setDnDBSchemaVersion("V2");
    DBScanner.setWithKey(false);
  }

  @Test
  public void testOMDB() throws Exception {
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    // Dummy om.db with only keyTable
    dbStore = DBStoreBuilder.newBuilder(conf)
      .setName("om.db")
      .setPath(newFolder.toPath())
      .addTable("keyTable")
      .build();
    // insert 5 keys
    for (int i = 0; i < 5; i++) {
      OmKeyInfo value = OMRequestTestUtils.createOmKeyInfo("sampleVol",
          "sampleBuck", "key" + (i + 1), HddsProtos.ReplicationType.STAND_ALONE,
          HddsProtos.ReplicationFactor.ONE);
      String key = "key" + (i + 1);
      Table<byte[], byte[]> keyTable = dbStore.getTable("keyTable");
      byte[] arr = value
          .getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray();
      keyTable.put(key.getBytes(UTF_8), arr);
    }
    rdbParser.setDbPath(dbStore.getDbLocation().getAbsolutePath());
    dbScanner.setParent(rdbParser);
    Assert.assertEquals(5, getKeyNames(dbScanner).size());
    Assert.assertTrue(getKeyNames(dbScanner).contains("key1"));
    Assert.assertTrue(getKeyNames(dbScanner).contains("key5"));
    Assert.assertFalse(getKeyNames(dbScanner).contains("key6"));

    DBScanner.setLimit(1);
    Assert.assertEquals(1, getKeyNames(dbScanner).size());

    DBScanner.setLimit(0);
    try {
      getKeyNames(dbScanner);
      Assert.fail("IllegalArgumentException is expected");
    }  catch (IllegalArgumentException e) {
      //ignore
    }

    // If set with -1, check if it dumps entire table data.
    DBScanner.setLimit(-1);
    Assert.assertEquals(5, getKeyNames(dbScanner).size());

    // Test dump to file.
    File tempFile = folder.newFolder();
    String outFile = tempFile.getAbsolutePath() + "keyTable"
        + LocalDateTime.now();
    BufferedReader bufferedReader = null;
    try {
      DBScanner.setLimit(-1);
      DBScanner.setFileName(outFile);
      keyNames = getKeyNames(dbScanner);
      Assert.assertEquals(5, keyNames.size());
      Assert.assertTrue(new File(outFile).exists());

      bufferedReader = new BufferedReader(
          new InputStreamReader(new FileInputStream(outFile), UTF_8));

      String readLine;
      int count = 0;

      while ((readLine = bufferedReader.readLine()) != null) {
        for (String keyName : keyNames) {
          if (readLine.contains(keyName)) {
            count++;
            break;
          }
        }
      }

      // As keyName will be in the file twice for each key.
      // Once in keyName and second time in fileName.

      // Sample key data.
      // {
      // ..
      // ..
      // "keyName": "key5",
      // "fileName": "key5",
      // ..
      // ..
      // }

      Assert.assertEquals("File does not have all keys",
          keyNames.size() * 2, count);
    } finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
      if (new File(outFile).exists()) {
        FileUtils.deleteQuietly(new File(outFile));
      }
    }
  }

  private List<String> getKeyNames(DBScanner scanner)
            throws Exception {
    keyNames.clear();
    scanner.setTableName("keyTable");
    scanner.call();
    Assert.assertFalse(scanner.getScannedObjects().isEmpty());
    for (Object o : scanner.getScannedObjects()) {
      OmKeyInfo keyInfo = (OmKeyInfo)o;
      keyNames.add(keyInfo.getKeyName());
    }
    return keyNames;
  }

  @Test
  public void testDNDBSchemaV3() throws Exception {
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }

    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, true);
    dbStore = BlockUtils.getUncachedDatanodeStore(newFolder.getAbsolutePath() +
            "/" + OzoneConsts.CONTAINER_DB_NAME, OzoneConsts.SCHEMA_V3, conf,
        false).getStore();

    // insert 2 containers, each with 2 blocks
    final int containerCount = 2;
    final int blockCount = 2;
    int blockId = 1;
    Table<byte[], byte[]> blockTable = dbStore.getTable("block_data");
    for (int i = 1; i <= containerCount; i++) {
      for (int j = 1; j <= blockCount; j++, blockId++) {
        String key =
            DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix(i) + blockId;
        BlockData blockData = new BlockData(new BlockID(i, blockId));
        blockTable.put(FixedLengthStringUtils.string2Bytes(key),
            blockData.getProtoBufMessage().toByteArray());
      }
    }

    rdbParser.setDbPath(dbStore.getDbLocation().getAbsolutePath());
    dbScanner.setParent(rdbParser);
    dbScanner.setTableName("block_data");
    DBScanner.setDnDBSchemaVersion("V3");
    DBScanner.setWithKey(true);

    // Scan all container
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      dbScanner.call();
      // Assert that output has info for container 2 block 4
      Assert.assertTrue(capture.getOutput().contains("2: 4"));
      // Assert that output has info for container 1 block 1
      Assert.assertTrue(capture.getOutput().contains("1: 1"));
    }

    // Scan container 1
    DBScanner.setContainerId(1);
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      dbScanner.call();
      // Assert that output doesn't have info for container 2 block 4
      Assert.assertFalse(capture.getOutput().contains("2: 4"));
      // Assert that output has info for container 1 block 1
      Assert.assertTrue(capture.getOutput().contains("1: 1"));
    }

    // Scan container 2
    DBScanner.setContainerId(2);
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      dbScanner.call();
      // Assert that output has info for container 2 block 4
      Assert.assertTrue(capture.getOutput().contains("2: 4"));
      // Assert that output doesn't have info for container 1 block 1
      Assert.assertFalse(capture.getOutput().contains("1: 1"));
    }
  }

  @Test
  public void testDNDBSchemaV2() throws Exception {
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }

    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);
    dbStore = BlockUtils.getUncachedDatanodeStore(newFolder.getAbsolutePath() +
            "/" + OzoneConsts.CONTAINER_DB_NAME, OzoneConsts.SCHEMA_V2, conf,
        false).getStore();

    // insert 1 containers with 2 blocks
    final long cid = 1;
    final int blockCount = 2;
    int blockId = 1;
    Table<byte[], byte[]> blockTable = dbStore.getTable("block_data");
    for (int j = 1; j <= blockCount; j++, blockId++) {
      String key = String.valueOf(blockId);
      BlockData blockData = new BlockData(new BlockID(cid, blockId));
      blockTable.put(StringUtils.string2Bytes(key),
          blockData.getProtoBufMessage().toByteArray());
    }

    rdbParser.setDbPath(dbStore.getDbLocation().getAbsolutePath());
    dbScanner.setParent(rdbParser);
    dbScanner.setTableName("block_data");
    DBScanner.setDnDBSchemaVersion("V2");
    DBScanner.setWithKey(true);

    // Scan all container
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      dbScanner.call();
      // Assert that output has info for block 2
      Assert.assertTrue(capture.getOutput().contains("2"));
    }
  }
}
