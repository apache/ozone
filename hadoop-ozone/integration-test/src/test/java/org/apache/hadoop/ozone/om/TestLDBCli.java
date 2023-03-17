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


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringUtils;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.codec.OmKeyInfoCodec;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

/**
 * This class tests the Debug LDB CLI that reads from rocks db file.
 */
public class TestLDBCli {
  private OzoneConfiguration conf;

  private RDBParser rdbParser;
  private DBScanner dbScanner;
  private DBStore dbStore = null;
  private List<String> keyNames;
  private static final String DEFAULT_ENCODING = UTF_8.name();
  private static final String KEY_TABLE = "keyTable";
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private final StringWriter stdout = new StringWriter();
  private final StringWriter stderr = new StringWriter();
  private CommandLine cmd;
  private final Map<String, Map<String, ?>> expectedMap = new TreeMap<>();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Gson gson = new Gson();

  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    rdbParser = new RDBParser();
    dbScanner = new DBScanner();
    keyNames = new ArrayList<>();

    cmd = new CommandLine(rdbParser).addSubcommand(dbScanner)
        .setOut(new PrintWriter(stdout)).setErr(new PrintWriter(stderr));

    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    // Dummy om.db with only keyTable
    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(newFolder.toPath()).addTable(KEY_TABLE).build();

    // insert 5 keys
    for (int i = 0; i < 5; i++) {
      OmKeyInfo value =
          OMRequestTestUtils.createOmKeyInfo("sampleVol", "sampleBuck",
              "key" + (i + 1), HddsProtos.ReplicationType.STAND_ALONE,
              HddsProtos.ReplicationFactor.ONE);
      String key = "key" + (i + 1);
      byte[] keyBytes = key.getBytes(UTF_8);
      byte[] valBytes =
          value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray();
      Table<byte[], byte[]> keyTable = dbStore.getTable(KEY_TABLE);
      keyTable.put(keyBytes, valBytes);

      // Populate map
      String k = new StringCodec().fromPersistedFormat(keyBytes);
      OmKeyInfo v = new OmKeyInfoCodec(true).fromPersistedFormat(valBytes);
      expectedMap.put(k, toMap(v));
    }
  }

  private static Map<String, Object> toMap(OmKeyInfo obj) throws IOException {
    // Have to use Gson here since DBScanner uses Gson.
    // JsonUtils (ObjectMapper) would parse object differently.
    String json = gson.toJson(obj);
//    String json = JsonUtils.toJsonStringWithDefaultPrettyPrinter(obj);
    return MAPPER.readValue(
        json, new TypeReference<Map<String, Object>>() { });
  }

  @After
  public void shutdown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
    System.setOut(System.out);
    // Restore the static fields in DBScanner
    DBScanner.setContainerId(-1);
    DBScanner.setDnDBSchemaVersion("V2");
  }

  private void assertNoError(int exitCode) {
    Assertions.assertEquals(0, exitCode, stderr.toString());
  }

  private void assertContents(Map<String, ?> expected, String actualStr)
      throws IOException {
    // Parse actual output string into Map
    Map<String, ? extends Map<String, ?>> actualMap = MAPPER.readValue(
        actualStr, new TypeReference<Map<String, Map<String, ?>>>() { });

//    String expectedStr = new Gson().toJson(expectedMap);
//    String actualToStrAgain = new Gson().toJson(actual);

//    Assertions.assertEquals(expectedStr, actualToStrAgain);
    Assertions.assertEquals(expected, actualMap);
  }

  @Test
  public void testDefaultScan() throws IOException {
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE);

    assertNoError(exitCode);
    assertContents(expectedMap, stdout.toString());
  }

  @Test
  public void testOMDB() throws Exception {

    rdbParser.setDbPath(dbStore.getDbLocation().getAbsolutePath());
    dbScanner.setParent(rdbParser);
    DBScanner.setLimit(100);
    assertEquals(5, getKeyNames(dbScanner).size());
    Assert.assertTrue(getKeyNames(dbScanner).contains("key1"));
    Assert.assertTrue(getKeyNames(dbScanner).contains("key5"));
    Assert.assertFalse(getKeyNames(dbScanner).contains("key6"));

    final ByteArrayOutputStream outputStreamCaptor =
        new ByteArrayOutputStream();
    System.setOut(new PrintStream(outputStreamCaptor, false, DEFAULT_ENCODING));
    DBScanner.setShowCount(true);
    dbScanner.call();
    assertEquals("5",
        outputStreamCaptor.toString(DEFAULT_ENCODING).trim());
    System.setOut(System.out);
    DBScanner.setShowCount(false);


    DBScanner.setLimit(1);
    assertEquals(1, getKeyNames(dbScanner).size());

    // Testing the startKey function
    DBScanner.setLimit(-1);
    DBScanner.setStartKey("key3");
    assertEquals(3, getKeyNames(dbScanner).size());

    DBScanner.setLimit(0);
    try {
      getKeyNames(dbScanner);
      Assert.fail("IllegalArgumentException is expected");
    }  catch (IllegalArgumentException e) {
      //ignore
    }

    // If set with -1, check if it dumps entire table data.
    DBScanner.setLimit(-1);
    DBScanner.setStartKey(null);
    assertEquals(5, getKeyNames(dbScanner).size());

    // Test dump to file.
    File tempFile = folder.newFolder();
    String outFile = tempFile.getAbsolutePath() + "keyTable"
        + LocalDateTime.now();
    BufferedReader bufferedReader = null;
    try {
      DBScanner.setLimit(-1);
      DBScanner.setFileName(outFile);
      keyNames = getKeyNames(dbScanner);
      assertEquals(5, keyNames.size());
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

      assertEquals("File does not have all keys",
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
    return Collections.emptyList();
//    keyNames.clear();
//    scanner.setTableName("keyTable");
//    scanner.call();
//    Assert.assertFalse(scanner.getScannedObjects().isEmpty());
//    for (Object o : scanner.getScannedObjects()) {
//      OmKeyInfo keyInfo = (OmKeyInfo)o;
//      keyNames.add(keyInfo.getKeyName());
//    }
//    return keyNames;
  }

  @Test
  public void testDNDBSchemaV3() throws Exception {
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }

    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, true);
    dbStore = BlockUtils.getUncachedDatanodeStore(
        newFolder.getAbsolutePath() + "/" + OzoneConsts.CONTAINER_DB_NAME,
        OzoneConsts.SCHEMA_V3, conf, false).getStore();

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
    dbScanner.setWithKey(true);

    // Scan all container
    DBScanner.setLimit(-1);
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
    DBScanner.setLimit(2);
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
    DBScanner.setLimit(2);
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
    dbScanner.setWithKey(true);

    // Scan all container
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      dbScanner.call();
      // Assert that output has info for block 2
      Assert.assertTrue(capture.getOutput().contains("2"));
    }
  }
}
