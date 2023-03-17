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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class tests the Debug LDB CLI that reads from rocks db file.
 */
public class TestLDBCli {
  private static final String KEY_TABLE = "keyTable";
  private static final String BLOCK_DATA_TABLE = "block_data";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Gson gson = new Gson();
  private OzoneConfiguration conf;
  private DBStore dbStore = null;
  @TempDir
  private File newFolder;
  private StringWriter stdout, stderr;
  private CommandLine cmd;
  private NavigableMap<String, Map<String, ?>> expectedMap;

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    stdout = new StringWriter();
    stderr = new StringWriter();

    cmd = new CommandLine(new RDBParser())
        .addSubcommand(new DBScanner())
        .setOut(new PrintWriter(stdout))
        .setErr(new PrintWriter(stderr));

    expectedMap = new TreeMap<>();
  }

  private void prepareTable(String tableName, boolean schemaV3)
      throws IOException {

    switch (tableName) {
    case KEY_TABLE:
      // Dummy om.db with only keyTable
      dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
          .setPath(newFolder.toPath()).addTable(KEY_TABLE).build();

      Table<byte[], byte[]> keyTable = dbStore.getTable(KEY_TABLE);
      // Insert 5 keys
      for (int i = 1; i <= 5; i++) {
        String key = "key" + i;
        OmKeyInfo value = OMRequestTestUtils.createOmKeyInfo("vol1", "buck1",
            key, HddsProtos.ReplicationType.STAND_ALONE,
            HddsProtos.ReplicationFactor.ONE);
        keyTable.put(key.getBytes(UTF_8),
            value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());

        // Populate map
        expectedMap.put(key, toMap(value));
      }
      break;

    case BLOCK_DATA_TABLE:
      conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
          schemaV3);
      dbStore = BlockUtils.getUncachedDatanodeStore(
          newFolder.getAbsolutePath() + "/" + OzoneConsts.CONTAINER_DB_NAME,
          schemaV3 ? OzoneConsts.SCHEMA_V3 : OzoneConsts.SCHEMA_V2,
          conf, false).getStore();

      Table<byte[], byte[]> blockTable = dbStore.getTable(BLOCK_DATA_TABLE);
      // insert 2 containers, each with 2 blocks
      final int containerCount = 2;
      final int blockCount = 2;
      int blockId = 1;
      for (int cid = 1; cid <= containerCount; cid++) {
        for (int blockIdx = 1; blockIdx <= blockCount; blockIdx++, blockId++) {
          if (schemaV3) {
            String key = DatanodeSchemaThreeDBDefinition
                .getContainerKeyPrefix(cid) + blockId;
            BlockData blockData = new BlockData(new BlockID(cid, blockId));
            blockTable.put(FixedLengthStringUtils.string2Bytes(key),
                blockData.getProtoBufMessage().toByteArray());

            // Populate map. Schema V3 ldb output key is "containerId: blockId"
            String ldbKey = cid + ": " + blockId;
            expectedMap.put(ldbKey, toMap(blockData));
          } else {
            String key = String.valueOf(blockId);
            BlockData blockData = new BlockData(new BlockID(cid, blockId));
            blockTable.put(StringUtils.string2Bytes(key),
                blockData.getProtoBufMessage().toByteArray());

            // Populate map. Schema V2 ldb output key is just blockId
            expectedMap.put(key, toMap(blockData));
          }
        }
      }
      break;

    default:
      throw new IllegalArgumentException("Unsupported table: " + tableName);
    }
  }

  private static Map<String, Object> toMap(Object obj) throws IOException {
    // Have to use Gson here since DBScanner uses Gson.
    // JsonUtils (ObjectMapper) would parse object differently.
    String json = gson.toJson(obj);
    return MAPPER.readValue(
        json, new TypeReference<Map<String, Object>>() { });
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  private void assertNoError(int exitCode) {
    Assertions.assertEquals(0, exitCode, stderr.toString());
  }

  private void assertContents(Map<String, ?> expected, String actualStr)
      throws IOException {
    // Parse actual output string into Map
    Map<Object, ? extends Map<Object, ?>> actualMap = MAPPER.readValue(
        actualStr, new TypeReference<Map<Object, Map<Object, ?>>>() { });

    Assertions.assertEquals(expected, actualMap);
  }

  @Test
  public void testDefault() throws IOException {
    prepareTable(KEY_TABLE, false);
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE);

    assertNoError(exitCode);
    assertContents(expectedMap, stdout.toString());
  }

  @Test
  public void testLength() throws IOException {
    prepareTable(KEY_TABLE, false);
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE,
        "--length", "1");

    assertNoError(exitCode);
    assertContents(expectedMap.headMap("key1", true), stdout.toString());
  }

  @Test
  public void testInvalidLength() throws IOException {
    prepareTable(KEY_TABLE, false);
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE,
        "--length", "0");

    Assertions.assertNotEquals(0, exitCode);
    Assertions.assertTrue(stderr.toString().contains(
        "IllegalArgumentException: List length should be a positive number"));
  }

  @Test
  public void testUnlimitedLength() throws IOException {
    prepareTable(KEY_TABLE, false);
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE,
        "--length", "-1");

    assertNoError(exitCode);
    assertContents(expectedMap, stdout.toString());
  }

  @Test
  public void testDNDBSchemaV3Default() throws IOException {
    prepareTable(BLOCK_DATA_TABLE, true);

    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", BLOCK_DATA_TABLE,
        "--dn-schema", "V3",
        "--length", "-1");

    assertNoError(exitCode);
    assertContents(expectedMap, stdout.toString());
  }

  @Test
  public void testDNDBSchemaV3LimitContainer1() throws IOException {
    prepareTable(BLOCK_DATA_TABLE, true);

    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", BLOCK_DATA_TABLE,
        "--dn-schema", "V3",
        "--container-id", "1",
        "--length", "2");

    assertNoError(exitCode);
    // Result should include "1: 1" and "1: 2"
    assertContents(expectedMap.headMap("1: 2", true), stdout.toString());
  }

  @Test
  public void testDNDBSchemaV3LimitContainer2() throws IOException {
    prepareTable(BLOCK_DATA_TABLE, true);

    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", BLOCK_DATA_TABLE,
        "--dn-schema", "V3",
        "--container-id", "2",
        "--length", "2");

    assertNoError(exitCode);
    // Result should include "2: 3" and "2: 4"
    assertContents(expectedMap.tailMap("2: 3", true), stdout.toString());
  }

  @Test
  public void testDNDBSchemaV2() throws IOException {
    prepareTable(BLOCK_DATA_TABLE, false);

    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", BLOCK_DATA_TABLE,
        "--dn-schema", "V2");

    assertNoError(exitCode);
    assertContents(expectedMap, stdout.toString());
  }
}
