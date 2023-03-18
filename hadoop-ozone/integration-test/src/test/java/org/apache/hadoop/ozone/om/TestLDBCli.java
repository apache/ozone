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
import org.apache.commons.lang3.tuple.Pair;
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
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

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
  private File tempDir;
  private StringWriter stdout, stderr;
  private CommandLine cmd;
  private NavigableMap<String, Map<String, ?>> dbMap;

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    stdout = new StringWriter();
    stderr = new StringWriter();

    cmd = new CommandLine(new RDBParser())
        .addSubcommand(new DBScanner())
        .setOut(new PrintWriter(stdout))
        .setErr(new PrintWriter(stderr));

    dbMap = new TreeMap<>();
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  private void prepareTable(String tableName, boolean schemaV3)
      throws IOException {

    switch (tableName) {
    case KEY_TABLE:
      // Dummy om.db with only keyTable
      dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
          .setPath(tempDir.toPath()).addTable(KEY_TABLE).build();

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
        dbMap.put(key, toMap(value));
      }
      break;

    case BLOCK_DATA_TABLE:
      conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
          schemaV3);
      dbStore = BlockUtils.getUncachedDatanodeStore(
          tempDir.getAbsolutePath() + "/" + OzoneConsts.CONTAINER_DB_NAME,
          schemaV3 ? OzoneConsts.SCHEMA_V3 : OzoneConsts.SCHEMA_V2,
          conf, false).getStore();

      Table<byte[], byte[]> blockTable = dbStore.getTable(BLOCK_DATA_TABLE);
      // Insert 2 containers with 2 blocks each
      final int containerCount = 2;
      final int blockCount = 2;
      int blockId = 1;
      for (int cid = 1; cid <= containerCount; cid++) {
        for (int blockIdx = 1; blockIdx <= blockCount; blockIdx++, blockId++) {
          byte[] dbKey;
          String mapKey;
          BlockData blockData = new BlockData(new BlockID(cid, blockId));
          if (schemaV3) {
            String dbKeyStr = DatanodeSchemaThreeDBDefinition
                .getContainerKeyPrefix(cid) + blockId;
            dbKey = FixedLengthStringUtils.string2Bytes(dbKeyStr);
            // Schema V3 ldb scan output key is "containerId: blockId"
            mapKey = cid + ": " + blockId;
          } else {
            String dbKeyStr = String.valueOf(blockId);
            dbKey = StringUtils.string2Bytes(dbKeyStr);
            // Schema V2 ldb scan output key is "blockId"
            mapKey = dbKeyStr;
          }
          blockTable.put(dbKey, blockData.getProtoBufMessage().toByteArray());
          dbMap.put(mapKey, toMap(blockData));
        }
      }
      break;

    default:
      throw new IllegalArgumentException("Unsupported table: " + tableName);
    }
  }

  private static Map<String, Object> toMap(Object obj) throws IOException {
    // Have to use the same serializer (Gson) as DBScanner does.
    // JsonUtils (ObjectMapper) parses object differently.
    String json = gson.toJson(obj);
    return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
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

  /**
   * Defines ldb tool test cases.
   */
  private static Stream<Arguments> scanTestCases() {
    return Stream.of(
        Arguments.of(
            Named.of("Default", Pair.of(0, "")),
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("No extra args", Collections.emptyList()),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of("Length", Pair.of(0, "")),
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Limit 1", Arrays.asList("--length", "1")),
            Named.of("Expect key1 only", Pair.of("key1", "key2"))
        ),
        Arguments.of(
            Named.of("InvalidLength", Pair.of(1, "IllegalArgumentException")),
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Limit 0", Arrays.asList("--length", "0")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of("UnlimitedLength", Pair.of(0, "")),
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Limit -1", Arrays.asList("--length", "-1")),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of("Default", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, true)),
            Named.of("V3", Arrays.asList("--dn-schema", "V3")),
            Named.of("Expect '1: 1'-'2: 4'", Pair.of("1: 1", "3: 5"))
        ),
        Arguments.of(
            Named.of("ContainerID 1", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, true)),
            Named.of("V3, cid 1", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "1",
                "--length", "2")),
            Named.of("Expect '1: 1' and '1: 2'", Pair.of("1: 1", "2: 3"))
        ),
        Arguments.of(
            Named.of("ContainerID 2", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, true)),
            Named.of("V3, cid 2", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "2",
                "--length", "2")),
            Named.of("Expect '2: 3' and '2: 4'", Pair.of("2: 3", "3: 5"))
        ),
        Arguments.of(
            Named.of("ContainerID 2 Length", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, true)),
            Named.of("V3, cid 2, limit 1", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "2",
                "--length", "1")),
            Named.of("Expect '2: 3' only", Pair.of("2: 3", "2: 4"))
        ),
        Arguments.of(
            Named.of("Default", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, false)),
            Named.of("V2", Collections.emptyList()),
            Named.of("Expect 1-4", Pair.of("1", "5"))
        ),
        Arguments.of(
            Named.of("Explicit V2", Pair.of(0, "")),
            Named.of(BLOCK_DATA_TABLE, Pair.of(BLOCK_DATA_TABLE, false)),
            Named.of("V2", Arrays.asList("--dn-schema", "V2")),
            Named.of("Expect 1-4", Pair.of("1", "5"))
        )
    );
  }

  @ParameterizedTest
  @MethodSource("scanTestCases")
  void testLDBScan(Pair<Integer, String> expectedExitCodeStderrPair,
      Pair<String, Boolean> tableAndOption,
      List<String> scanArgs,
      Pair<String, String> dbMapRange) throws IOException {

    final String tableName = tableAndOption.getLeft();
    final Boolean schemaV3 = tableAndOption.getRight();
    // Prepare dummy table. Populate dbMap that contains expected results
    prepareTable(tableName, schemaV3);

    // Prepare scan args
    List<String> completeScanArgs = new ArrayList<>();
    completeScanArgs.addAll(Arrays.asList(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", tableName));
    completeScanArgs.addAll(scanArgs);

    int exitCode = cmd.execute(completeScanArgs.toArray(new String[0]));
    // Check exit code. Print stderr if not expected
    int expectedExitCode = expectedExitCodeStderrPair.getLeft();
    Assertions.assertEquals(expectedExitCode, exitCode, stderr.toString());

    // Construct expected result map given test param input
    Map<String, Map<String, ?>> expectedMap;
    if (dbMapRange != null) {
      expectedMap = dbMap.subMap(dbMapRange.getLeft(), dbMapRange.getRight());
    } else {
      expectedMap = new TreeMap<>();
    }

    // Verify result
    if (exitCode == 0) {
      assertContents(expectedMap, stdout.toString());
    } else {
      Assertions.assertEquals(0, stdout.toString().length());
    }

    // Check stderr
    final String stderrShouldContain = expectedExitCodeStderrPair.getRight();
    Assertions.assertTrue(stderr.toString().contains(stderrShouldContain));
  }

}
