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

package org.apache.hadoop.ozone.debug;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import jakarta.annotation.Nonnull;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.debug.ldb.DBScanner;
import org.apache.hadoop.ozone.debug.ldb.RDBParser;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

/**
 * This class tests `ozone debug ldb` CLI that reads from a RocksDB directory.
 */
public class TestLDBCli {
  private static final String KEY_TABLE = "keyTable";
  private static final String BLOCK_DATA = "block_data";
  public static final String PIPELINES = "pipelines";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private OzoneConfiguration conf;
  private DBStore dbStore;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;
  private NavigableMap<String, Map<String, ?>> dbMap;
  private String keySeparatorSchemaV3 =
      new OzoneConfiguration().getObject(DatanodeConfiguration.class)
          .getContainerSchemaV3KeySeparator();

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    stdout = new StringWriter();
    pstdout = new PrintWriter(stdout);
    stderr = new StringWriter();
    pstderr = new PrintWriter(stderr);

    cmd = new CommandLine(new RDBParser())
        .setOut(pstdout)
        .setErr(pstderr);

    dbMap = new TreeMap<>();
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

  /**
   * Defines ldb tool test cases.
   */
  @SuppressWarnings({"methodlength"})
  private static Stream<Arguments> scanTestCases() {
    return Stream.of(
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("No extra args", Collections.emptyList()),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Length", Pair.of(0, "")),
            Named.of("Limit 1", Arrays.asList("--length", "1")),
            Named.of("Expect key1 only", Pair.of("key1", "key2"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("InvalidLength", Pair.of(1, "IllegalArgumentException")),
            Named.of("Limit 0", Arrays.asList("--length", "0")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("UnlimitedLength", Pair.of(0, "")),
            Named.of("Limit -1", Arrays.asList("--length", "-1")),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("StartKey key3", Arrays.asList("--startkey", "key3")),
            Named.of("Expect key3-key5", Pair.of("key3", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("invalid StartKey key9", Arrays.asList("--startkey", "key9")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("EndKey key3", Arrays.asList("--endkey", "key3")),
            Named.of("Expect key1-key3", Pair.of("key1", "key4"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Invalid EndKey key9", Arrays.asList("--endkey", "key9")),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter key3", Arrays.asList("--filter", "keyName:equals:key3")),
            Named.of("Expect key3", Pair.of("key3", "key4"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter invalid key", Arrays.asList("--filter", "keyName:equals:key9")),
            Named.of("Expect key1-key3", null)
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter dataSize<2000", Arrays.asList("--filter", "dataSize:lesser:2000")),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter dataSize<500", Arrays.asList("--filter", "dataSize:lesser:500")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter dataSize>500", Arrays.asList("--filter", "dataSize:greater:500")),
            Named.of("Expect key1-key5", Pair.of("key1", "key6"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter dataSize>2000", Arrays.asList("--filter", "dataSize:greater:2000")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter key3 regex", Arrays.asList("--filter", "keyName:regex:^.*3$")),
            Named.of("Expect key3", Pair.of("key3", "key4"))
        ),
        Arguments.of(
            Named.of(KEY_TABLE, Pair.of(KEY_TABLE, false)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("Filter keys whose dataSize digits start with 5 using regex",
                Arrays.asList("--filter", "dataSize:regex:^5.*$")),
            Named.of("Expect empty result", null)
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V3", Pair.of(BLOCK_DATA, true)),
            Named.of("Default", Pair.of(0, "")),
            Named.of("V3", Arrays.asList("--dn-schema", "V3")),
            Named.of("Expect '1|1'-'2|4'", Pair.of("1|1", "3|5"))
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V3", Pair.of(BLOCK_DATA, true)),
            Named.of("Specify", Pair.of(0, "")),
            Named.of("V3", Collections.emptyList()),
            Named.of("Expect '1|1'-'2|4'", Pair.of("1|1", "3|5"))
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V3", Pair.of(BLOCK_DATA, true)),
            Named.of("ContainerID 1", Pair.of(0, "")),
            Named.of("V3 + cid 1", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "1",
                "--length", "2")),
            Named.of("Expect '1|1' and '1|2'", Pair.of("1|1", "2|3"))
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V3", Pair.of(BLOCK_DATA, true)),
            Named.of("ContainerID 2", Pair.of(0, "")),
            Named.of("V3 + cid 2", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "2",
                "--length", "2")),
            Named.of("Expect '2|3' and '2|4'", Pair.of("2|3", "3|5"))
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V3", Pair.of(BLOCK_DATA, true)),
            Named.of("ContainerID 2 + Length", Pair.of(0, "")),
            Named.of("V3 + cid 2 + limit 1", Arrays.asList(
                "--dn-schema", "V3",
                "--container-id", "2",
                "--length", "1")),
            Named.of("Expect '2|3' only", Pair.of("2|3", "2|4"))
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V2", Pair.of(BLOCK_DATA, false)),
            Named.of("Erroneously parse V2 table as V3",
                Pair.of(1, "Error: Invalid")),
            Named.of("Default to V3", Collections.emptyList()),
            Named.of("Expect exception", null)
        ),
        Arguments.of(
            Named.of(BLOCK_DATA + " V2", Pair.of(BLOCK_DATA, false)),
            Named.of("Explicit V2", Pair.of(0, "")),
            Named.of("V2", Arrays.asList("--dn-schema", "V2")),
            Named.of("Expect 1-4", Pair.of("1", "5"))
        )
    );
  }

  @ParameterizedTest
  @MethodSource("scanTestCases")
  void testLDBScan(
      @Nonnull Pair<String, Boolean> tableAndOption,
      @Nonnull Pair<Integer, String> expectedExitCodeStderrPair,
      List<String> scanArgs,
      Pair<String, String> dbMapRange) throws IOException {

    final String tableName = tableAndOption.getLeft();
    final Boolean schemaV3 = tableAndOption.getRight();
    // Prepare dummy table. Populate dbMap that contains expected results
    prepareTable(tableName, schemaV3);

    // Prepare scan args
    List<String> completeScanArgs = new ArrayList<>();
    completeScanArgs.addAll(Arrays.asList(
        "scan",
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "--column-family", tableName));
    completeScanArgs.addAll(scanArgs);

    int exitCode = cmd.execute(completeScanArgs.toArray(new String[0]));
    // Check exit code. Print stderr if not expected
    int expectedExitCode = expectedExitCodeStderrPair.getLeft();
    assertEquals(expectedExitCode, exitCode, stderr.toString());

    // Construct expected result map given test param input
    Map<String, Map<String, ?>> expectedMap;
    if (dbMapRange != null) {
      expectedMap = dbMap.subMap(dbMapRange.getLeft(), dbMapRange.getRight());
    } else {
      expectedMap = new TreeMap<>();
    }

    if (exitCode == 0) {
      // Verify stdout on success
      assertContents(expectedMap, stdout.toString());
    }

    // Check stderr
    final String stderrShouldContain = expectedExitCodeStderrPair.getRight();
    assertThat(stderr.toString()).contains(stderrShouldContain);
  }

  @Test
  void testScanOfPipelinesWhenNoData() throws IOException {
    // Prepare dummy table
    prepareTable(PIPELINES, false);

    // Prepare scan args
    List<String> completeScanArgs = new ArrayList<>(Arrays.asList(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", PIPELINES));

    int exitCode = cmd.execute(completeScanArgs.toArray(new String[0]));
    // Check exit code. Print stderr if not expected
    assertEquals(0, exitCode, stderr.toString());

    // Check stdout
    assertEquals("{  }\n", stdout.toString());

    // Check stderr
    assertEquals("", stderr.toString());
  }

  @Test
  void testScanWithRecordsPerFile() throws IOException {
    // Prepare dummy table
    int recordsCount = 5;
    prepareKeyTable(recordsCount);

    String scanDir1 = tempDir.getAbsolutePath() + "/scandir1";
    // Prepare scan args
    int maxRecordsPerFile = 2;
    List<String> completeScanArgs1 = new ArrayList<>(Arrays.asList(
        "scan",
        "--column-family", KEY_TABLE, "--out", scanDir1 + File.separator + "keytable",
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "--max-records-per-file", String.valueOf(maxRecordsPerFile)));
    File tmpDir1 = new File(scanDir1);
    tmpDir1.deleteOnExit();

    int exitCode1 = cmd.execute(completeScanArgs1.toArray(new String[0]));
    assertEquals(0, exitCode1);
    assertTrue(tmpDir1.isDirectory());
    File[] subFiles = tmpDir1.listFiles();
    assertNotNull(subFiles);
    assertEquals(Math.ceil(recordsCount / (maxRecordsPerFile * 1.0)), subFiles.length);
    for (File subFile : subFiles) {
      JsonNode jsonNode = MAPPER.readTree(subFile);
      assertNotNull(jsonNode);
    }

    String scanDir2 = tempDir.getAbsolutePath() + "/scandir2";
    // Used with parameter '-l'
    List<String> completeScanArgs2 = new ArrayList<>(Arrays.asList(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE, "--out", scanDir2 + File.separator + "keytable",
        "--max-records-per-file", String.valueOf(maxRecordsPerFile), "-l", "2"));
    File tmpDir2 = new File(scanDir2);
    tmpDir2.deleteOnExit();

    int exitCode2 = cmd.execute(completeScanArgs2.toArray(new String[0]));
    assertEquals(0, exitCode2);
    assertTrue(tmpDir2.isDirectory());
    assertEquals(1, tmpDir2.listFiles().length);
  }

  @Test
  void testSchemaCommand() throws IOException {
    // Prepare dummy table
    prepareTable(KEY_TABLE, false);

    // Prepare scan args
    List<String> completeScanArgs = new ArrayList<>(Arrays.asList(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "value-schema",
        "--column-family", KEY_TABLE));

    int exitCode = cmd.execute(completeScanArgs.toArray(new String[0]));
    // Check exit code. Print stderr if not expected
    assertEquals(0, exitCode, stderr.toString());

    // Check stdout
    Pattern p = Pattern.compile(".*keyName.*", Pattern.MULTILINE);
    Matcher m = p.matcher(stdout.toString());
    assertTrue(m.find());
    // Check stderr
    assertEquals("", stderr.toString());
  }

  /**
   * Converts String input to a Map and compares to the given Map input.
   * @param expected expected result Map
   * @param actualStr String input
   */
  private void assertContents(Map<String, ?> expected, String actualStr)
      throws IOException {
    // Parse actual output (String) into Map
    Map<Object, ? extends Map<Object, ?>> actualMap = MAPPER.readValue(
        actualStr, new TypeReference<Map<Object, Map<Object, ?>>>() { });

    assertEquals(expected, actualMap);
  }

  /**
   * Prepare the table for testing.
   * Also populate dbMap that contains all possibly expected results.
   * @param tableName table name
   * @param schemaV3 set to true for SchemaV3. applicable to block_data table
   */
  private void prepareTable(String tableName, boolean schemaV3)
      throws IOException {

    switch (tableName) {
    case KEY_TABLE:
      prepareKeyTable(5);
      break;

    case BLOCK_DATA:
      conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
          schemaV3);
      dbStore = BlockUtils.getUncachedDatanodeStore(
          tempDir.getAbsolutePath() + "/" + OzoneConsts.CONTAINER_DB_NAME,
          schemaV3 ? OzoneConsts.SCHEMA_V3 : OzoneConsts.SCHEMA_V2,
          conf, false).getStore();

      Table<byte[], byte[]> blockTable = dbStore.getTable(BLOCK_DATA);
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
            dbKey = FixedLengthStringCodec.string2Bytes(dbKeyStr);
            // Schema V3 ldb scan output key is "containerId: blockId"
            mapKey = cid + keySeparatorSchemaV3 + blockId;
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
    case PIPELINES:
      // Empty table
      dbStore = DBStoreBuilder.newBuilder(conf).setName("scm.db")
          .setPath(tempDir.toPath()).addTable(PIPELINES).build();
      break;
    default:
      throw new IllegalArgumentException("Unsupported table: " + tableName);
    }
  }

  /**
   * Prepare the keytable for testing.
   * @param recordsCount prepare the number of keys
   */
  private void prepareKeyTable(int recordsCount) throws IOException {
    if (recordsCount < 1) {
      throw new IllegalArgumentException("recordsCount must be greater than 1.");
    }
    // Dummy om.db with only keyTable
    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(tempDir.toPath()).addTable(KEY_TABLE).build();
    Table<byte[], byte[]> keyTable = dbStore.getTable(KEY_TABLE);
    for (int i = 1; i <= recordsCount; i++) {
      String key = "key" + i;
      OmKeyInfo value = OMRequestTestUtils.createOmKeyInfo("vol1", "buck1",
          key, ReplicationConfig.fromProtoTypeAndFactor(STAND_ALONE,
              HddsProtos.ReplicationFactor.ONE)).build();
      keyTable.put(key.getBytes(UTF_8), value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
      // Populate map
      dbMap.put(key, toMap(value));
    }
  }

  private static Map<String, Object> toMap(Object obj) throws IOException {
    ObjectWriter objectWriter = DBScanner.JsonSerializationHelper.getWriter();
    String json = objectWriter.writeValueAsString(obj);
    return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
  }

}
