/*
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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the Debug LDB CLI that reads from an om.db file.
 */
public class TestOmLDBCli {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String KEY_TABLE = "keyTable";

  private DBStore dbStore;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private CommandLine cmd;
  private StringWriter output;
  private StringWriter error;
  private NavigableMap<String, Map<String, Object>> keys;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    // Dummy om.db with only keyTable
    dbStore = DBStoreBuilder.newBuilder(conf)
        .setName("om.db")
        .setPath(newFolder.toPath())
        .addTable(KEY_TABLE)
        .build();

    // insert 5 keys
    keys = new TreeMap<>();
    for (int i = 0; i < 5; i++) {
      OmKeyInfo value = OMRequestTestUtils.createOmKeyInfo("sampleVol",
          "sampleBuck", "key" + (i + 1), HddsProtos.ReplicationType.STAND_ALONE,
          HddsProtos.ReplicationFactor.ONE);
      String key = "key" + (i);
      keys.put(key, toMap(value));
      Table<byte[], byte[]> keyTable = dbStore.getTable(KEY_TABLE);
      byte[] arr = value
          .getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray();
      keyTable.put(key.getBytes(UTF_8), arr);
    }

    output = new StringWriter();
    error = new StringWriter();
    cmd = new CommandLine(new RDBParser())
        .addSubcommand(new DBScanner())
        .setOut(new PrintWriter((output)))
        .setErr(new PrintWriter((error)));
  }

  @After
  public void shutdown() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  public void testDefaults() throws Exception {
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column_family", KEY_TABLE);

    assertNoError(exitCode);
    assertContents(output.toString(), keys);
  }

  @Test
  public void testLength() throws Exception {
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column_family", KEY_TABLE,
        "--length", "1");

    assertNoError(exitCode);
    assertContents(output.toString(), keys.headMap("key0", true));
  }

  @Test
  public void testInvalidLength() {
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column_family", KEY_TABLE,
        "--length", "0");

    assertNotEquals(0, exitCode);
    assertTrue(error.toString().contains("IllegalArgument"));
  }

  @Test
  public void testUnlimitedLength() throws Exception {
    // If set with -1, check if it dumps entire table data.
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column_family", KEY_TABLE,
        "--length", "-1");

    assertNoError(exitCode);
    assertContents(output.toString(), keys);
  }

  @Test
  public void testOutputToFile() throws IOException {
    File tempFile = folder.newFolder();
    String outFile = tempFile.getAbsolutePath() + KEY_TABLE
        + LocalDateTime.now();
    int exitCode = cmd.execute(
        "--db", dbStore.getDbLocation().getAbsolutePath(),
        "scan",
        "--column_family", KEY_TABLE,
        "--out", outFile,
        "--length", "-1");

    assertNoError(exitCode);
    File file = new File(outFile);
    assertTrue(file.exists());
    assertContents(FileUtils.readFileToString(file, UTF_8), keys);
  }

  private void assertNoError(int exitCode) {
    assertEquals(error.toString(), 0, exitCode);
  }

  private void assertContents(String content, Map<String, ?> expected)
      throws IOException {
    Map<String, ? extends Map<String, ?>> result = MAPPER.readValue(content,
        new TypeReference<Map<String, Map<String, ?>>>() { });

    assertEquals(expected, result);
  }

  private static Map<String, Object> toMap(OmKeyInfo obj) throws IOException {
    String json = JsonUtils.toJsonStringWithDefaultPrettyPrinter(obj);
    return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
  }

}
