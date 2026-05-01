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

package org.apache.hadoop.ozone.debug.ldb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Tests for the ozone debug ldb drop-column-family command.
 */
public class TestDropColumnFamily {

  private static final String KEY_TABLE = "keyTable";
  private static final String PIPELINES = "pipelines";

  @TempDir
  private File tempDir;

  private DBStore dbStore;
  private StringWriter stdout;
  private StringWriter stderr;
  private PrintWriter pstdout;
  private PrintWriter pstderr;
  private CommandLine cmd;

  @BeforeEach
  public void setup() {
    stdout = new StringWriter();
    stderr = new StringWriter();
    pstdout = new PrintWriter(stdout);
    pstderr = new PrintWriter(stderr);
    cmd = new CommandLine(new RDBParser())
        .setOut(pstdout)
        .setErr(pstderr);
  }

  @AfterEach
  public void teardown() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
    pstderr.close();
    stderr.close();
    pstdout.close();
    stdout.close();
  }

  @Test
  public void dropsColumnFamily() throws Exception {
    File dbLocation = createDb(KEY_TABLE, PIPELINES);

    int exitCode = cmd.execute(
        "--db", dbLocation.getAbsolutePath(),
        "drop-column-family",
        "--column-family", KEY_TABLE,
        "-y");

    assertEquals(0, exitCode, stderr.toString());
    assertThat(stderr.toString()).doesNotContain("WARNING:");
    assertThat(stdout.toString())
        .contains("Dropped column family " + KEY_TABLE);
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", PIPELINES)
        .doesNotContain(KEY_TABLE);
  }

  @Test
  public void failsForMissingColumnFamily() throws Exception {
    File dbLocation = createDb(KEY_TABLE);

    int exitCode = cmd.execute(
        "--db", dbLocation.getAbsolutePath(),
        "drop-column-family",
        "--column-family", PIPELINES,
        "-y");

    assertEquals(1, exitCode);
    assertThat(stderr.toString())
        .contains(PIPELINES
            + " is not a column family in DB for the given path.");
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", KEY_TABLE);
  }

  @Test
  public void failsForDefaultColumnFamily() throws Exception {
    File dbLocation = createDb(KEY_TABLE);

    int exitCode = cmd.execute(
        "--db", dbLocation.getAbsolutePath(),
        "drop-column-family",
        "--column-family", "default");

    assertEquals(1, exitCode);
    assertThat(stderr.toString())
        .contains("Default column family cannot be dropped.");
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", KEY_TABLE);
  }

  @Test
  public void dropsColumnFamilyAfterConfirmation() throws Exception {
    File dbLocation = createDb(KEY_TABLE, PIPELINES);

    int exitCode = withTextFromSystemIn("y")
        .execute(() -> cmd.execute(
            "--db", dbLocation.getAbsolutePath(),
            "drop-column-family",
            "--column-family", KEY_TABLE));

    assertEquals(0, exitCode, stderr.toString());
    assertThat(stderr.toString())
        .contains("WARNING: Dropping a column family mutates RocksDB");
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", PIPELINES)
        .doesNotContain(KEY_TABLE);
  }

  @Test
  public void abortsDropColumnFamilyByDefault() throws Exception {
    File dbLocation = createDb(KEY_TABLE, PIPELINES);

    int exitCode = withTextFromSystemIn("n")
        .execute(() -> cmd.execute(
            "--db", dbLocation.getAbsolutePath(),
            "drop-column-family",
            "--column-family", KEY_TABLE));

    assertEquals(1, exitCode);
    assertThat(stderr.toString())
        .contains("WARNING: Dropping a column family mutates RocksDB")
        .contains("Aborting command.");
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", KEY_TABLE, PIPELINES);
  }

  @Test
  public void scanCountWorksWhenDbIsOpen() throws Exception {
    File dbLocation = createOpenDb(KEY_TABLE);

    int exitCode = cmd.execute(
        "--db", dbLocation.getAbsolutePath(),
        "scan",
        "--column-family", KEY_TABLE,
        "--count");

    assertEquals(0, exitCode, stderr.toString());
    assertThat(stdout.toString().trim()).isNotEmpty();
  }

  @Test
  public void dropColumnFamilyFailsWhenDbIsOpen() throws Exception {
    File dbLocation = createOpenDb(KEY_TABLE, PIPELINES);

    int exitCode = cmd.execute(
        "--db", dbLocation.getAbsolutePath(),
        "drop-column-family",
        "--column-family", KEY_TABLE,
        "-y");

    assertEquals(1, exitCode);
    assertThat(stderr.toString())
        .contains("Failed to drop column family " + KEY_TABLE);
    assertThat(listColumnFamilies(dbLocation))
        .contains("default", KEY_TABLE, PIPELINES);
  }

  private File createDb(String... columnFamilies) throws IOException {
    File dbLocation = createOpenDb(columnFamilies);
    dbStore.close();
    dbStore = null;
    return dbLocation;
  }

  private File createOpenDb(String... columnFamilies) throws IOException {
    DBStoreBuilder builder = DBStoreBuilder.newBuilder(new OzoneConfiguration())
        .setName("om.db")
        .setPath(tempDir.toPath());
    for (String columnFamily : columnFamilies) {
      builder.addTable(columnFamily);
    }

    dbStore = builder.build();
    return dbStore.getDbLocation();
  }

  private static List<String> listColumnFamilies(File dbLocation)
      throws Exception {
    List<String> columnFamilies = new ArrayList<>();
    for (byte[] columnFamily
        : RocksDatabase.listColumnFamiliesEmptyOptions(
            dbLocation.getAbsolutePath())) {
      columnFamilies.add(new String(columnFamily, UTF_8));
    }
    return columnFamilies;
  }
}
