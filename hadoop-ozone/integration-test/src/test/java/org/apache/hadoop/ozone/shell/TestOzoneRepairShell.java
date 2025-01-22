/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Ozone Repair shell.
 */
public class TestOzoneRepairShell {

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf = null;

  private static final String TRANSACTION_INFO_TABLE_TERM_INDEX_PATTERN = "([0-9]+#[0-9]+)";

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  @BeforeEach
  public void setup() throws Exception {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    IOUtils.closeQuietly(out, err);
  }

  @AfterAll
  static void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @Test
  public void testUpdateTransactionInfoTable() throws Exception {
    CommandLine cmd = new OzoneRepair().getCmd();
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    cluster.getOzoneManager().stop();

    String cmdOut = scanTransactionInfoTable(dbPath);
    String[] originalHighestTermIndex = parseScanOutput(cmdOut);

    String testTerm = "1111";
    String testIndex = "1111";
    int exitCode = withTextFromSystemIn("y")
        .execute(() -> cmd.execute("om", "update-transaction",
            "--db", dbPath,
            "--term", testTerm,
            "--index", testIndex));
    assertEquals(0, exitCode, err);
    assertThat(out.get())
        .contains(
            "The original highest transaction Info was " +
                String.format("(t:%s, i:%s)", originalHighestTermIndex[0], originalHighestTermIndex[1]),
            String.format("The highest transaction info has been updated to: (t:%s, i:%s)", testTerm, testIndex)
        );

    String cmdOut2 = scanTransactionInfoTable(dbPath);
    assertThat(cmdOut2).contains(testTerm + "#" + testIndex);

    withTextFromSystemIn("y")
        .execute(() -> cmd.execute("om", "update-transaction",
            "--db", dbPath,
            "--term", originalHighestTermIndex[0],
            "--index", originalHighestTermIndex[1]));
    cluster.getOzoneManager().restart();
    try (OzoneClient ozoneClient = cluster.newClient()) {
      ozoneClient.getObjectStore().createVolume("vol1");
    }
  }

  private String scanTransactionInfoTable(String dbPath) {
    CommandLine debugCmd = new OzoneDebug().getCmd();
    debugCmd.execute("ldb", "--db", dbPath, "scan", "--column_family", "transactionInfoTable");
    return out.get();
  }

  private String[] parseScanOutput(String output) {
    Pattern pattern = Pattern.compile(TRANSACTION_INFO_TABLE_TERM_INDEX_PATTERN);
    Matcher matcher = pattern.matcher(output);
    if (matcher.find()) {
      return matcher.group(1).split("#");
    }
    throw new IllegalStateException("Failed to scan and find raft's highest term and index from TransactionInfo table");
  }

  @Test
  public void testQuotaRepair() throws Exception {
    CommandLine cmd = new OzoneRepair().getCmd();

    int exitCode = cmd.execute("om", "quota", "status", "--service-host", conf.get(OZONE_OM_ADDRESS_KEY));
    assertEquals(0, exitCode, err);

    exitCode = withTextFromSystemIn("y")
        .execute(() -> cmd.execute("om", "quota", "start", "--service-host", conf.get(OZONE_OM_ADDRESS_KEY)));
    assertEquals(0, exitCode, err);

    GenericTestUtils.waitFor(() -> {
      out.reset();
      // verify quota trigger is completed having non-zero lastRunFinishedTime
      cmd.execute("om", "quota", "status", "--service-host", conf.get(OZONE_OM_ADDRESS_KEY));
      try {
        return out.get().contains("\"lastRunFinishedTime\":\"\"");
      } catch (Exception ex) {
        // do nothing
      }
      return false;
    }, 1000, 10000);
  }
}
