/**
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
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.repair.RDBRepair;
import org.apache.hadoop.ozone.repair.om.TransactionInfoRepair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Ozone Repair shell.
 */
public class TestOzoneRepairShell {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf = null;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        conf.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testUpdateTransactionInfoTable() throws Exception {
    CommandLine cmd = new CommandLine(new RDBRepair()).addSubcommand(new TransactionInfoRepair());
    String dbPath = OMStorage.getOmDbDir(conf) + OM_KEY_PREFIX + OM_DB_NAME;

    cluster.getOzoneManager().stop();

    String testTermIndex = "1111#1111";
    String[] args =
        new String[] {"--db=" + dbPath, "transaction", "--highest-transaction", testTermIndex};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine cmdDBScanner = new CommandLine(new RDBParser()).addSubcommand(new DBScanner()).setOut(pstdout);
    String[] argsDBScanner =
        new String[] {"--db=" + dbPath, "scan", "--column_family", "transactionInfoTable"};
    cmdDBScanner.execute(argsDBScanner);
    String cmdOut = stdout.toString();
    assertThat(cmdOut).contains(testTermIndex);
  }

}
