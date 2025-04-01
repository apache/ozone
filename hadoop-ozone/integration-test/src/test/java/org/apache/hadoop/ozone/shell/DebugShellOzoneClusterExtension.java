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

package org.apache.hadoop.ozone.shell;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;


public class DebugShellOzoneClusterExtension implements BeforeAllCallback, AfterAllCallback {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf = null;
  private static OzoneClient client;
  private static ObjectStore store = null;
  private static OzoneDebug ozoneDebugShell;
  private static OzoneShell ozoneShell;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (cluster != null) {
      return;
    }
    // Configure properties
    ozoneShell = new OzoneShell();
    ozoneDebugShell = new OzoneDebug();
    conf = ozoneDebugShell.getOzoneConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);
    startCluster();
  }

  static void startCluster() throws Exception {
    final int numDNs = 5;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDNs)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (context.getParent().isPresent()) {
      return;
    }
    IOUtils.closeQuietly(client, cluster);
    cluster = null;
  }

  public MiniOzoneCluster getCluster() {
    return cluster;
  }

  public OzoneConfiguration getConfiguration() {
    return conf;
  }

  public OzoneClient getClient() {
    return client;
  }

  public ObjectStore getStore() {
    return store;
  }

  public OzoneDebug getOzoneDebugShell() {
    return ozoneDebugShell;
  }

  public static OzoneShell getOzoneShell() {
    return ozoneShell;
  }
}
