/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ratis;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Test class for SCM Ratis Server.
 */
public class TestSCMRatisServer {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SCMRatisServer scmRatisServer;
  private StorageContainerManager scm;
  private String scmId;
  private SCMNodeDetails scmNodeDetails;
  private static final long LEADER_ELECTION_TIMEOUT = 500L;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    scmId = UUID.randomUUID().toString();
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = conf.getInt(
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
        ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    scmNodeDetails = new SCMNodeDetails.Builder()
        .setRatisPort(ratisPort)
        .setRpcAddress(rpcAddress)
        .setSCMNodeId(scmId)
        .setSCMServiceId(OzoneConsts.SCM_SERVICE_ID_DEFAULT)
        .build();

    // Standalone SCM Ratis server
    initSCM();
    scm = HddsTestUtils.getScm(conf);
    scm.start();
    scmRatisServer = SCMRatisServer.newSCMRatisServer(
        conf, scm, scmNodeDetails, Collections.EMPTY_LIST);
    scmRatisServer.start();
  }

  @After
  public void shutdown() {
    if (scmRatisServer != null) {
      scmRatisServer.stop();
    }
    if (scm != null) {
      scm.stop();
    }
  }

  @Test
  public void testStartSCMRatisServer() throws Exception {
    Assert.assertEquals("Ratis Server should be in running state",
        LifeCycle.State.RUNNING, scmRatisServer.getServerState());
  }

  @Test
  public void verifyRaftGroupIdGenerationWithCustomOmServiceId() throws
      Exception {
    String customScmServiceId = "scmIdCustom123";
    OzoneConfiguration newConf = new OzoneConfiguration();
    String newOmId = UUID.randomUUID().toString();
    String path = GenericTestUtils.getTempPath(newOmId);
    Path metaDirPath = Paths.get(path, "scm-meta");
    newConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    newConf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        LEADER_ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = 9873;
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    SCMNodeDetails nodeDetails = new SCMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setSCMNodeId(newOmId)
        .setSCMServiceId(customScmServiceId)
        .build();
    // Starts a single node Ratis server
    scmRatisServer.stop();
    SCMRatisServer newScmRatisServer = SCMRatisServer
        .newSCMRatisServer(newConf, scm, nodeDetails,
            Collections.emptyList());
    newScmRatisServer.start();

    UUID uuid = UUID.nameUUIDFromBytes(customScmServiceId.getBytes());
    RaftGroupId raftGroupId = newScmRatisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
    newScmRatisServer.stop();
  }

  private void initSCM() throws IOException {
    String clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();

    final String path = folder.newFolder().toString();
    Path scmPath = Paths.get(path, "scm-meta");
    Files.createDirectories(scmPath);
    conf.set(OZONE_METADATA_DIRS, scmPath.toString());
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
  }
}
