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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.event.Level;
import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;

/**
 * Tests failover with SCM HA setup.
 */
public class TestFailoverWithSCMHA {
  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private String scmServiceId;
  private int numOfOMs = 1;
  private int numOfSCMs = 3;

  private static final long SNAPSHOT_THRESHOLD = 5;

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
            SNAPSHOT_THRESHOLD);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId).setNumOfOzoneManagers(numOfOMs)
        .setNumOfStorageContainerManagers(numOfSCMs).setNumOfActiveSCMs(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFailover() throws Exception {
    SCMClientConfig scmClientConfig =
        conf.getObject(SCMClientConfig.class);
    scmClientConfig.setRetryCount(1);
    scmClientConfig.setRetryInterval(100);
    scmClientConfig.setMaxRetryTimeout(1500);
    Assert.assertEquals(scmClientConfig.getRetryCount(), 15);
    conf.setFromObject(scmClientConfig);
    StorageContainerManager scm = getLeader(cluster);
    Assert.assertNotNull(scm);
    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider);
    GenericTestUtils
        .setLogLevel(SCMBlockLocationFailoverProxyProvider.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
        .captureLogs(SCMBlockLocationFailoverProxyProvider.LOG);
    ScmBlockLocationProtocol scmBlockLocationProtocol = TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
    scmBlockLocationProtocol.getScmInfo();
    Assert.assertTrue(logCapture.getOutput()
        .contains("Performing failover to suggested leader"));
    scm = getLeader(cluster);
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    GenericTestUtils.setLogLevel(SCMContainerLocationFailoverProxyProvider.LOG,
        Level.DEBUG);
    logCapture = GenericTestUtils.LogCapturer
        .captureLogs(SCMContainerLocationFailoverProxyProvider.LOG);
    proxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);

    scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, "ozone");
    Assert.assertTrue(logCapture.getOutput()
        .contains("Performing failover to suggested leader"));
  }

  @Test
  public void testMoveFailover() throws Exception {
    SCMClientConfig scmClientConfig =
        conf.getObject(SCMClientConfig.class);
    scmClientConfig.setRetryCount(1);
    scmClientConfig.setRetryInterval(100);
    scmClientConfig.setMaxRetryTimeout(1500);
    Assert.assertEquals(scmClientConfig.getRetryCount(), 15);
    conf.setFromObject(scmClientConfig);
    StorageContainerManager scm = getLeader(cluster);
    Assert.assertNotNull(scm);

    final ContainerID id =
        getContainer(HddsProtos.LifeCycleState.CLOSED).containerID();
    DatanodeDetails dn1 = randomDatanodeDetails();
    DatanodeDetails dn2 = randomDatanodeDetails();

    //here we just want to test whether the new leader will get the same
    //inflight move after failover, so no need to create container and datanode,
    //just mock them bypassing all the pre checks.
    scm.getReplicationManager().getMoveScheduler().startMove(id.getProtobuf(),
        (new MoveDataNodePair(dn1, dn2)).getProtobufMessage(CURRENT_VERSION));

    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider);
    GenericTestUtils
        .setLogLevel(SCMBlockLocationFailoverProxyProvider.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
        .captureLogs(SCMBlockLocationFailoverProxyProvider.LOG);
    ScmBlockLocationProtocol scmBlockLocationProtocol = TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
    scmBlockLocationProtocol.getScmInfo();
    Assert.assertTrue(logCapture.getOutput()
        .contains("Performing failover to suggested leader"));
    scm = getLeader(cluster);
    Assert.assertNotNull(scm);

    //switch to the new leader successfully, new leader should
    //get the same inflightMove
    Map<ContainerID, MoveDataNodePair> inflightMove =
        scm.getReplicationManager().getMoveScheduler().getInflightMove();
    Assert.assertTrue(inflightMove.containsKey(id));
    MoveDataNodePair mp = inflightMove.get(id);
    Assert.assertTrue(dn2.equals(mp.getTgt()));
    Assert.assertTrue(dn1.equals(mp.getSrc()));

    //complete move in the new leader
    scm.getReplicationManager().getMoveScheduler()
        .completeMove(id.getProtobuf());


    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    GenericTestUtils.setLogLevel(SCMContainerLocationFailoverProxyProvider.LOG,
        Level.DEBUG);
    logCapture = GenericTestUtils.LogCapturer
        .captureLogs(SCMContainerLocationFailoverProxyProvider.LOG);
    proxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);

    scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, "ozone");
    Assert.assertTrue(logCapture.getOutput()
        .contains("Performing failover to suggested leader"));

    //switch to the new leader successfully, new leader should
    //get the same inflightMove , which should not contains
    //that container.
    scm = getLeader(cluster);
    Assert.assertNotNull(scm);
    inflightMove = scm.getReplicationManager()
        .getMoveScheduler().getInflightMove();
    Assert.assertFalse(inflightMove.containsKey(id));
  }

  static StorageContainerManager getLeader(MiniOzoneHAClusterImpl impl) {
    for (StorageContainerManager scm : impl.getStorageContainerManagers()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }
}
