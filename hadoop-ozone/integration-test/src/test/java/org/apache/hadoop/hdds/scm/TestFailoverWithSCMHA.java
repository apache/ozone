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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBalancerConfigurationProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.IllegalContainerBalancerStateException;
import org.apache.hadoop.hdds.scm.container.balancer.InvalidContainerBalancerConfigurationException;
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
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Tests failover with SCM HA setup.
 */
public class TestFailoverWithSCMHA {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String SCM_SERVICE_ID = "scm-service-test1";
  private static final int NUM_OF_OMS = 1;
  private static final int NUM_OF_SCMS = 3;

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;

  private static final long SNAPSHOT_THRESHOLD = 5;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
            SNAPSHOT_THRESHOLD);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID).setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS).setNumOfActiveSCMs(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

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
    assertEquals(15, scmClientConfig.getRetryCount());
    conf.setFromObject(scmClientConfig);
    StorageContainerManager scm = getLeader(cluster);
    assertNotNull(scm);
    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider, conf);
    GenericTestUtils
        .setLogLevel(SCMBlockLocationFailoverProxyProvider.class, Level.DEBUG);
    LogCapturer logCapture = LogCapturer.captureLogs(SCMBlockLocationFailoverProxyProvider.class);
    ScmBlockLocationProtocol scmBlockLocationProtocol = TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
    scmBlockLocationProtocol.getScmInfo();
    assertThat(logCapture.getOutput())
        .contains("Performing failover to suggested leader");
    scm = getLeader(cluster);
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    GenericTestUtils.setLogLevel(SCMContainerLocationFailoverProxyProvider.class,
        Level.DEBUG);
    logCapture = LogCapturer.captureLogs(SCMContainerLocationFailoverProxyProvider.class);
    proxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);

    scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, "ozone");
    assertThat(logCapture.getOutput())
        .contains("Performing failover to suggested leader");
  }

  /**
   * Starts ContainerBalancer when the cluster is already balanced.
   * ContainerBalancer will identify that no unbalanced nodes are present and
   * exit and stop in the first iteration. We test that ContainerBalancer
   * persists ContainerBalancerConfigurationProto#shouldRun as false in all
   * the 3 SCMs when it stops.
   * @throws IOException
   * @throws IllegalContainerBalancerStateException
   * @throws InvalidContainerBalancerConfigurationException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testContainerBalancerPersistsConfigurationInAllSCMs()
      throws IOException, IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException, InterruptedException,
      TimeoutException {
    SCMClientConfig scmClientConfig =
        conf.getObject(SCMClientConfig.class);
    scmClientConfig.setRetryInterval(100);
    scmClientConfig.setMaxRetryTimeout(1500);
    assertEquals(15, scmClientConfig.getRetryCount());
    conf.setFromObject(scmClientConfig);
    StorageContainerManager leader = getLeader(cluster);
    assertNotNull(leader);

    ScmClient scmClient = new ContainerOperationClient(conf);
    // assert that container balancer is not running right now
    assertFalse(scmClient.getContainerBalancerStatus());
    ContainerBalancerConfiguration balancerConf =
        conf.getObject(ContainerBalancerConfiguration.class);
    ContainerBalancer containerBalancer = leader.getContainerBalancer();

    /*
    Start container balancer. Since this cluster is already balanced,
    container balancer should exit early, stop, and persist configuration to DB.
     */
    containerBalancer.startBalancer(balancerConf);

    // assert that balancer has stopped since the cluster is already balanced
    GenericTestUtils.waitFor(() -> !containerBalancer.isBalancerRunning(),
        10, 500);
    assertFalse(containerBalancer.isBalancerRunning());

    ByteString byteString =
        leader.getScmMetadataStore().getStatefulServiceConfigTable().get(
            containerBalancer.getServiceName());
    ContainerBalancerConfigurationProto proto =
        ContainerBalancerConfigurationProto.parseFrom(byteString);
    GenericTestUtils.waitFor(() -> !proto.getShouldRun(), 5, 50);

    long leaderTermIndex =
        leader.getScmHAManager().getRatisServer().getSCMStateMachine()
        .getLastAppliedTermIndex().getIndex();

    /*
    Fetch persisted configuration to verify that `shouldRun` is set to false.
     */
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (!scm.checkLeader()) {
        // Wait and retry for follower to update transactions to leader
        // snapshot index.
        // Timeout error if follower does not load update within 3s
        GenericTestUtils.waitFor(() -> scm.getScmHAManager().getRatisServer()
            .getSCMStateMachine().getLastAppliedTermIndex()
            .getIndex() >= leaderTermIndex, 100, 3000);
        ContainerBalancer followerBalancer = scm.getContainerBalancer();
        GenericTestUtils.waitFor(
            () -> !followerBalancer.isBalancerRunning(), 50, 5000);
        GenericTestUtils.waitFor(() -> !followerBalancer.shouldRun(), 100,
            5000);
      }
      scm.getStatefulServiceStateManager().readConfiguration(
          containerBalancer.getServiceName());
      byteString =
          scm.getScmMetadataStore().getStatefulServiceConfigTable().get(
              containerBalancer.getServiceName());
      ContainerBalancerConfigurationProto protobuf =
          ContainerBalancerConfigurationProto.parseFrom(byteString);
      assertFalse(protobuf.getShouldRun());
    }
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
