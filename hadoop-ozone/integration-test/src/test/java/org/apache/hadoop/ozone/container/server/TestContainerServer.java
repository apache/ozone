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

package org.apache.hadoop.ozone.container.server;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test Containers.
 */
public class TestContainerServer {
  @TempDir
  private static Path testDir;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static CertificateClient caClient;
  private static VolumeChoosingPolicy volumeChoosingPolicy;
  @TempDir
  private Path tempDir;

  @BeforeAll
  public static void setup() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    CONF.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, testDir.toString());
    CONF.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    caClient = new DNCertificateClient(new SecurityConfig(CONF), null,
        dn, null, null, null);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(CONF);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    caClient.close();
  }

  @Test
  public void testClientServer() throws Exception {
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    runTestClientServer(1, (pipeline, conf) -> conf
            .setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode()
                    .getStandalonePort().getValue()),
        XceiverClientGrpc::new,
        (dn, conf) -> new XceiverServerGrpc(datanodeDetails, conf,
            new TestContainerDispatcher(), caClient), (dn, p) -> {
        });
  }

  @Test
  public void testClientServerRatisGrpc() throws Exception {
    runTestClientServerRatis(GRPC, 1);
    runTestClientServerRatis(GRPC, 3);
  }

  static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT,
        dn.getRatisPort().getValue());
    final String dir = testDir.resolve(dn.getUuid().toString()).toString();
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);

    final ContainerDispatcher dispatcher = new TestContainerDispatcher();
    return XceiverServerRatis.newXceiverServerRatis(null, dn, conf, dispatcher,
        new ContainerController(newContainerSet(), Maps.newHashMap()),
        caClient, null);
  }

  static void runTestClientServerRatis(RpcType rpc, int numNodes)
      throws Exception {
    runTestClientServer(numNodes,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(rpc, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestContainerServer::newXceiverServerRatis,
        (dn, p) -> RatisTestHelper.initXceiverServerRatis(rpc, dn, p));
  }

  static void runTestClientServer(
      int numDatanodes,
      CheckedBiConsumer<Pipeline, OzoneConfiguration, IOException> initConf,
      CheckedBiFunction<Pipeline, OzoneConfiguration, XceiverClientSpi,
                IOException> createClient,
      CheckedBiFunction<DatanodeDetails, OzoneConfiguration, XceiverServerSpi,
          IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    XceiverClientSpi client = null;
    try {
      final Pipeline pipeline =
          MockPipeline.createPipeline(numDatanodes);
      initConf.accept(pipeline, CONF);

      for (DatanodeDetails dn : pipeline.getNodes()) {
        final XceiverServerSpi s = createServer.apply(dn, CONF);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, CONF);
      client.connect();

      final ContainerCommandRequestProto request =
          ContainerTestHelper
              .getCreateContainerRequest(
                  ContainerTestHelper.getTestContainerID(), pipeline);
      assertNotNull(request.getTraceID());

      client.sendCommand(request);
    } finally {
      if (client != null) {
        client.close();
      }
      servers.forEach(XceiverServerSpi::stop);
    }
  }

  private HddsDispatcher createDispatcher(DatanodeDetails dd, UUID scmId,
                                                 OzoneConfiguration conf)
      throws IOException {
    ContainerSet containerSet = newContainerSet();
    conf.set(HDDS_DATANODE_DIR_KEY,
        Paths.get(testDir.toString(), "dfs", "data", "hdds",
            RandomStringUtils.secure().nextAlphabetic(4)).toString());
    conf.set(OZONE_METADATA_DIRS, testDir.toString());
    VolumeSet volumeSet = new MutableVolumeSet(dd.getUuidString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    StateContext context = ContainerTestUtils.getMockContext(dd, conf);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              dd.getUuid().toString(),
              containerSet, volumeSet, volumeChoosingPolicy, metrics,
              c -> { }, new ContainerChecksumTreeManager(conf)));
    }
    HddsDispatcher hddsDispatcher = new HddsDispatcher(
        conf, containerSet, volumeSet, handlers, context, metrics, null);
    hddsDispatcher.setClusterId(scmId.toString());
    return hddsDispatcher;
  }

  @Test
  public void testClientServerWithContainerDispatcher() throws Exception {
    DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
    HddsDispatcher hddsDispatcher = createDispatcher(dd,
        UUID.randomUUID(), CONF);
    runTestClientServer(1, (pipeline, conf) -> conf
            .setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode().getStandalonePort().getValue()),
        XceiverClientGrpc::new,
        (dn, conf) -> new XceiverServerGrpc(dd, conf,
            hddsDispatcher, caClient), (dn, p) -> {
        });
  }

  private static class TestContainerDispatcher implements ContainerDispatcher {
    /**
     * Dispatches commands to container layer.
     *
     * @param msg - Command Request
     * @return Command Response
     */
    @Override
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg,
        DispatcherContext context) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void init() {
    }

    @Override
    public void validateContainerCommand(
        ContainerCommandRequestProto msg) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Handler getHandler(ContainerProtos.ContainerType containerType) {
      return null;
    }

    @Override
    public void setClusterId(String scmId) {

    }

    @Override
    public void buildMissingContainerSetAndValidate(
        Map<Long, Long> container2BCSIDMap) {
    }
  }
}
