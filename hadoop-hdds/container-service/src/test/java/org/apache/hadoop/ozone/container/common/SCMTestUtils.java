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
package org.apache.hadoop.ozone.container.common;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageContainerDatanodeProtocolService;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.ozone.test.GenericTestUtils;

import com.google.protobuf.BlockingService;
import org.mockito.Mockito;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.logging.log4j.util.StackLocatorUtil.getCallerClass;

/**
 * Test Endpoint class.
 */
public final class SCMTestUtils {
  /**
   * Never constructed.
   */
  private SCMTestUtils() {
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(Configuration conf,
      InetSocketAddress addr, Class<?>
      protocol, BlockingService instance, int handlerCount)
      throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    HddsServerUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }


  /**
   * Start Datanode RPC server.
   */
  public static RPC.Server startScmRpcServer(ConfigurationSource configuration,
      StorageContainerDatanodeProtocol server,
      InetSocketAddress rpcServerAddresss, int handlerCount) throws
      IOException {

    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(configuration);
    RPC.setProtocolEngine(hadoopConfig,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService scmDatanodeService =
        StorageContainerDatanodeProtocolService.
            newReflectiveBlockingService(
                new StorageContainerDatanodeProtocolServerSideTranslatorPB(
                    server, Mockito.mock(ProtocolMessageMetrics.class)));

    RPC.Server scmServer = startRpcServer(hadoopConfig, rpcServerAddresss,
        StorageContainerDatanodeProtocolPB.class, scmDatanodeService,
        handlerCount);

    scmServer.start();
    return scmServer;
  }

  public static InetSocketAddress getReuseableAddress() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      String addr = InetAddress.getLoopbackAddress().getHostAddress();
      return new InetSocketAddress(addr, port);
    }
  }

  public static OzoneConfiguration getConf() {
    String name = getCallerClass(2).getSimpleName()
        + "-" + randomAlphanumeric(10);
    File testDir = GenericTestUtils.getTestDir(name);
    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> FileUtils.deleteQuietly(testDir)));
    return getConf(testDir);
  }

  public static OzoneConfiguration getConf(File testDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        new File(testDir, "datanode").getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        new File(testDir, "metadata").getAbsolutePath());
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR,
        new File(testDir, "datanodeID").getAbsolutePath());
    conf.setClass(SpaceUsageCheckFactory.Conf.configKeyForClassName(),
        MockSpaceUsageCheckFactory.None.class,
        SpaceUsageCheckFactory.class);
    return conf;
  }

  public static OzoneConfiguration getOzoneConf() {
    return new OzoneConfiguration();
  }

  public static HddsProtos.ReplicationType getReplicationType(
      ConfigurationSource conf) {
    return isUseRatis(conf) ?
        HddsProtos.ReplicationType.RATIS :
        HddsProtos.ReplicationType.STAND_ALONE;
  }

  public static HddsProtos.ReplicationFactor getReplicationFactor(
      ConfigurationSource conf) {
    return isUseRatis(conf) ?
        HddsProtos.ReplicationFactor.THREE :
        HddsProtos.ReplicationFactor.ONE;
  }

  private static boolean isUseRatis(ConfigurationSource c) {
    return c.getBoolean(
        ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_DEFAULT);
  }

}
