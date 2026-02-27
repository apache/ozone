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

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getReconAddressForDatanodes;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getSCMAddressForDatanodes;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmHeartbeatInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;
import static org.apache.hadoop.hdds.utils.HddsVersionInfo.HDDS_VERSION_INFO;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * This command simulates a number of datanodes and coordinates with
 * SCM to create a number of containers on the said datanodes.
 * <p>
 * This tool is created to verify the SCM/Recon ability to handle thousands
 * datanodes and exabytes of container data.
 * <p>
 * Usage:
 * ozone freon simulate-datanode -t 20 -n 5000 -c 40000
 * -t: number of threads to run datanodes heartbeat.
 * -n: number of data node to simulate.
 * -c: number containers to simulate per datanode.
 * <p>
 * The simulation can be stopped and restored safely as datanode states,
 * including pipelines and containers, are saved to a file when the process
 * exits.
 * <p>
 * When the number containers exceeds the required one, all datanodes are
 * transitioned to readonly mode (all pipelines are closed).
 */
@CommandLine.Command(name = "simulate-datanode",
    description =
        "Simulate one or many datanodes and register them to SCM." +
            "This is used to stress test SCM handling a massive cluster.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class DatanodeSimulator implements Callable<Void>, FreonSubcommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      DatanodeSimulator.class);

  private Map<InetSocketAddress,
      StorageContainerDatanodeProtocolClientSideTranslatorPB> scmClients;
  private InetSocketAddress reconAddress;
  private StorageContainerDatanodeProtocolClientSideTranslatorPB reconClient;

  private ConfigurationSource conf;
  private List<DatanodeSimulationState> datanodes;
  private Map<UUID, DatanodeSimulationState> datanodesMap;

  private ScheduledExecutorService heartbeatScheduler;
  private LayoutVersionProto layoutInfo;

  @CommandLine.ParentCommand
  private Freon freonCommand;
  @CommandLine.Option(names = {"-t", "--threads"},
      description = "Size of the threadpool running heartbeat.",
      defaultValue = "10")
  private int threadCount = 10;
  @CommandLine.Option(names = {"-n", "--nodes"},
      description = "Number of simulated datanode instances.",
      defaultValue = "1")
  private int datanodesCount = 1;

  @CommandLine.Option(names = {"-c", "--containers"},
      description = "Number of simulated containers per datanode.",
      defaultValue = "5")
  private int containers = 1;

  @CommandLine.Option(names = {"-r", "--reload"},
      description = "Reload the datanodes created by previous simulation run.",
      defaultValue = "true")
  private boolean reload = true;

  private Random random = new Random();

  // stats
  private AtomicLong totalHeartbeats = new AtomicLong(0);
  private AtomicLong totalFCRs = new AtomicLong(0);
  private AtomicLong totalICRs = new AtomicLong(0);
  private StorageContainerLocationProtocol scmContainerClient;

  @Override
  public Void call() throws Exception {
    init();
    loadOrCreateDatanodes();

    // Register datanodes to SCM/Recon and schedule heartbeat for each.
    int successCount = 0;
    for (DatanodeSimulationState dn : datanodes) {
      successCount += startDatanode(dn) ? 1 : 0;
    }

    LOGGER.info("{} datanodes have been created and registered to SCM/Recon",
        successCount);

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          heartbeatScheduler.shutdown();
          try {
            heartbeatScheduler.awaitTermination(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          IOUtils.closeQuietly(scmClients.values());
          IOUtils.closeQuietly(reconClient);
          LOGGER.info("Successfully closed all the used resources");
          saveDatanodesToFile();
        })
    );

    backgroundStatsReporter();

    // Starts creating containers and add them to simulated datanodes state
    // as per SCM assignment.
    // When this simulation is done on a cluster with some real datanodes,
    // several containers will be assign to a mixture of read and simulated
    // datanodes, resulting some containers are recorded as under-replicated.
    // This should be fine though as those containers don't participate in any
    // real operations, and the simulation is expected to run with
    // a dominant number of simulated datanodes.
    LOGGER.info("Start growing containers.");
    try {
      growContainers();
    } catch (IOException e) {
      LOGGER.error("Error creating containers, exiting.", e);
      throw e;
    }

    // After reaching the number of expected containers, the simulated
    // datanodes are moved to read-only states to avoid participating in
    // the normal write operations that should only happen on real datanodes.
    LOGGER.info("Finished creating container, " +
        "transitioning datanodes to readonly");
    moveDatanodesToReadonly();
    LOGGER.info("All datanodes have been transitioned to read-only.");

    return null;
  }

  private void moveDatanodesToReadonly() {
    for (DatanodeSimulationState dn : datanodes) {
      dn.setReadOnly(true);
      for (String pipeline : dn.getPipelines()) {
        try {
          scmContainerClient.closePipeline(HddsProtos.PipelineID.newBuilder()
              .setId(pipeline).build());
        } catch (IOException e) {
          LOGGER.error("Error closing pipeline {}", pipeline, e);
        }
      }
    }
  }

  private void backgroundStatsReporter() {
    long interval = getScmHeartbeatInterval(conf);
    final AtomicLong lastTotalHeartbeats = new AtomicLong(0);
    final AtomicLong lastTotalFCRs = new AtomicLong(0);
    final AtomicLong lastTotalICRs = new AtomicLong(0);
    final AtomicReference<Instant> lastCheck =
        new AtomicReference<>(Instant.now());
    heartbeatScheduler.scheduleAtFixedRate(() -> {

      long heartbeats = totalHeartbeats.get() - lastTotalHeartbeats.get();
      lastTotalHeartbeats.set(totalHeartbeats.get());
      long fcrs = totalFCRs.get() - lastTotalFCRs.get();
      lastTotalFCRs.set(totalFCRs.get());

      long icrs = totalICRs.get() - lastTotalICRs.get();
      lastTotalICRs.set(totalICRs.get());

      long intervalInSeconds = Instant.now().getEpochSecond()
          - lastCheck.get().getEpochSecond();
      lastCheck.set(Instant.now());

      LOGGER.info("Heartbeat status: \n" +
              "Total heartbeat in cycle: {} ({} per second) \n" +
              "Total incremental reported in cycle: {} ({} per second) \n" +
              "Total full reported in cycle: {} ({} per second)",
          heartbeats, heartbeats / intervalInSeconds,
          icrs, icrs / intervalInSeconds,
          fcrs, fcrs / intervalInSeconds);
    }, interval, interval, TimeUnit.MILLISECONDS);
  }

  private void loadOrCreateDatanodes() throws UnknownHostException {
    List<InetSocketAddress> allEndpoints = new LinkedList<>(
        scmClients.keySet());
    allEndpoints.add(reconAddress);

    if (reload) {
      datanodes = loadDatanodesFromFile();
      for (DatanodeSimulationState datanode : datanodes) {
        datanode.initEndpointsState(allEndpoints);
      }
    } else {
      datanodes = new ArrayList<>(datanodesCount);
    }

    long containerReportInterval = conf.getTimeDuration(
        HDDS_CONTAINER_REPORT_INTERVAL,
        HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);


    for (int i = datanodes.size(); i < datanodesCount; i++) {
      datanodes.add(new DatanodeSimulationState(randomDatanodeDetails(conf),
          containerReportInterval, allEndpoints, containers));
    }

    datanodesMap = new HashMap<>();
    for (DatanodeSimulationState datanode : datanodes) {
      datanodesMap.put(datanode.getDatanodeDetails().getUuid(), datanode);
    }
  }

  /**
   * Calls SCM to allocate new containers and add the containers to datanodes
   * state according to the allocation response from SCM.
   */
  private void growContainers() throws IOException {
    int totalAssignedContainers = 0;
    for (DatanodeSimulationState datanode : datanodes) {
      totalAssignedContainers += datanode.getContainers().size();
    }

    int totalExpectedContainers = datanodesCount * containers;
    int totalCreatedContainers = 0;
    while (totalAssignedContainers < totalExpectedContainers) {
      ContainerWithPipeline cp =
          scmContainerClient.allocateContainer(ReplicationType.RATIS,
              ReplicationFactor.THREE, "test");

      for (DatanodeDetails datanode : cp.getPipeline().getNodeSet()) {
        if (datanodesMap.containsKey(datanode.getUuid())) {
          datanodesMap.get(datanode.getUuid())
              .newContainer(cp.getContainerInfo().getContainerID());
          totalAssignedContainers++;
        }
      }

      totalCreatedContainers++;
      // closed immediately.
      scmContainerClient.closeContainer(cp.getContainerInfo().getContainerID());
    }

    LOGGER.info("Finish assigning {} containers from {} created containers.",
        totalAssignedContainers, totalCreatedContainers);
  }

  private boolean startDatanode(DatanodeSimulationState dn)
      throws IOException {
    if (!registerDataNode(dn)) {
      LOGGER.info("Failed to register datanode to SCM: {}",
          dn.getDatanodeDetails());
      return false;
    }

    // Schedule heartbeat tasks for the given datanode to all SCMs/Recon.
    long scmHeartbeatInterval = HddsServerUtil.getScmHeartbeatInterval(conf);
    scmClients.forEach((endpoint, client) -> {
      // Use random initial delay as a jitter to avoid peaks.
      long initialDelay = RandomUtils.secure().randomLong(0, scmHeartbeatInterval);
      Runnable runnable = () -> heartbeat(endpoint, client, dn);
      heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
          scmHeartbeatInterval, TimeUnit.MILLISECONDS);
    });

    long reconHeartbeatInterval =
        HddsServerUtil.getReconHeartbeatInterval(conf);
    long initialDelay = RandomUtils.secure().randomLong(0, reconHeartbeatInterval);
    Runnable runnable = () -> heartbeat(reconAddress, reconClient, dn);
    heartbeatScheduler.scheduleAtFixedRate(runnable, initialDelay,
        reconHeartbeatInterval, TimeUnit.MILLISECONDS);

    LOGGER.info("Successfully registered datanode to SCM: {}",
        dn.getDatanodeDetails());
    return true;
  }

  void saveDatanodesToFile() {
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    File file = new File(metaDirPath, "datanode-simulation.json");
    try {
      JsonUtils.writeToFile(datanodes, file);
    } catch (IOException e) {
      throw new RuntimeException("Error saving datanodes to file.", e);
    }
    LOGGER.info("{} datanodes has been saved to {}", datanodes.size(),
        file.getAbsolutePath());
  }

  List<DatanodeSimulationState> loadDatanodesFromFile() {
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    File file = new File(metaDirPath, "datanode-simulation.json");
    if (!file.exists()) {
      LOGGER.info("File {} doesn't exists, nothing is loaded",
          file.getAbsolutePath());
      return new ArrayList<>();
    }
    try {
      return JsonUtils.readFromFile(file, DatanodeSimulationState.class);
    } catch (IOException e) {
      throw new RuntimeException("Error Reading datanodes from file.", e);
    }
  }

  private void heartbeat(InetSocketAddress endpoint,
                         StorageContainerDatanodeProtocol client,
                         DatanodeSimulationState dn) {
    try {
      SCMHeartbeatRequestProto heartbeat = dn.heartbeatRequest(endpoint,
          layoutInfo);
      SCMHeartbeatResponseProto response = client.sendHeartbeat(heartbeat);
      dn.ackHeartbeatResponse(response);

      totalHeartbeats.incrementAndGet();
      if (heartbeat.hasContainerReport()) {
        totalFCRs.incrementAndGet();
      } else {
        totalICRs.addAndGet(heartbeat.getIncrementalContainerReportCount());
      }

      // If SCM requests this datanode to register, issue a reregister
      // immediately.
      if (response.getCommandsList().stream()
          .anyMatch(x -> x.getCommandType() ==
              SCMCommandProto.Type.reregisterCommand)) {
        client.register(
            dn.getDatanodeDetails().getExtendedProtoBufMessage(),
            dn.createNodeReport(), dn.createFullContainerReport(),
            dn.createPipelineReport(), this.layoutInfo);
      }
    } catch (Exception e) {
      LOGGER.info("Error sending heartbeat for {}: {}",
          dn.getDatanodeDetails(), e.getMessage(), e);
    }
  }

  private void init() throws IOException {
    conf = freonCommand.getOzoneConf();
    Collection<InetSocketAddress> addresses = getSCMAddressForDatanodes(conf);
    scmClients = new HashMap<>(addresses.size());
    for (InetSocketAddress address : addresses) {
      scmClients.put(address, createScmClient(address));
    }

    reconAddress = getReconAddressForDatanodes(conf);
    reconClient = createReconClient(reconAddress);

    heartbeatScheduler = Executors.newScheduledThreadPool(threadCount);

    scmContainerClient = HAUtils.getScmContainerClient(conf);

    this.layoutInfo = createLayoutInfo();
  }

  private LayoutVersionProto createLayoutInfo() throws IOException {
    Storage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString());

    HDDSLayoutVersionManager layoutVersionManager =
        new HDDSLayoutVersionManager(layoutStorage.getLayoutVersion());

    return LayoutVersionProto.newBuilder()
        .setMetadataLayoutVersion(
            layoutVersionManager.getMetadataLayoutVersion())
        .setSoftwareLayoutVersion(
            layoutVersionManager.getSoftwareLayoutVersion())
        .build();
  }

  private DatanodeDetails randomDatanodeDetails(ConfigurationSource config)
      throws UnknownHostException {
    DatanodeDetails details = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .build();
    details.setInitialVersion(HDDSVersion.SOFTWARE_VERSION.serialize());
    details.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION.serialize());
    details.setHostName(HddsUtils.getHostName(config));
    details.setIpAddress(randomIp());
    details.setStandalonePort(0);
    details.setRatisPort(0);
    details.setRestPort(0);
    details.setVersion(HDDS_VERSION_INFO.getVersion());
    details.setSetupTime(Time.now());
    details.setRevision(HDDS_VERSION_INFO.getRevision());
    details.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION.serialize());
    return details;
  }

  private boolean registerDataNode(DatanodeSimulationState dn)
      throws IOException {

    ContainerReportsProto containerReports =
        ContainerReportsProto.newBuilder().build();

    NodeReportProto nodeReport = dn.createNodeReport();

    PipelineReportsProto pipelineReports = PipelineReportsProto
        .newBuilder().build();
    boolean isRegistered = false;

    for (StorageContainerDatanodeProtocol client : scmClients.values()) {
      try {
        SCMRegisteredResponseProto response =
            client.register(
                dn.getDatanodeDetails().getExtendedProtoBufMessage(),
                nodeReport, containerReports, pipelineReports, this.layoutInfo);
        if (response.hasHostname() && response.hasIpAddress()) {
          dn.getDatanodeDetails().setHostName(response.getHostname());
          dn.getDatanodeDetails().setIpAddress(response.getIpAddress());
        }
        if (response.hasNetworkName() && response.hasNetworkLocation()) {
          dn.getDatanodeDetails().setNetworkName(response.getNetworkName());
          dn.getDatanodeDetails()
              .setNetworkLocation(response.getNetworkLocation());
        }
        isRegistered = isRegistered ||
            (response.getErrorCode() ==
                SCMRegisteredResponseProto.ErrorCode.success);
      } catch (IOException e) {
        LOGGER.error("Error register datanode to SCM", e);
      }
    }

    try {
      reconClient.register(dn.getDatanodeDetails().getExtendedProtoBufMessage(),
          nodeReport, containerReports, pipelineReports, this.layoutInfo);
    } catch (IOException e) {
      LOGGER.error("Error register datanode to Recon", e);
    }

    dn.setRegistered(isRegistered);

    return isRegistered;
  }

  private StorageContainerDatanodeProtocolClientSideTranslatorPB
      createScmClient(InetSocketAddress address) throws IOException {

    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(
        hadoopConfig,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);

    StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        StorageContainerDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig),
        getScmRpcTimeOutInMilliseconds(conf),
        retryPolicy).getProxy();

    return new StorageContainerDatanodeProtocolClientSideTranslatorPB(
        rpcProxy);
  }

  private StorageContainerDatanodeProtocolClientSideTranslatorPB
      createReconClient(InetSocketAddress address) throws IOException {
    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(hadoopConfig, ReconDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(ReconDatanodeProtocolPB.class);

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);
    ReconDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        ReconDatanodeProtocolPB.class, version,
        address, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig),
        getScmRpcTimeOutInMilliseconds(conf),
        retryPolicy).getProxy();

    return new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);
  }

  private String randomIp() {
    return random.nextInt(256) + "." +
        random.nextInt(256) + "." +
        random.nextInt(256) + "." +
        random.nextInt(256);
  }

  private static int getScmRpcTimeOutInMilliseconds(ConfigurationSource conf) {
    return (int) conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT,
        OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
  }

}
