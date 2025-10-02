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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryCount;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmRpcRetryInterval;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Benchmark for scm throughput.
 * - allocate blocks (ops)
 * - allocate containers (ops)
 * - process reports(container reports only) (ops)
 *
 * Remember to add the following configs to your ozone-site.xml:
 * - ozone.scm.heartbeat.thread.interval: 1h
 * - hdds.heartbeat.interval: 1h
 * - ozone.scm.stale.node.interval: 1d
 * - ozone.scm.dead.node.interval: 2d
 * These make the faked datanodes long live.
 */
@CommandLine.Command(name = "scm-throughput-benchmark",
    aliases = "stb",
    description = "Benchmark for scm throughput.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public final class SCMThroughputBenchmark implements Callable<Void>, FreonSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMThroughputBenchmark.class);

  @CommandLine.ParentCommand
  private Freon freon;

  @CommandLine.Option(names = {"--benchmark"},
      description = "Which type of benchmark to run " +
          "(AllocateBlocks, AllocateContainers, ProcessReports).",
      required = true,
      defaultValue = "")
  private String benchmarkType = "";

  @CommandLine.Option(names = {"--num-blocks"},
      description = "Number of blocks.",
      defaultValue = "1000")
  private int numBlocks = 1000;

  @CommandLine.Option(names = {"--block-size"},
      description = "Block size.",
      defaultValue = "4096")
  private long blockSize = 4096;

  @CommandLine.Option(names = {"--num-containers"},
      description = "Number of containers.",
      defaultValue = "100")
  private int numContainers = 100;

  @CommandLine.Option(names = {"--num-datanodes"},
      description = "Number of fake datanodes.",
      defaultValue = "10")
  private int numDatanodes = 10;

  @CommandLine.Option(names = {"--num-threads"},
      description = "Number of scm client threads.",
      defaultValue = "4")
  private int numThreads = 4;

  @CommandLine.Option(names = {"--num-heartbeats"},
      description = "Number of heartbeats that carries reports.",
      defaultValue = "4")
  private int numHeartbeats = 4;

  @CommandLine.Option(names = {"--scmHost", "--scm-host"},
      required = true,
      description = "The leader scm host x.x.x.x.")
  private String scm;

  @CommandLine.Mixin
  private FreonReplicationOptions replication;

  static final int CHECK_INTERVAL_MILLIS = 5000;

  private static final Random RANDOM = new Random();

  private OzoneConfiguration conf;

  private List<FakeDatanode> datanodes;

  private StorageContainerDatanodeProtocol datanodeScmClient;

  private StorageContainerLocationProtocol scmContainerClient;

  private ScmBlockLocationProtocol scmBlockClient;

  @Override
  public Void call() throws Exception {
    conf = freon.getOzoneConf();

    ThroughputBenchmark benchmark = createBenchmark();
    initCluster(benchmark);
    benchmark.run();

    return null;
  }

  private ThroughputBenchmark createBenchmark() {
    ThroughputBenchmark benchmark = null;
    BenchmarkType type = BenchmarkType.valueOf(benchmarkType);
    switch (type) {
    case AllocateBlocks:
      benchmark = new BlockBenchmark(numThreads, numBlocks, blockSize);
      break;
    case AllocateContainers:
      benchmark = new ContainerBenchmark(numThreads, numContainers);
      break;
    case ProcessReports:
      benchmark = new ReportBenchmark(numDatanodes, numContainers,
          numHeartbeats);
      break;
    default:
      throw new IllegalArgumentException(benchmarkType +
          " is not a valid benchmarkType.");
    }

    LOG.info("Run benchmark: {}", type);

    return benchmark;
  }

  private void initCluster(ThroughputBenchmark benchmark)
      throws IOException, InterruptedException, IllegalArgumentException {

    initSCMClients();

    registerFakeDatanodes();

    if (benchmark.requiresPipelines()) {
      activatePipelines();
    }

    exitSafeMode();
  }

  private void initSCMClients() throws IOException {
    datanodeScmClient = createDatanodeScmClient();

    scmContainerClient = createScmContainerClient();

    scmBlockClient = createScmBlockClient();

    LOG.info("Initialized scm clients " +
        "{datanodeClient, containerClient, blockClient}");
  }

  private void registerFakeDatanodes() throws IOException {
    datanodes = new ArrayList<>();

    for (int i = 0; i < numDatanodes; i++) {
      FakeDatanode dn = new FakeDatanode();
      dn.register();
      datanodes.add(dn);
    }

    LOG.info("Registered datanode(fake): {}", numDatanodes);
  }

  /**
   * Activate pipelines manually, since we only have fake datanodes
   * that don't react to scm commands.
   * @throws IOException
   * @throws InterruptedException
   */
  private void activatePipelines() throws IOException, InterruptedException {
    LOG.info("Waiting for pipelines to be allocated automatically");

    Thread.sleep(Duration.ofSeconds(60).toMillis());

    List<Pipeline> pipelines = scmContainerClient.listPipelines();

    for (Pipeline pipeline : pipelines) {
      scmContainerClient.activatePipeline(pipeline.getId().getProtobuf());
    }

    LOG.info("Force opened pipelines: {}", pipelines.size());
  }

  /**
   * Exit SafeMode manually, so we could do block allocations.
   * @throws IOException
   */
  private void exitSafeMode() throws IOException {
    try {
      if (!scmContainerClient.forceExitSafeMode()) {
        throw new IOException("Safe mode exit failed");
      }
    } catch (IOException e) {
      LOG.warn("Safe mode exit with exception, but we can go on");
    }

    try {
      if (scmContainerClient.inSafeMode()) {
        throw new IOException("Safe mode exit failed indeed");
      }
    } catch (IOException e) {
      LOG.error("{}", e);
      throw e;
    }

    LOG.info("Force exited safe mode");
  }

  private StorageContainerDatanodeProtocol createDatanodeScmClient()
      throws IOException {
    int dnPort = conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
        OZONE_SCM_DATANODE_PORT_DEFAULT);
    InetSocketAddress scmAddress = NetUtils.createSocketAddr(scm, dnPort);

    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(this.conf);
    RPC.setProtocolEngine(
        hadoopConfig,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    long version =
        RPC.getProtocolVersion(StorageContainerDatanodeProtocolPB.class);

    SCMClientConfig scmClientConfig = conf.getObject(SCMClientConfig.class);
    int rpcTimeout = (int) scmClientConfig.getRpcTimeOut();

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            getScmRpcRetryCount(conf), getScmRpcRetryInterval(conf),
            TimeUnit.MILLISECONDS);

    StorageContainerDatanodeProtocolPB rpcProxy = RPC.getProtocolProxy(
        StorageContainerDatanodeProtocolPB.class, version,
        scmAddress, UserGroupInformation.getCurrentUser(), hadoopConfig,
        NetUtils.getDefaultSocketFactory(hadoopConfig), rpcTimeout,
        retryPolicy).getProxy();

    return new StorageContainerDatanodeProtocolClientSideTranslatorPB(rpcProxy);
  }

  private StorageContainerLocationProtocol createScmContainerClient() {
    return HAUtils.getScmContainerClient(conf);
  }

  private ScmBlockLocationProtocol createScmBlockClient() {
    return HAUtils.getScmBlockClient(conf);
  }

  /**
   * Base class for all benchmark types.
   */
  private abstract class ThroughputBenchmark {

    private static final String DURATION_FORMAT = "HH:mm:ss,SSS";

    private long startTime;
    private long execTime;
    private String formattedTime;
    private int numThreads;
    private Queue<Runnable> taskQueue;
    private ExecutorService executor;

    ThroughputBenchmark(int threads) {
      this.numThreads = threads;
      this.taskQueue = new LinkedList<>();
      this.executor = Executors.newFixedThreadPool(this.numThreads);
    }

    public void run() throws InterruptedException {
      prepare();

      execTasks();

      showSummary();
    }

    public void prepare() {
      LOG.info("Preparing tasks to run");
    }

    public void execTasks() throws InterruptedException {
      setStartTime(System.nanoTime());

      LOG.info("Benchmark tasks started");

      for (int i = 0; i < this.numThreads; i++) {
        this.executor.execute(taskQueue.poll());
      }

      waitForComplete();

      this.executor.shutdown();
      this.executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void showSummary() {
      execTime = System.nanoTime() - this.startTime;
      formattedTime = DurationFormatUtils.formatDuration(
          TimeUnit.NANOSECONDS.toMillis(execTime),
          DURATION_FORMAT);
    }

    public abstract void waitForComplete() throws InterruptedException;

    protected long getStartTime() {
      return this.startTime;
    }

    protected void setStartTime(long time) {
      this.startTime = time;
    }

    protected long getExecTime() {
      return this.execTime;
    }

    protected String getFormattedTime() {
      return this.formattedTime;
    }

    protected int getNumThreads() {
      return this.numThreads;
    }

    protected void enqueueTask(Runnable task) {
      this.taskQueue.add(task);
    }

    public boolean requiresPipelines() {
      return false;
    }
  }

  /**
   * Benchmarks throughput of allocate block operation from scm clients.
   */
  private class BlockBenchmark extends ThroughputBenchmark {

    private final ReplicationConfig replicationConfig;
    private final ExcludeList excludeList = new ExcludeList();
    private AtomicLong totalBlockCounter;
    private AtomicLong succBlockCounter;
    private AtomicLong failBlockCounter;
    private int totalBlocks;
    private long blockSize;

    BlockBenchmark(int threads, int blocks, long blockSize) {
      super(threads);
      this.totalBlocks = blocks;
      this.blockSize = blockSize;
      this.totalBlockCounter = new AtomicLong();
      this.succBlockCounter = new AtomicLong();
      this.failBlockCounter = new AtomicLong();
      ReplicationConfig rc = replication.fromParamsOrConfig(conf);
      if (rc == null) {
        rc = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
      }
      replicationConfig = rc;
    }

    @Override
    public boolean requiresPipelines() {
      return replicationConfig.getReplicationType() == ReplicationType.RATIS;
    }

    @Override
    public void prepare() {
      super.prepare();
      for (int i = 0; i < getNumThreads(); i++) {
        enqueueTask(new BlockTask(blockSize, replicationConfig));
      }
    }

    @Override
    public void showSummary() {
      super.showSummary();

      long execSecs = TimeUnit.SECONDS.convert(getExecTime(),
          TimeUnit.NANOSECONDS);
      long blocks = succBlockCounter.get();
      float blocksPerSec = execSecs != 0 ? (float) blocks / execSecs : blocks;

      System.out.println("***************************************");
      System.out.printf("Total allocated blocks: %d%n",
          succBlockCounter.get());
      System.out.printf("Total failed blocks: %d%n",
          failBlockCounter.get());
      System.out.printf("Execution Time: %s%n", getFormattedTime());
      System.out.printf("Throughput: %f (ops)%n", blocksPerSec);
      System.out.println("***************************************");
    }

    @Override
    public void waitForComplete() throws InterruptedException {
      while (totalBlockCounter.get() < this.totalBlocks) {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
        LOG.info("Blocks allocated: {}/{}",
            totalBlockCounter.get(), this.totalBlocks);
      }
    }

    private void doAllocateBlock(long size, ReplicationConfig config) {
      try {
        scmBlockClient.allocateBlock(size, 1, config, "STB", excludeList);
        succBlockCounter.incrementAndGet();
      } catch (IOException e) {
        LOG.error("Failed to allocate block", e);
        failBlockCounter.incrementAndGet();
      }
    }

    private class BlockTask implements Runnable {

      private final long blockSize;
      private final ReplicationConfig replicationConfig;

      BlockTask(long blockSize, ReplicationConfig replicationConfig) {
        this.blockSize = blockSize;
        this.replicationConfig = replicationConfig;
      }

      @Override
      public void run() {
        while (totalBlockCounter.getAndIncrement() < totalBlocks) {
          doAllocateBlock(blockSize, replicationConfig);
        }
      }
    }
  }

  /**
   * Benchmarks throughput of allocate container operation which
   * comes along with allocate block.
   * We do this to see how allocate container scales.
   */
  private class ContainerBenchmark extends ThroughputBenchmark {

    private AtomicInteger totalContainerCounter;
    private AtomicInteger succContainerCounter;
    private AtomicInteger failContainerCounter;
    private int totalContainers;

    ContainerBenchmark(int threads, int containers) {
      super(threads);
      this.totalContainers = containers;
      this.totalContainerCounter = new AtomicInteger();
      this.succContainerCounter = new AtomicInteger();
      this.failContainerCounter = new AtomicInteger();
    }

    @Override
    public void prepare() {
      super.prepare();
      for (int i = 0; i < getNumThreads(); i++) {
        enqueueTask(new ContainerTask());
      }
    }

    @Override
    public void showSummary() {
      super.showSummary();

      long execSecs = TimeUnit.SECONDS.convert(getExecTime(),
          TimeUnit.NANOSECONDS);
      long containers = succContainerCounter.get();
      float containersPerSec = execSecs != 0 ?
          (float)containers / execSecs : containers;

      System.out.println("***************************************");
      System.out.printf("Total allocated containers: %d%n",
          succContainerCounter.get());
      System.out.printf("Total failed containers: %d%n",
          failContainerCounter.get());
      System.out.printf("Execution Time: %s%n", getFormattedTime());
      System.out.printf("Throughput: %f (ops)%n", containersPerSec);
      System.out.println("***************************************");
    }

    @Override
    public void waitForComplete() throws InterruptedException {
      while (totalContainerCounter.get() < this.totalContainers) {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
        LOG.info("Containers allocated: {}/{}",
            totalContainerCounter.get(), this.totalContainers);
      }
    }

    private void doAllocateContainer(ReplicationFactor factor) {
      try {
        scmContainerClient.allocateContainer(
            ReplicationType.RATIS, factor, "STB");
        succContainerCounter.incrementAndGet();
      } catch (IOException e) {
        LOG.error("{}", e);
        failContainerCounter.incrementAndGet();
      }
    }

    private class ContainerTask implements Runnable {

      ContainerTask() {
      }

      @Override
      public void run() {
        while (totalContainerCounter.getAndIncrement() < totalContainers) {
          doAllocateContainer(ReplicationFactor.THREE);
        }
      }
    }
  }

  /**
   * Benchmarks throughput of process report in scm.
   * 1. Create containers.
   * 2. Send container reports based on the info of the containers created.
   */
  private class ReportBenchmark extends ThroughputBenchmark {

    private static final String REPORT_PREFIX =
        "scm_container_manager_metrics_num_container_reports_processed_";
    private static final String REPORT_SUCC_KEY = REPORT_PREFIX + "successful";
    private static final String REPORT_FAIL_KEY = REPORT_PREFIX + "failed";

    private AtomicInteger succReportSendCounter;
    private AtomicInteger failReportSendCounter;
    private int reportSuccProcessedOnRegister;
    private int reportFailProcessedOnRegister;
    private int succReportsProcessed;
    private int failReportsProcessed;
    private int numReports;
    private int numReportRounds;
    private int totalContainers;
    private int containersPerReport;
    private List<ContainerInfo> containers;

    ReportBenchmark(int threads, int containers, int rounds) {
      super(threads);
      this.succReportSendCounter = new AtomicInteger();
      this.failReportSendCounter = new AtomicInteger();
      this.numReportRounds = rounds;
      this.numReports = numDatanodes * rounds;
      this.totalContainers = containers;
      this.containers = new ArrayList<>();
      this.reportSuccProcessedOnRegister = 0;
      this.reportFailProcessedOnRegister = 0;
    }

    /**
     * Prepare containers on scm side for reports.
     */
    @Override
    public void prepare() {
      // allocate containers
      // here we intend to use RATIS/ONE, then we don't have to
      // distribute containers to datanodes in regards of replication.

      LOG.info("Preparing containers for reports");

      for (int i = 0; i < totalContainers; i++) {
        try {
          ContainerWithPipeline container = scmContainerClient
              .allocateContainer(ReplicationType.RATIS, ReplicationFactor.ONE,
                  "STB");
          containers.add(container.getContainerInfo());
        } catch (IOException e) {
          LOG.error("{}", e);
        }
      }

      LOG.info("Allocated containers: {}", containers.size());

      // build container reports based on container info returned
      totalContainers = containers.size();
      containersPerReport = totalContainers / numDatanodes;
      int from = 0;
      for (int i = 0; i < getNumThreads(); i++) {
        datanodes.get(i).buildContainerReports(containers.subList(
            from, Math.min(from + containersPerReport, totalContainers)));
        from += containersPerReport;
      }

      getScmReportProcessed();
      reportSuccProcessedOnRegister = this.succReportsProcessed;
      reportFailProcessedOnRegister = this.failReportsProcessed;
      this.succReportsProcessed = 0;
      this.failReportsProcessed = 0;

      super.prepare();
      for (int i = 0; i < getNumThreads(); i++) {
        enqueueTask(new ReportTask(datanodes.get(i), numReportRounds));
      }
    }

    @Override
    public void showSummary() {
      super.showSummary();

      long execSecs = TimeUnit.SECONDS.convert(getExecTime(),
          TimeUnit.NANOSECONDS);
      float reportsPerSec = execSecs > 0 ?
          (float) succReportsProcessed / execSecs : succReportsProcessed;

      System.out.println("***************************************");
      System.out.printf("Total container reports processed: %d%n",
          succReportsProcessed);
      System.out.printf("Total container reports failed: %d%n",
          failReportsProcessed);
      System.out.printf("Containers per report: %d%n", containersPerReport);
      System.out.printf("Execution Time: %s%n", getFormattedTime());
      System.out.printf("Throughput: %f (ops)%n", reportsPerSec);
      System.out.println("***************************************");
    }

    @Override
    public void waitForComplete() throws InterruptedException {
      while (succReportsProcessed + failReportsProcessed < numReports) {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
        LOG.info("Processing reports: ({}+{})/{}",
            succReportsProcessed, failReportsProcessed, numReports);
        getScmReportProcessed();
      }
    }

    private void getScmReportProcessed() {
      List<String> metricsLines = getScmReportProcessedMetricsLines();
      for (String line : metricsLines) {
        if (line.startsWith("#")) {
          continue;
        }
        if (line.startsWith(REPORT_SUCC_KEY)) {
          this.succReportsProcessed = Integer.parseInt(getMetricsValue(line))
              - this.reportSuccProcessedOnRegister;
        }
        if (line.startsWith(REPORT_FAIL_KEY)) {
          this.failReportsProcessed = Integer.parseInt(getMetricsValue(line))
              - this.reportFailProcessedOnRegister;
        }
      }
    }

    private List<String> getScmReportProcessedMetricsLines() {
      HttpClient client = HttpClientBuilder.create().build();
      String uri = String.format("http://%s:%d/prom", scm,
          ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT);
      HttpGet get = new HttpGet(uri);
      try {
        HttpResponse execute = client.execute(get);
        if (execute.getStatusLine().getStatusCode() != 200) {
          throw new RuntimeException(
              "Can't read prometheus metrics endpoint" + execute.getStatusLine()
                  .getStatusCode());
        }
        try (BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(execute.getEntity().getContent(),
                StandardCharsets.UTF_8))) {
          return bufferedReader.lines().collect(Collectors.toList());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    String getMetricsValue(String line) {
      String[] parts = line.split(" ");
      return parts[1];
    }

    private class ReportTask implements Runnable {

      private FakeDatanode datanode;
      private int rounds;

      ReportTask(FakeDatanode datanode, int rounds) {
        this.datanode = datanode;
        this.rounds = rounds;
      }

      @Override
      public void run() {
        for (int i = 0; i < rounds; i++) {
          try {
            datanode.sendHeartbeat();
            succReportSendCounter.incrementAndGet();
          } catch (IOException | TimeoutException e) {
            LOG.error("{}", e);
            failReportSendCounter.incrementAndGet();
          }
        }
      }
    }
  }

  /**
   * This class simulates the register and heartbeat behavior
   * of a normal Datanode, but does not have real daemons.
   */
  private class FakeDatanode {
    private DatanodeDetails datanodeDetails;
    private ContainerReportsProto containerReport;

    FakeDatanode() {
      datanodeDetails = createRandomDatanodeDetails();
      containerReport = null;
    }

    public void register() throws IOException {
      SCMRegisteredResponseProto response = datanodeScmClient.register(
          datanodeDetails.getExtendedProtoBufMessage(),
          createNodeReport(datanodeDetails.getUuid()),
          createContainerReport(),
          createPipelineReport(),
          UpgradeUtils.defaultLayoutVersionProto());

      if (response.hasHostname() && response.hasIpAddress()) {
        datanodeDetails.setHostName(response.getHostname());
        datanodeDetails.setIpAddress(response.getIpAddress());
      }
      if (response.hasNetworkName() && response.hasNetworkLocation()) {
        datanodeDetails.setNetworkName(response.getNetworkName());
        datanodeDetails.setNetworkLocation(response.getNetworkLocation());
      }
    }

    public void sendHeartbeat() throws IOException, TimeoutException {
      SCMHeartbeatRequestProto heartbeatRequest = SCMHeartbeatRequestProto
          .newBuilder()
          .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
          .setContainerReport(containerReport)
          .setDataNodeLayoutVersion(UpgradeUtils.defaultLayoutVersionProto())
          .build();
      datanodeScmClient.sendHeartbeat(heartbeatRequest);
      // scm commands are ignored
    }

    public void buildContainerReports(List<ContainerInfo> containers) {
      ContainerReportsProto.Builder reportBuilder =
          ContainerReportsProto.newBuilder();
      for (ContainerInfo cinfo : containers) {
        ContainerReplicaProto.Builder replicaBuilder =
            ContainerReplicaProto.newBuilder();
        replicaBuilder.setContainerID(cinfo.getContainerID())
            .setReadCount(0)
            .setWriteCount(1)
            .setReadBytes(0)
            .setWriteBytes(4096)
            .setKeyCount(1)
            .setUsed(4096)
            .setState(ContainerReplicaProto.State.OPEN)
            .setDeleteTransactionId(cinfo.getDeleteTransactionId())
            .setBlockCommitSequenceId(cinfo.getSequenceId())
            .setOriginNodeId(datanodeDetails.getUuidString());

        reportBuilder.addReports(replicaBuilder.build());
      }

      containerReport = reportBuilder.build();
    }

    public DatanodeDetails getDatanodeDetails() {
      return this.datanodeDetails;
    }
  }

  private static DatanodeDetails createRandomDatanodeDetails() {
    UUID uuid = UUID.randomUUID();
    String ipAddress =
        RANDOM.nextInt(256) + "." + RANDOM.nextInt(256) + "." + RANDOM
            .nextInt(256) + "." + RANDOM.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newStandalonePort(0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newRatisPort(0);
    DatanodeDetails.Port restPort = DatanodeDetails.newRestPort(0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid).setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  private static NodeReportProto createNodeReport(UUID nodeId) {
    List<StorageReportProto> storageReports = new ArrayList<>();
    List<MetadataStorageReportProto> metadataStorageReports =
        new ArrayList<>();
    storageReports.add(createStorageReport(nodeId));
    metadataStorageReports.add(createMetadataStorageReport());
    NodeReportProto.Builder nb = NodeReportProto.newBuilder();
    nb.addAllStorageReport(storageReports)
        .addAllMetadataStorageReport(metadataStorageReports);
    return nb.build();
  }

  private static StorageReportProto createStorageReport(UUID nodeId) {
    StorageReportProto.Builder srb = StorageReportProto.newBuilder();
    srb.setStorageUuid(nodeId.toString())
        .setStorageLocation("/data")
        .setCapacity(100 * OzoneConsts.TB)
        .setScmUsed(0)
        .setFailed(false)
        .setRemaining(100 * OzoneConsts.TB)
        .setStorageType(StorageTypeProto.DISK);
    return srb.build();
  }

  private static MetadataStorageReportProto createMetadataStorageReport() {
    MetadataStorageReportProto.Builder mrb =
        MetadataStorageReportProto.newBuilder();
    mrb.setStorageLocation("/meta")
        .setCapacity(100 * OzoneConsts.GB)
        .setScmUsed(0)
        .setFailed(false)
        .setRemaining(100 * OzoneConsts.GB)
        .setStorageType(StorageTypeProto.DISK);
    return mrb.build();
  }

  private static ContainerReportsProto createContainerReport() {
    return ContainerReportsProto.newBuilder().build();
  }

  private static PipelineReportsProto createPipelineReport() {
    return PipelineReportsProto.newBuilder().build();
  }

  /**
   * Type of benchmarks.
   */
  public enum BenchmarkType {
    AllocateBlocks,
    AllocateContainers,
    ProcessReports,
  }
}
