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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.TimeDurationUtil;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Base class for simplified performance tests.
 */
@CommandLine.Command
@SuppressWarnings("java:S2245") // no need for secure random
public class BaseFreonGenerator implements FreonSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseFreonGenerator.class);

  private static final int CHECK_INTERVAL_MILLIS = 100;

  private static final String DIGEST_ALGORITHM = "MD5";

  private static final Pattern ENV_VARIABLE_IN_PATTERN =
      Pattern.compile("__(.+?)__");

  @ParentCommand
  private Freon freonCommand;

  @Option(names = {"-n", "--number-of-tests"},
      description = "Number of the generated objects.",
      defaultValue = "1000")
  private long testNo = 1000;

  @Option(names = {"-t", "--threads", "--thread"},
      description = "Number of threads used to execute",
      defaultValue = "10")
  private int threadNo = 10;

  @Option(names = {"--duration"},
      description = "Duration to run the test. "
          + " Can be '30s', '5m', '1h', '7d' etc..")
  private String duration;

  @Option(names = {"-f", "--fail-at-end"},
      description = "If turned on, all the tasks will be executed even if "
          + "there are failures.")
  private boolean failAtEnd;

  @Option(names = {"-p", "--prefix"},
      description = "Unique identifier of the test execution. Usually used as"
          + " a prefix of the generated object names. If empty, a random name"
          + " will be generated",
      defaultValue = "")
  private String prefix = "";

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  private MetricRegistry metrics = new MetricRegistry();

  private AtomicLong successCounter;
  private AtomicLong failureCounter;
  private AtomicLong attemptCounter;

  private long startTime;
  private long durationInSecond;
  private boolean timebase;

  private PathSchema pathSchema;
  private String spanName;
  private ExecutorService executor;
  private ProgressBar progressBar;

  private final ThreadLocal<Long> threadSequenceId = new ThreadLocal<>();
  private final AtomicLong id = new AtomicLong(0);

  private final AtomicBoolean completed = new AtomicBoolean(false);

  /**
   * The main logic to execute a test generator.
   *
   * @param provider creates the new steps to execute.
   */
  public void runTests(TaskProvider provider) {
    setup(provider);
    startTaskRunners(provider);
    waitForCompletion();
    shutdown();
    reportAnyFailure();
  }

  /**
   * Performs {@code provider}-specific initialization.
   */
  private void setup(TaskProvider provider) {
    //provider is usually a lambda, print out only the owner class name:
    spanName = provider.getClass().getSimpleName().split("\\$")[0];
  }

  /**
   * Launches {@code threadNo} task runners in executor.  Each one executes test
   * tasks in a loop until completion or failure.
   */
  private void startTaskRunners(TaskProvider provider) {
    for (int i = 0; i < threadNo; i++) {
      executor.execute(() -> taskLoop(provider));
    }
  }

  /**
   * Runs test tasks in a loop until completion or failure.  This is executed
   * concurrently in {@code executor}.
   */
  private void taskLoop(TaskProvider provider) {
    threadSequenceId.set(id.getAndIncrement());
    while (!completed.get()) {
      long counter = attemptCounter.getAndIncrement();
      if (timebase) {
        if (Time.monotonicNow()
            > startTime + TimeUnit.SECONDS.toMillis(durationInSecond)) {
          completed.set(true);
          break;
        }
      } else {
        //in case of an other failed test, we shouldn't execute more tasks.
        if (counter >= testNo || (!failAtEnd && failureCounter.get() > 0)) {
          completed.set(true);
          break;
        }
      }

      tryNextTask(provider, counter % testNo);
    }

    taskLoopCompleted();
  }

  /**
   * Provides a way to clean up per-thread resources.
   */
  protected void taskLoopCompleted() {
    // no-op
  }

  /**
   * Runs a single test task (eg. key creation).
   * @param taskId unique ID of the task
   */
  private void tryNextTask(TaskProvider provider, long taskId) {
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan(spanName)) {
      provider.executeNextTask(taskId);
      successCounter.incrementAndGet();
    } catch (Exception e) {
      TracingUtil.getActiveSpan().addEvent("failure with exception: " + e.getMessage());
      TracingUtil.getActiveSpan().setStatus(StatusCode.ERROR);
      failureCounter.incrementAndGet();
      LOG.error("Error on executing task {}", taskId, e);
    }
  }

  /**
   * Waits until the requested number of tests are executed, or until any
   * failure in early failure mode (the default).  This is run in the main
   * thread.
   */
  private void waitForCompletion() {
    while (!completed.get() && (failureCounter.get() == 0 || failAtEnd)) {
      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private void shutdown() {
    if (!failAtEnd) {
      progressBar.terminate();
    } else {
      progressBar.shutdown();
    }
    threadSequenceId.remove();
    executor.shutdown();
    try {
      executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      LOG.error("Error attempting to shutdown", ex);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @throws RuntimeException if any tests failed
   */
  private void reportAnyFailure() {
    if (failureCounter.get() > 0) {
      throw new RuntimeException("One or more freon test is failed.");
    }
  }

  /**
   * Initialize internal counters, and variables. Call it before runTests.
   */
  public void init() {
    // run outside of picocli, e.g. unit tests
    if (freonCommand == null) {
      freonCommand = new Freon();
    }

    freonCommand.startHttpServer();

    successCounter = new AtomicLong(0);
    failureCounter = new AtomicLong(0);
    attemptCounter = new AtomicLong(0);

    if (prefix.isEmpty()) {
      prefix = !allowEmptyPrefix() ? RandomStringUtils.secure().nextAlphanumeric(10).toLowerCase() : "";
    } else {
      //replace environment variables to support multi-node execution
      prefix = resolvePrefix(prefix);
    }
    if (duration != null && !allowDuration()) {
      LOG.warn("--duration is ignored");
      duration = null;
    }
    if (duration != null) {
      durationInSecond = TimeDurationUtil.getTimeDurationHelper(
          "--runtime", duration, TimeUnit.SECONDS);
      if (durationInSecond <= 0) {
        throw new IllegalArgumentException(
            "Incomplete command, "
                + "the runtime must be given, and must not be negative");
      }
      timebase = true;
    }

    if (testNo <= 0) {
      throw new IllegalArgumentException(
              "Invalid command, "
                      + "the testNo must be a positive integer");
    }
    LOG.info("Executing test with prefix {} and number-of-tests {}",
        prefix.isEmpty() ? "''" : prefix, testNo);

    pathSchema = new PathSchema(prefix);

    ShutdownHookManager.get().addShutdownHook(
        () -> {
          try {
            freonCommand.stopHttpServer();
          } catch (Exception ex) {
            LOG.error("HTTP server can't be stopped.", ex);
          }
          printReport();
          if (freonCommand.isVerbose()) {
            printOption();
          }
        }, 10);

    executor = Executors.newFixedThreadPool(threadNo);
    long maxValue;
    LongSupplier supplier;
    if (duration != null) {
      maxValue = durationInSecond;
      supplier = () -> (Time.monotonicNow() - startTime) / 1000;
    } else {
      maxValue = testNo;
      supplier = () -> successCounter.get() + failureCounter.get();
    }
    progressBar = new ProgressBar(System.out, maxValue, supplier,
        freonCommand.isInteractive(), realTimeStatusSupplier());
    progressBar.start();

    startTime = Time.monotonicNow();
  }

  public Supplier<String> realTimeStatusSupplier() {
    return () -> "";
  }

  /**
   * Resolve environment variables in the prefixes.
   */
  public String resolvePrefix(String inputPrefix) {
    Matcher m = ENV_VARIABLE_IN_PATTERN.matcher(inputPrefix);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String environment = System.getenv(m.group(1));
      m.appendReplacement(sb, environment != null ? environment : "");
    }
    m.appendTail(sb);
    return sb.toString();
  }

  /**
   * Print out reports from the executed tests.
   */
  public void printReport() {
    ScheduledReporter reporter = freonCommand.isInteractive()
        ? ConsoleReporter.forRegistry(metrics).build()
        : Slf4jReporter.forRegistry(metrics).build();
    reporter.report();

    List<String> messages = new LinkedList<>();
    messages.add("Total execution time (sec): " +
        Math.round((Time.monotonicNow() - startTime) / 1000.0));
    messages.add("Failures: " + failureCounter.get());
    messages.add("Successful executions: " + successCounter.get());
    if (failureCounter.get() > 0) {
      messages.add("Expected " + testNo
          + " --number-of-tests objects!, successfully executed "
          + successCounter.get());
    }

    Consumer<String> print = freonCommand.isInteractive()
        ? System.out::println
        : LOG::info;
    messages.forEach(print);
  }

  /**
   * Print Option info about the executed tests.
   */
  public void printOption() {
    List<String> messages = new LinkedList<>();
    messages.add("\nOption:");
    for (CommandLine.Model.OptionSpec option : spec.options()) {
      String name = option.longestName();
      messages.add(name + "=" + option.getValue());
    }

    Consumer<String> print = freonCommand.isInteractive()
            ? System.out::println
            : LOG::info;
    messages.forEach(print);
  }

  /**
   * Print out reports with the given message.
   */
  public void print(String msg) {
    Consumer<String> print = freonCommand.isInteractive()
            ? System.out::println
            : LOG::info;
    print.accept(msg);
  }

  /**
   * Create the OM RPC client to use it for testing.
   */
  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      OzoneConfiguration conf, String omServiceID) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();

    if (omServiceID == null) {

      //if only one serviceId is configured, use that
      final String[] configuredServiceIds =
          conf.getTrimmedStrings(OZONE_OM_SERVICE_IDS_KEY);
      if (configuredServiceIds.length == 1) {
        omServiceID = configuredServiceIds[0];
      }
    }

    OmTransport transport = OmTransportFactory.create(conf, ugi, omServiceID);
    return new OzoneManagerProtocolClientSideTranslatorPB(transport, clientId);
  }

  public StorageContainerLocationProtocol createStorageContainerLocationClient(
      OzoneConfiguration ozoneConf) throws IOException {
    return HAUtils.getScmContainerClient(ozoneConf);
  }

  @SuppressWarnings("java:S3864") // Stream.peek (for debug)
  public static Pipeline findPipelineForTest(String pipelineId,
      StorageContainerLocationProtocol client, Logger log) throws IOException {
    Stream<Pipeline> pipelines = client.listPipelines().stream();
    Pipeline pipeline;
    if (log.isDebugEnabled()) {
      pipelines = pipelines
          .peek(p -> log.debug("Found pipeline {}", p.getId().getId()));
    }
    if (pipelineId != null && !pipelineId.isEmpty()) {
      pipeline = pipelines
          .filter(p -> p.getId().toString().equals(pipelineId))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Pipeline ID is defined, but there is no such pipeline: "
                  + pipelineId));
    } else {
      pipeline = pipelines
          .filter(p -> p.getReplicationConfig().getRequiredNodes() == 3)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "Pipeline ID is NOT defined, and no pipeline " +
                  "has been found with factor=THREE"));
      log.info("Using pipeline {}", pipeline.getId());
    }
    return pipeline;
  }

  /**
   * Generate a key/file name based on the prefix and counter.
   */
  public String generateObjectName(long counter) {
    return pathSchema.getPath(counter);
  }

  /**
   * Generate a bucket name based on the prefix and counter.
   */
  public String generateBucketName(long counter) {
    return getPrefix() + counter;
  }

  /**
   * Create missing target volume/bucket.
   */
  public void ensureVolumeAndBucketExist(OzoneClient rpcClient,
      String volumeName, String bucketName) throws IOException {

    OzoneVolume volume;
    ensureVolumeExists(rpcClient, volumeName);
    volume = rpcClient.getObjectStore().getVolume(volumeName);

    try {
      volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        volume.createBucket(bucketName);
      } else {
        throw ex;
      }
    }

  }

  /**
   * Create missing target volume.
   */
  public void ensureVolumeExists(
      OzoneClient rpcClient,
      String volumeName) throws IOException {
    try {
      rpcClient.getObjectStore().getVolume(volumeName);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
        rpcClient.getObjectStore().createVolume(volumeName);
      } else {
        throw ex;
      }
    }
  }

  /**
   * Calculate checksum of a byte array.
   */
  public static byte[] getDigest(byte[] content) {
    DigestUtils dig = new DigestUtils(DIGEST_ALGORITHM);
    dig.getMessageDigest().reset();
    return dig.digest(content);
  }

  /**
   * Calculate checksum of an Input stream.
   */
  public static byte[] getDigest(InputStream stream) throws IOException {
    DigestUtils dig = new DigestUtils(DIGEST_ALGORITHM);
    dig.getMessageDigest().reset();
    return dig.digest(stream);
  }

  /**
   * When no prefix is specified,
   * if allowEmptyPrefix is false, a random prefix will be used;
   * if allowEmptyPrefix is true, an empty prefix will be used.
   */
  public boolean allowEmptyPrefix() {
    return false;
  }

  public String getPrefix() {
    return prefix;
  }

  /**
   * Whether to enable Duration.
   * If enabled, the command will load the --duration option.
   * If not enabled, the command will not load the --duration option.
   */
  public boolean allowDuration() {
    return true;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public OzoneConfiguration createOzoneConfiguration() {
    return freonCommand.getOzoneConf();
  }

  /**
   * Simple contract to execute a new step during a freon test.
   */
  @FunctionalInterface
  public interface TaskProvider {
    void executeNextTask(long step) throws Exception;
  }

  public long getAttemptCount() {
    return attemptCounter.get();
  }

  public long getSuccessCount() {
    return successCounter.get();
  }

  public long getFailureCount() {
    return failureCounter.get();
  }

  public boolean isCompleted() {
    return completed.get();
  }

  public int getThreadNo() {
    return threadNo;
  }

  public void setThreadNo(int threadNo) {
    this.threadNo = threadNo;
  }

  /**
   * Get current Thread sequence ID.
   * Similar to Thread tid, but sequence ID starts at 0 and is consecutive
   * each thread will be set a self-incrementing sequence id
   * after it be created.
   * @return Current Thread sequence ID
   */
  public long getThreadSequenceId() {
    return threadSequenceId.get();
  }

  protected OzoneClient createOzoneClient(String omServiceID,
      OzoneConfiguration conf) throws Exception {
    if (omServiceID != null) {
      return OzoneClientFactory.getRpcClient(omServiceID, conf);
    } else {
      return OzoneClientFactory.getRpcClient(conf);
    }
  }

  public void setTestNo(long testNo) {
    this.testNo = testNo;
  }

  public long getTestNo() {
    return testNo;
  }
}
