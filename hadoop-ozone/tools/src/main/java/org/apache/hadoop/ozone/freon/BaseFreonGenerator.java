/*
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
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Base class for simplified performance tests.
 */
public class BaseFreonGenerator {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseFreonGenerator.class);

  private static final int CHECK_INTERVAL_MILLIS = 1000;

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
  private int threadNo;

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

  private MetricRegistry metrics = new MetricRegistry();

  private AtomicLong successCounter;
  private AtomicLong failureCounter;
  private AtomicLong attemptCounter;

  private long startTime;

  private PathSchema pathSchema;
  private String spanName;
  private ExecutorService executor;
  private ProgressBar progressBar;

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
    while (true) {
      long counter = attemptCounter.getAndIncrement();

      //in case of an other failed test, we shouldn't execute more tasks.
      if (counter >= testNo || (!failAtEnd && failureCounter.get() > 0)) {
        return;
      }

      tryNextTask(provider, counter);
    }
  }

  /**
   * Runs a single test task (eg. key creation).
   * @param taskId unique ID of the task
   */
  private void tryNextTask(TaskProvider provider, long taskId) {
    Scope scope =
        GlobalTracer.get().buildSpan(spanName)
            .startActive(true);
    try {
      provider.executeNextTask(taskId);
      successCounter.incrementAndGet();
    } catch (Exception e) {
      scope.span().setTag("failure", true);
      failureCounter.incrementAndGet();
      LOG.error("Error on executing task {}", taskId, e);
    } finally {
      scope.close();
    }
  }

  /**
   * Waits until the requested number of tests are executed, or until any
   * failure in early failure mode (the default).  This is run in the main
   * thread.
   */
  private void waitForCompletion() {
    while (successCounter.get() + failureCounter.get() < testNo && (
        failureCounter.get() == 0 || failAtEnd)) {
      try {
        Thread.sleep(CHECK_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private void shutdown() {
    if (failureCounter.get() > 0 && !failAtEnd) {
      progressBar.terminate();
    } else {
      progressBar.shutdown();
    }
    executor.shutdown();
    try {
      executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * @throws RuntimeException if any tests failed
   */
  private void reportAnyFailure() {
    if (failureCounter.get() > 0) {
      throw new RuntimeException("One ore more freon test is failed.");
    }
  }

  /**
   * Initialize internal counters, and variables. Call it before runTests.
   */
  public void init() {

    freonCommand.startHttpServer();

    successCounter = new AtomicLong(0);
    failureCounter = new AtomicLong(0);
    attemptCounter = new AtomicLong(0);

    if (prefix.length() == 0) {
      prefix = RandomStringUtils.randomAlphanumeric(10);
    } else {
      //replace environment variables to support multi-node execution
      prefix = resolvePrefix(prefix);
    }
    LOG.info("Executing test with prefix {}", prefix);

    pathSchema = new PathSchema(prefix);

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          try {
            freonCommand.stopHttpServer();
          } catch (Exception ex) {
            LOG.error("HTTP server can't be stopped.", ex);
          }
          printReport();
        }));

    executor = Executors.newFixedThreadPool(threadNo);

    progressBar = new ProgressBar(System.out, testNo, successCounter::get,
        freonCommand.isInteractive());
    progressBar.start();

    startTime = System.currentTimeMillis();
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
        Math.round((System.currentTimeMillis() - startTime) / 1000.0));
    messages.add("Failures: " + failureCounter.get());
    messages.add("Successful executions: " + successCounter.get());

    Consumer<String> print = freonCommand.isInteractive()
        ? System.out::println
        : LOG::info;
    messages.forEach(print);
  }

  /**
   * Create the OM RPC client to use it for testing.
   */
  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      OzoneConfiguration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    long omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    InetSocketAddress omAddress = OmUtils.getOmAddressForClients(conf);
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();
    return new OzoneManagerProtocolClientSideTranslatorPB(
        RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress,
            ugi, conf, NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)), clientId);
  }

  public StorageContainerLocationProtocol createStorageContainerLocationClient(
      OzoneConfiguration ozoneConf)
      throws IOException {

    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress scmAddress =
        getScmAddressForClients(ozoneConf);

    RPC.setProtocolEngine(ozoneConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    StorageContainerLocationProtocol client =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
                    scmAddress, UserGroupInformation.getCurrentUser(),
                    ozoneConf,
                    NetUtils.getDefaultSocketFactory(ozoneConf),
                    Client.getRpcTimeout(ozoneConf))),
            StorageContainerLocationProtocol.class, ozoneConf);
    return client;
  }

  /**
   * Generate a key/file name based on the prefix and counter.
   */
  public String generateObjectName(long counter) {
    return pathSchema.getPath(counter);
  }

  /**
   * Create missing target volume/bucket.
   */
  public void ensureVolumeAndBucketExist(OzoneConfiguration ozoneConfiguration,
      String volumeName, String bucketName) throws IOException {

    try (OzoneClient rpcClient = OzoneClientFactory
        .getRpcClient(ozoneConfiguration)) {

      OzoneVolume volume = null;
      try {
        volume = rpcClient.getObjectStore().getVolume(volumeName);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
          rpcClient.getObjectStore().createVolume(volumeName);
          volume = rpcClient.getObjectStore().getVolume(volumeName);
        } else {
          throw ex;
        }
      }

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
  }

  /**
   * Create missing target volume.
   */
  public void ensureVolumeExists(
      OzoneConfiguration ozoneConfiguration,
      String volumeName) throws IOException {
    try (OzoneClient rpcClient = OzoneClientFactory
        .getRpcClient(ozoneConfiguration)) {

      try {
        rpcClient.getObjectStore().getVolume(volumeName);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
          rpcClient.getObjectStore().createVolume(volumeName);
        }
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

  public String getPrefix() {
    return prefix;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public OzoneConfiguration createOzoneConfiguration() {
    return freonCommand.createOzoneConfiguration();
  }

  /**
   * Simple contract to execute a new step during a freon test.
   */
  @FunctionalInterface
  public interface TaskProvider {
    void executeNextTask(long step) throws Exception;
  }

  public int getThreadNo() {
    return threadNo;
  }
}
