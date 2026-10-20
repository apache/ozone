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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.upgrade.JobworkerVersion;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.commands.JobworkerCommandHandler;
import org.apache.hadoop.ozone.jobworker.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.jobworker.version.JobworkerBuildVersionInfo;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * JobworkerService is the main class that starts and stops the JobWorker service.
 */

@Command(name = "ozone jobworker",
    hidden = true, description = "Start the JobWorker service for Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class JobworkerService extends GenericCli implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(
      JobworkerService.class);
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private OzoneConfiguration conf;
  private JobworkerDetails jobworkerDetails;
  private JobworkerStateMachine jobworkerStateMachine;
  private OzoneAdmins admins;
  private boolean printBanner;
  private String[] args;

  /**
   * Create a JobworkerService instance based on the supplied command-line arguments.
   * <p>
   *
   * @param args command line arguments.
   */
  @VisibleForTesting
  public JobworkerService(String[] args) {
    this(false, args);
  }

  /**
   * Create a JobworkerService instance based on the supplied command-line arguments.
   *
   * @param args        command line arguments.
   * @param printBanner if true, then log a verbose startup message.
   */
  private JobworkerService(boolean printBanner, String[] args) {
    this.printBanner = printBanner;
    this.args = args != null ? Arrays.copyOf(args, args.length) : null;
  }

  public static void main(String[] args) {
    try {
      OzoneNetUtils.disableJvmNetworkAddressCacheIfRequired(
          new OzoneConfiguration());
      JobworkerService jobworkerService =
          new JobworkerService(true, args);
      jobworkerService.run(args);
    } catch (Throwable e) {
      LOG.error("Exception in JobworkerService.", e);
      terminate(1, e);
    }
  }

  public static Logger getLogger() {
    return LOG;
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = getOzoneConf();
    if (printBanner) {
      HddsServerUtil.startupShutdownMessage(JobworkerBuildVersionInfo.JOBWORKER_VERSION_INFO,
          JobworkerService.class, args, LOG, configuration);
    }
    start(configuration);
    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        stop();
        join();
      } catch (Exception e) {
        LOG.error("Error during stopping JobworkerService.", e);
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    return null;
  }

  public void setConfiguration(OzoneConfiguration configuration) {
    this.conf = configuration;
  }

  /**
   * Start the JobworkerService with the given configuration.
   *
   * @param configuration The configuration to use
   */
  public void start(OzoneConfiguration configuration) {
    setConfiguration(configuration);
    start();
  }

  /**
   * Start the JobworkerService with the current configuration.
   */
  public void start() {
    try {
      // only for compatibility integration testing, reset the stop flag
      isStopped.set(false);
      String hostname = HddsUtils.getHostName(conf);
      String ip = InetAddress.getByName(hostname).getHostAddress();

      // Initialize JobworkerDetail
      jobworkerDetails = initializeJobworkerDetails(hostname, ip);

      // Initialize tracing
      TracingUtil.initTracing(
          "JobworkerService." + jobworkerDetails.getUuidString()
              .substring(0, 8), conf);
      LOG.info("JobworkerService host:{} ip:{}", hostname, ip);

      // TODO jobworker do we need the layoutStorage?

      jobworkerStateMachine = new JobworkerStateMachine(
          jobworkerDetails, conf, this::terminateJobworker, createCommandHandlers());

      String starterUser =
          UserGroupInformation.getCurrentUser().getShortUserName();
      admins = OzoneAdmins.getOzoneAdmins(starterUser, conf);
      LOG.info("JobWorker started with admins: {}", admins.getAdminUsernames());

      jobworkerStateMachine.startDaemon();

      LOG.info("JobworkerService started successfully");
    } catch (IOException | RuntimeException e) {
      throw new RuntimeException("Can't start the JobworkerService", e);
    }
  }

  /**
   * Initialize JobworkerDetails for this JobWorker.
   *
   * @param hostname The hostname of this JobWorker
   * @param ip       The IP address of this JobWorker
   * @return JobworkerDetails instance
   */
  private JobworkerDetails initializeJobworkerDetails(String hostname, String ip) {
    // A jobworker keeps no state across restarts, so it registers with a new identity each time it starts.
    return JobworkerDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setIpAddress(ip)
        .setHostName(hostname)
        .setOperationalState(HddsProtos.NodeOperationalState.IN_SERVICE)
        .setJobworkerVersion(JobworkerVersion.CURRENT)
        .setVersion(JobworkerBuildVersionInfo.JOBWORKER_VERSION_INFO.getVersion())
        .setBuildDate(JobworkerBuildVersionInfo.JOBWORKER_VERSION_INFO.getDate())
        .setRevision(JobworkerBuildVersionInfo.JOBWORKER_VERSION_INFO.getRevision())
        .setSetupTime(Time.now())
        .build();
  }

  /** Creates the statically registered handlers owned by this worker service. */
  protected JobworkerCommandHandler[] createCommandHandlers() {
    return new JobworkerCommandHandler[0];
  }

  /**
   * Join the service threads.
   */
  public void join() {
    try {
      if (jobworkerStateMachine != null) {
        jobworkerStateMachine.join();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while stopping JobworkerService", e);
    }

  }

  /**
   * Stop the service.
   */
  public void stop() {
    if (!isStopped.getAndSet(true)) {
      LOG.info("Stopping JobworkerService...");

      if (jobworkerStateMachine != null) {
        jobworkerStateMachine.stopDaemon();
      }
      // Only used for compatibility with integration tests
      // that may create multiple different clusters in a JVM
      VersionEndpointTask.resetClusterId();
      LOG.info("JobworkerService stopped");
    }
  }

  /**
   * Terminate the JobWorker service.
   */
  public void terminateJobworker() {
    stop();
    terminate(1);
  }

  /**
   * Print an error message.
   *
   * @param error The error to print
   */
  @Override
  public void printError(Throwable error) {
    LOG.error("Exception in JobworkerService.", error);
  }

  @VisibleForTesting
  public JobworkerStateMachine getJobworkerStateMachine() {
    return jobworkerStateMachine;
  }

  @VisibleForTesting
  public JobworkerDetails getJobworkerDetails() {
    return jobworkerDetails;
  }

}
