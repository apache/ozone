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

package org.apache.hadoop.ozone.jobworker.report;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerReportManager is responsible for managing all the {@link JobworkerReportPublisher}
 * and also provides {@link ScheduledExecutorService} to JobworkerReportPublisher
 * which should be used for scheduling the reports. It also manages the report queue for
 * sending reports to OzoneManager endpoints.
 */
public final class JobworkerReportManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerReportManager.class);

  private final JobworkerStateContext context;
  private final List<JobworkerReportPublisher> publishers;
  private final ScheduledExecutorService executorService;
  // Store all reports to be sent to endpoints
  private final Map<String, Map<InetSocketAddress, List<Message>>> reportQueue;
  private final int maxReportCount;
  private final int maxReportInBytes;

  /**
   * Construction of {@link JobworkerReportManager} should be done via
   * {@link JobworkerReportManager.Builder}.
   *
   * @param context    StateContext, which holds the report
   * @param publishers List of publishers which generates a report
   * @param threadNamePrefix Thread name prefix
   * @param conf Ozone Config
   */
  private JobworkerReportManager(JobworkerStateContext context, List<JobworkerReportPublisher> publishers,
                                 String threadNamePrefix, ConfigurationSource conf) {
    this.context = context;
    this.publishers = publishers;
    this.executorService = HadoopExecutors.newScheduledThreadPool(
        publishers.size(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(threadNamePrefix +
                "JobworkerReportManager-%d").build());
    this.reportQueue = Collections.synchronizedMap(new HashMap<>());
    JobworkerConfiguration jwConf = conf.getObject(JobworkerConfiguration.class);
    maxReportCount = jwConf.getMaxReportCount();
    maxReportInBytes = jwConf.getMaxReportSizeInBytes();
  }

  /**
   * Initializes JobworkerReportManager, also initializes all the configured
   * report publishers.
   */
  public void init() {
    for (JobworkerReportPublisher publisher : publishers) {
      publisher.init(context, executorService, this);
    }
  }

  /**
   * Shutdown the JobworkerReportManager.
   */
  public void shutdown() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown Jobworker Report Manager", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Registers an endpoint with the report manager for report queuing.
   *
   * @param endpoint The endpoint to register
   */
  public void registerEndpoint(InetSocketAddress endpoint, String omServiceId) {
    reportQueue.computeIfAbsent(omServiceId, ignore -> new HashMap<>());
    synchronized (reportQueue) {
      if (!reportQueue.get(omServiceId).containsKey(endpoint)) {
        reportQueue.get(omServiceId).put(endpoint, new LinkedList<>());
        LOG.info("Registered endpoint {} omServiceId {} for report queuing", endpoint, omServiceId);
      }
    }
  }

  /**
   * Adds a report to the state context.
   *
   * @param report report to be added
   */
  public void addReport(Message report) {
    if (report == null) {
      return;
    }

    final Descriptors.Descriptor descriptor = report.getDescriptorForType();
    if (descriptor == null) {
      LOG.warn("Invalid report with null descriptor");
      return;
    }

    final String reportType = descriptor.getFullName();
    if (reportType == null) {
      LOG.warn("Invalid report with a null report type");
      return;
    }

    if (report instanceof CommandStatusReportsProto) {
      // Command status reports only need to report to the OM group that send the command.
      addCommandStatusReportForOMGroup(report);
    } else {
      addReportForAllOMGroup(report);
    }
  }

  /**
   * Adds a CommandStatusReportsProto report to a specific endpoint for a given OM service ID.
   *
   * @param omServiceId The target OM service ID
   * @param endpoint The target endpoint address
   * @param report CommandStatusReportsProto to be added
   */
  public void addCommandStatusReport(String omServiceId, InetSocketAddress endpoint,
                                     CommandStatusReportsProto report) {
    if (report == null) {
      return;
    }

    if (reportQueue.get(omServiceId) == null ||
        reportQueue.get(omServiceId).get(endpoint) == null) {
      LOG.warn("Invalid endpoint {} for omServiceId {}", endpoint, omServiceId);
      return;
    }

    synchronized (reportQueue) {
      reportQueue.get(omServiceId).get(endpoint).add(report);
      LOG.debug("Added report {} for omServiceId {} and endpoint {}",
          report.getDescriptorForType().getFullName(), omServiceId, endpoint);
    }
  }

  private void addCommandStatusReportForOMGroup(Message report) {
    Map<String, CommandStatusReportsProto.Builder> omServiceIdToReports = new HashMap<>();
    for (CommandStatus commandStatus : ((CommandStatusReportsProto) report).getCmdStatusList()) {
      String omServiceId = commandStatus.getOmServiceId();
      omServiceIdToReports.computeIfAbsent(omServiceId,
              ignore -> CommandStatusReportsProto.newBuilder())
          .addCmdStatus(commandStatus);
    }
    for (String omServiceId : omServiceIdToReports.keySet()) {
      if (reportQueue.get(omServiceId) == null) {
        LOG.warn("Invalid report with null endpoint for omServiceId {}", omServiceId);
        continue;
      }
      // Queue the report for specific OM group endpoint
      synchronized (reportQueue) {
        for (List<Message> queue : reportQueue.get(omServiceId).values()) {
          if (omServiceIdToReports.get(omServiceId).getCmdStatusList().stream()
              .anyMatch(CommandStatus::hasAttemptId)) {
            queue.removeIf(message -> message instanceof CommandStatusReportsProto
                && ((CommandStatusReportsProto) message).getCmdStatusList().stream()
                    .anyMatch(CommandStatus::hasAttemptId));
          }
          queue.add(omServiceIdToReports.get(omServiceId).build());
        }
        LOG.debug("Added report {} for omServiceId {}",
            report.getDescriptorForType().getFullName(), omServiceId);
      }
    }
  }

  private void addReportForAllOMGroup(Message report) {
    for (String omServiceId : reportQueue.keySet()) {
      for (List<Message> queue : reportQueue.get(omServiceId).values()) {
        queue.add(report);
      }
      LOG.debug("Added report {} for omServiceId {}",
          report.getDescriptorForType().getFullName(), omServiceId);
    }
  }

  /**
   * Returns a limited number of available reports for a specific endpoint.
   *
   * @param endpoint the endpoint
   * @return A limited number of lists of reports
   */
  public List<Message> getLimitedCountAvailableReports(String omServiceId, InetSocketAddress endpoint) {
    return getAllAvailableReportsUpToLimit(omServiceId, endpoint, maxReportCount, maxReportInBytes);
  }

  /**
   * Gets all available reports for a specific endpoint up to a specified count limit
   * and size limit.
   *
   * @param omServiceId The target OM service ID
   * @param endpoint The target endpoint address
   * @param countLimit Maximum number of reports to return
   * @param sizeLimitInBytes Maximum total size in bytes of all returned reports
   * @return List of reports within the given limits
   */
  public List<Message> getAllAvailableReportsUpToLimit(
      String omServiceId, InetSocketAddress endpoint,
      int countLimit, int sizeLimitInBytes) {

    List<Message> reportsToReturn = new ArrayList<>();
    if (reportQueue.get(omServiceId) == null) {
      LOG.warn("Invalid report with null endpoint for omServiceId {}, current reported omServiceId {}",
          omServiceId, reportQueue.keySet());
      return reportsToReturn;
    }

    synchronized (reportQueue) {
      List<Message> reportsForEndpoint = reportQueue.get(omServiceId).get(endpoint);
      if (reportsForEndpoint == null || reportsForEndpoint.isEmpty()) {
        return reportsToReturn;
      }

      long totalSize = 0;
      int index = 0;

      while (index < reportsForEndpoint.size() && reportsToReturn.size() < countLimit) {
        Message report = reportsForEndpoint.get(index);
        int reportSize = report.getSerializedSize();

        if (totalSize + reportSize > sizeLimitInBytes) {
          LOG.warn("Adding report would exceed the total size limit: {} + {} > {} bytes",
              totalSize, reportSize, sizeLimitInBytes);
          break;
        }

        reportsToReturn.add(report);
        totalSize += reportSize;
        index++;
      }

      if (!reportsToReturn.isEmpty()) {
        reportsForEndpoint.subList(0, reportsToReturn.size()).clear();
        LOG.debug("Retrieved {} reports (total size: {} bytes) for endpoint {}",
            reportsToReturn.size(), totalSize, endpoint);
      }
    }

    return reportsToReturn;
  }

  /**
   * Returns new {@link JobworkerReportManager.Builder} which can be used to construct.
   * {@link JobworkerReportManager}
   * @param conf  - Conf
   * @return builder - Builder.
   */
  public static Builder newBuilder(ConfigurationSource conf) {
    return new Builder(conf);
  }

  /**
   * Builder to construct {@link JobworkerReportManager}.
   */
  public static final class Builder {

    private JobworkerStateContext stateContext;
    private List<JobworkerReportPublisher> reportPublishers;
    private JobworkerReportPublisherFactory publisherFactory;
    private String threadNamePrefix = "";
    private ConfigurationSource conf;

    private Builder(ConfigurationSource conf) {
      this.reportPublishers = new ArrayList<>();
      this.publisherFactory = new JobworkerReportPublisherFactory(conf);
      this.conf = conf;
    }

    /**
     * Sets the {@link JobworkerStateContext}.
     *
     * @param context JobworkerStateContext
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder setStateContext(JobworkerStateContext context) {
      stateContext = context;
      return this;
    }

    /**
     * Adds publisher for the corresponding report.
     *
     * @param report report for which publisher needs to be added
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder addPublisherFor(Class<? extends Message> report) {
      reportPublishers.add(publisherFactory.getPublisherFor(report));
      return this;
    }

    /**
     * Adds new JobworkerReportPublisher to the JobworkerReportManager.
     *
     * @param publisher JobworkerReportPublisher
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder addPublisher(JobworkerReportPublisher publisher) {
      reportPublishers.add(publisher);
      return this;
    }

    public Builder addThreadNamePrefix(String threadPrefix) {
      this.threadNamePrefix = threadPrefix;
      return this;
    }

    /**
     * Build and returns JobworkerReportManager.
     *
     * @return {@link JobworkerReportManager}
     */
    public JobworkerReportManager build() {
      Objects.requireNonNull(stateContext);
      return new JobworkerReportManager(
          stateContext, reportPublishers, threadNamePrefix, conf);
    }
  }
}
