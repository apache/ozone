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

package org.apache.hadoop.ozone.container.common.report;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReportManager is responsible for managing all the {@link ReportPublisher}
 * and also provides {@link ScheduledExecutorService} to ReportPublisher
 * which should be used for scheduling the reports.
 */
public final class ReportManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReportManager.class);

  private final StateContext context;
  private final List<ReportPublisher> publishers;
  private final ScheduledExecutorService executorService;

  /**
   * Construction of {@link ReportManager} should be done via
   * {@link ReportManager.Builder}.
   *
   * @param context    StateContext which holds the report
   * @param publishers List of publishers which generates report
   */
  private ReportManager(StateContext context, List<ReportPublisher> publishers,
      String threadNamePrefix) {
    this.context = context;
    this.publishers = publishers;
    this.executorService = HadoopExecutors.newScheduledThreadPool(
        publishers.size(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(threadNamePrefix +
                "DatanodeReportManager-%d").build());
  }

  /**
   * Initializes ReportManager, also initializes all the configured
   * report publishers.
   */
  public void init() {
    for (ReportPublisher publisher : publishers) {
      publisher.init(context, executorService);
    }
  }

  /**
   * Shutdown the ReportManager.
   */
  public void shutdown() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown Report Manager", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns new {@link ReportManager.Builder} which can be used to construct.
   * {@link ReportManager}
   * @param conf  - Conf
   * @return builder - Builder.
   */
  public static Builder newBuilder(ConfigurationSource conf) {
    return new Builder(conf);
  }

  /**
   * Builder to construct {@link ReportManager}.
   */
  public static final class Builder {

    private StateContext stateContext;
    private List<ReportPublisher> reportPublishers;
    private ReportPublisherFactory publisherFactory;
    private String threadNamePrefix = "";

    private Builder(ConfigurationSource conf) {
      this.reportPublishers = new ArrayList<>();
      this.publisherFactory = new ReportPublisherFactory(conf);
    }

    /**
     * Sets the {@link StateContext}.
     *
     * @param context StateContext

     * @return ReportManager.Builder
     */
    public Builder setStateContext(StateContext context) {
      stateContext = context;
      return this;
    }

    /**
     * Adds publisher for the corresponding report.
     *
     * @param report report for which publisher needs to be added
     *
     * @return ReportManager.Builder
     */
    public Builder addPublisherFor(Class<? extends Message> report) {
      reportPublishers.add(publisherFactory.getPublisherFor(report));
      return this;
    }

    /**
     * Adds new ReportPublisher to the ReportManager.
     *
     * @param publisher ReportPublisher
     *
     * @return ReportManager.Builder
     */
    public Builder addPublisher(ReportPublisher publisher) {
      reportPublishers.add(publisher);
      return this;
    }

    public Builder addThreadNamePrefix(String threadPrefix) {
      this.threadNamePrefix = threadPrefix;
      return this;
    }

    /**
     * Build and returns ReportManager.
     *
     * @return {@link ReportManager}
     */
    public ReportManager build() {
      Objects.requireNonNull(stateContext, "stateContext == null");
      return new ReportManager(
          stateContext, reportPublishers, threadNamePrefix);
    }

  }
}
