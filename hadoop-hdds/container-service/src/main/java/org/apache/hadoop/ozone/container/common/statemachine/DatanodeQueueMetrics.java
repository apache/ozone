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

package org.apache.hadoop.ozone.container.common.statemachine;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.text.WordUtils;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class contains metrics related to Datanode queues.
 */
@Metrics(about = "Datanode Queue Metrics", context = OzoneConsts.OZONE)
public final class DatanodeQueueMetrics implements MetricsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeQueueMetrics.class);

  public static final String METRICS_SOURCE_NAME =
      DatanodeQueueMetrics.class.getSimpleName();

  public static final String STATE_CONTEXT_COMMAND_QUEUE_PREFIX =
      "StateContextCommandQueue";
  public static final String COMMAND_DISPATCHER_QUEUE_PREFIX =
      "CommandDispatcherCommandQueue";
  public static final String INCREMENTAL_REPORT_QUEUE_PREFIX =
      "IncrementalReportQueue";
  public static final String CONTAINER_ACTION_QUEUE_PREFIX =
      "ContainerActionQueue";
  public static final String PIPELINE_ACTION_QUEUE_PREFIX =
      "PipelineActionQueue";

  private DatanodeStateMachine datanodeStateMachine;
  private static DatanodeQueueMetrics instance;

  private Map<SCMCommandProto.Type, MetricsInfo> stateContextCommandQueueMap;
  private Map<SCMCommandProto.Type, MetricsInfo> commandDispatcherQueueMap;
  private Map<InetSocketAddress, MetricsInfo> incrementalReportsQueueMap;
  private Map<InetSocketAddress, MetricsInfo> containerActionQueueMap;
  private Map<InetSocketAddress, MetricsInfo> pipelineActionQueueMap;

  public DatanodeQueueMetrics(DatanodeStateMachine datanodeStateMachine) {
    this.datanodeStateMachine = datanodeStateMachine;

    initializeQueues();
  }

  public static synchronized DatanodeQueueMetrics create(DatanodeStateMachine
      datanodeStateMachine) {
    if (instance != null) {
      return instance;
    }
    instance = DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Queue metrics in Datanode",
        new DatanodeQueueMetrics(datanodeStateMachine));
    return instance;
  }

  private void initializeQueues() {
    // Add queue from StateContext.commandQueue
    stateContextCommandQueueMap = new HashMap<>();
    for (SCMCommandProto.Type type: SCMCommandProto.Type.values()) {
      stateContextCommandQueueMap.put(type, getMetricsInfo(
          STATE_CONTEXT_COMMAND_QUEUE_PREFIX, String.valueOf(type)));
    }

    // Add queue from DatanodeStateMachine.commandDispatcher
    commandDispatcherQueueMap = new HashMap<>();
    for (SCMCommandProto.Type type: SCMCommandProto.Type.values()) {
      commandDispatcherQueueMap.put(type, getMetricsInfo(
          COMMAND_DISPATCHER_QUEUE_PREFIX, String.valueOf(type)));
    }

    // Initialize queue for StateContext.incrementalReportQueue,
    // containerActionQueue, pipelineActionQueue
    incrementalReportsQueueMap = new HashMap<>();
    containerActionQueueMap = new HashMap<>();
    pipelineActionQueueMap = new HashMap<>();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME);

    EnumCounters<SCMCommandProto.Type> tmpEnum =
        datanodeStateMachine.getContext().getCommandQueueSummary();
    for (Map.Entry<SCMCommandProto.Type, MetricsInfo> entry:
        stateContextCommandQueueMap.entrySet()) {
      builder.addGauge(entry.getValue(),
          tmpEnum.get(entry.getKey()));
    }

    tmpEnum = datanodeStateMachine.getCommandDispatcher()
        .getQueuedCommandCount();
    for (Map.Entry<SCMCommandProto.Type, MetricsInfo> entry:
        commandDispatcherQueueMap.entrySet()) {
      builder.addGauge(entry.getValue(),
          tmpEnum.get(entry.getKey()));
    }

    for (Map.Entry<InetSocketAddress, MetricsInfo> entry:
        incrementalReportsQueueMap.entrySet()) {
      builder.addGauge(entry.getValue(),
          datanodeStateMachine.getContext()
              .getIncrementalReportQueueSize().getOrDefault(entry.getKey(), 0));
    }
    for (Map.Entry<InetSocketAddress, MetricsInfo> entry:
        containerActionQueueMap.entrySet()) {
      builder.addGauge(entry.getValue(),
          datanodeStateMachine.getContext()
              .getContainerActionQueueSize().getOrDefault(entry.getKey(), 0));
    }
    for (Map.Entry<InetSocketAddress, MetricsInfo> entry:
        pipelineActionQueueMap.entrySet()) {
      builder.addGauge(entry.getValue(),
          datanodeStateMachine.getContext().getPipelineActionQueueSize()
              .getOrDefault(entry.getKey(), 0));
    }
  }

  public static synchronized void unRegister() {
    instance = null;
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void addEndpoint(InetSocketAddress endpoint) {
    incrementalReportsQueueMap.computeIfAbsent(endpoint,
        k -> getMetricsInfo(INCREMENTAL_REPORT_QUEUE_PREFIX,
            CaseFormat.UPPER_UNDERSCORE
                .to(CaseFormat.UPPER_CAMEL, k.getHostName())));
    containerActionQueueMap.computeIfAbsent(endpoint,
        k -> getMetricsInfo(CONTAINER_ACTION_QUEUE_PREFIX,
            CaseFormat.UPPER_UNDERSCORE
                .to(CaseFormat.UPPER_CAMEL, k.getHostName())));
    pipelineActionQueueMap.computeIfAbsent(endpoint,
        k -> getMetricsInfo(PIPELINE_ACTION_QUEUE_PREFIX,
            CaseFormat.UPPER_UNDERSCORE
                .to(CaseFormat.UPPER_CAMEL, k.getHostName())));
  }

  public void removeEndpoint(InetSocketAddress endpoint) {
    incrementalReportsQueueMap.remove(endpoint);
    containerActionQueueMap.remove(endpoint);
    pipelineActionQueueMap.remove(endpoint);
  }

  @VisibleForTesting
  public int getIncrementalReportsQueueMapSize() {
    return incrementalReportsQueueMap.size();
  }

  @VisibleForTesting
  public int getContainerActionQueueMapSize() {
    return containerActionQueueMap.size();
  }

  @VisibleForTesting
  public int getPipelineActionQueueMapSize() {
    return pipelineActionQueueMap.size();
  }

  private MetricsInfo getMetricsInfo(String prefix, String metricName) {
    String metric = prefix + WordUtils.capitalize(metricName) + "Size";
    String description = "Queue size of " + metricName + " from " + prefix;
    return info(metric, description);
  }
}
