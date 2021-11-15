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

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStateManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.Map;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * The capacity pipeline choose policy that chooses pipeline
 * until return capacity relatively small pipeline.
 */
public class CapacityPipelineChoosePolicy implements PipelineChoosePolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacityPipelineChoosePolicy.class);

  private final NodeStateManager nodeStateManager;
  private final double criticalUtilizationThreshold;

  private final Map<UUID, Pair<Double, Double>> usedSpaceMap;

  public CapacityPipelineChoosePolicy(NodeStateManager nodeStateManager) {
    this.nodeStateManager = nodeStateManager;

    criticalUtilizationThreshold =
        this.nodeStateManager.getConfiguration().getDouble(
            OzoneConfigKeys.
                HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD,
            OzoneConfigKeys.
                HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD_DEFAULT);

    usedSpaceMap = new ConcurrentHashMap<>();

    ScheduledExecutorService cleanOzoneClientExecutor =
        Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("CapacityPipelineChoosePolicy-usedRatio" +
                    "-updater")
                .build());

    cleanOzoneClientExecutor.scheduleWithFixedDelay(() -> {
      long currentTimeMillis = System.currentTimeMillis();

      List<DatanodeInfo> allNodes = this.nodeStateManager.getAllNodes();

      StringBuilder sb = new StringBuilder();

      for (DatanodeInfo node : allNodes) {

        Pair<Double, Double> usedSpace = getUsedSpace(node);

        double usedRatio = usedSpace.getKey() / usedSpace.getValue();
        usedSpaceMap.put(node.getUuid(), usedSpace);
        sb.append("(<").append(node.getUuid()).append(", ")
            .append(node.getIpAddress()).append("> ")
            .append(usedRatio).append("); ");
      }

      if (LOG.isDebugEnabled()) {
        long cost = System.currentTimeMillis() - currentTimeMillis;

        LOG.debug("update usedRatioMap {} cost: {}ms", sb.toString(), cost);
      }

    }, MINUTES.toMillis(2), MINUTES.toMillis(1), MILLISECONDS);

    if (LOG.isDebugEnabled()) {
      LOG.debug("use pipeline choose policy: CapacityPipelineChoosePolicy, " +
              "criticalUtilizationThreshold: {}",
          criticalUtilizationThreshold);
    }

  }

  private Pair<Double, Double> getUsedSpace(DatanodeInfo datanodeInfo) {
    double capacity = 0L;
    double used = 0L;

    final List<StorageContainerDatanodeProtocolProtos.StorageReportProto>
        storageReportProtos = datanodeInfo
        .getStorageReports();
    for (StorageContainerDatanodeProtocolProtos.StorageReportProto
        reportProto : storageReportProtos) {
      capacity += reportProto.getCapacity();
      used += reportProto.getScmUsed();
    }

    return Pair.of(used, capacity);
  }

  private double getUsedRatioInternal(DatanodeDetails node, long size) {
    Pair<Double, Double> usedSpace = usedSpaceMap.get(node.getUuid());

    if (usedSpace != null) {
      return (usedSpace.getKey() + size) / usedSpace.getValue();
    } else {
      return 0D;
    }
  }

  private Pipeline choosePipelineWithUseSpace(List<Pipeline> pipelineList) {
    TreeSet<DataNodePipelineCandidate> candidates = new TreeSet<>();

    for (Pipeline pipeline : pipelineList) {
      HashMap<UUID, Pair<Double, Double>> dataNodeUsedSpace = new HashMap<>();

      for (DatanodeDetails node : pipeline.getNodes()) {
        if (usedSpaceMap.get(node.getUuid()) == null) {
          return pipelineList.get((int) (Math.random() * pipelineList.size()));
        }
        dataNodeUsedSpace.put(node.getUuid(), usedSpaceMap.get(node.getUuid()));
      }

      candidates
          .add(new DataNodePipelineCandidate(pipeline, dataNodeUsedSpace));
    }

    double min =
        candidates.last().getDataNodeRemainingRadio().doubleValue() * 0.99;

    // weighted random
    double rnd = ThreadLocalRandom.current().nextDouble();
    for (DataNodePipelineCandidate candidate : candidates) {
      rnd -= (candidate.getDataNodeRemainingRadio().doubleValue() - min);
      if (rnd <= 0) {
        return candidate.getPipeline();
      }
    }

    return pipelineList.get((int) (Math.random() * pipelineList.size()));
  }

  static class DataNodePipelineCandidate
      implements Comparable<DataNodePipelineCandidate> {
    private final Pipeline pipeline;

    private final Map<UUID, Pair<Double, Double>> dataNodeUsedSpace;

    DataNodePipelineCandidate(Pipeline pipeline,
                              Map<UUID, Pair<Double, Double>>
                                  dataNodeUsedSpace) {
      this.pipeline = pipeline;
      this.dataNodeUsedSpace = dataNodeUsedSpace;
    }

    public Pipeline getPipeline() {
      return pipeline;
    }

    public Map<UUID, Pair<Double, Double>> getDataNodeUsedSpace() {
      return dataNodeUsedSpace;
    }

    public Double getDataNodeRemainingRadio() {
      double use = 0D;
      double capacity = 0D;
      for (Map.Entry<UUID, Pair<Double, Double>> entry : dataNodeUsedSpace
          .entrySet()) {
        use += entry.getValue().getKey();
        capacity += entry.getValue().getValue();
      }
      return 1 - (use / capacity);
    }

    @Override
    public int compareTo(@NotNull DataNodePipelineCandidate o) {
      return Double
          .compare(o.getDataNodeRemainingRadio(), getDataNodeRemainingRadio());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DataNodePipelineCandidate)) {
        return false;
      }

      DataNodePipelineCandidate that = (DataNodePipelineCandidate) obj;

      return new EqualsBuilder()
          .append(getDataNodeRemainingRadio(), that.getDataNodeRemainingRadio())
          .append(getPipeline(), that.getPipeline())
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(11, 811)
          .append(getDataNodeRemainingRadio())
          .append(getPipeline())
          .toHashCode();
    }
  }

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
                                 PipelineRequestInformation pri) {

    if (usedSpaceMap.size() <=
        pipelineList.get(0).getReplicationConfig().getRequiredNodes()) {
      Pipeline pipeline =
          pipelineList.get((int) (Math.random() * pipelineList.size()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("choose pipeline: {}, usedSpaceMap.size: {}, factor: {}",
            pipeline.getId().getId(), usedSpaceMap.size(),
            pipeline.getReplicationConfig().getRequiredNodes());
      }
      return pipeline;
    }

    Pipeline other = null;

    while (pipelineList.size() > 0) {

      Pipeline pipeline = choosePipelineWithUseSpace(pipelineList);

      if (pipeline.isHealthy()) {

        Set<DatanodeDetails> nodeSet = pipeline.getNodeSet();

        HashMap<UUID, Double> usedRatioMap = new HashMap<>();
        boolean choose = true;

        for (DatanodeDetails datanodeDetails : nodeSet) {
          double usedRatio =
              getUsedRatioInternal(datanodeDetails, pri.getSize());

          if (usedRatio >= criticalUtilizationThreshold) {
            choose = false;
            break;
          }
          usedRatioMap.put(datanodeDetails.getUuid(), usedRatio);
        }

        if (choose) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("choose pipeline: {}, usedRatio: {}",
                pipeline.getId().getId(), usedRatioMap);
          }
          return pipeline;
        }
      }
      pipelineList.remove(pipeline);
      other = pipeline;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("choose other pipeline: {}", other);
    }
    return other;
  }
}
