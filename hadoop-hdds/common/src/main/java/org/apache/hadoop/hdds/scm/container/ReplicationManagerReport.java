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

package org.apache.hadoop.hdds.scm.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * This class is used by ReplicationManager. Each time ReplicationManager runs,
 * it creates a new instance of this class and increments the various counters
 * to allow for creating a report on the various container states within the
 * system. There is a counter for each LifeCycleState (open, closing, closed
 * etc) and the sum of each of the lifecycle state counters should equal the
 * total number of containers in SCM. Ie, each container can only be in one of
 * the Lifecycle states at any time.
 *
 * Additionally, there are a set of counters for the "health state" of the
 * containers, defined here in the HealthState enum. It is normal for containers
 * to be in these health states from time to time, but the presence of a
 * container in one of these health states generally means cluster is in a
 * degraded state. Normally, the cluster will recover by itself, but manual
 * intervention may be needed in some cases.
 *
 * To aid debugging, when containers are in one of the health states, a list of
 * up to sampleLimit container IDs are recorded in the report for each of the
 * states.
 */
public class ReplicationManagerReport {

  private int sampleLimit;
  private long reportTimeStamp;

  private final Map<String, LongAdder> stats;
  private final Map<String, List<ContainerID>> containerSample = new ConcurrentHashMap<>();

  public static ReplicationManagerReport fromProtobuf(
      HddsProtos.ReplicationManagerReportProto proto) {
    ReplicationManagerReport report = new ReplicationManagerReport(proto.getSampleLimit());
    report.setTimestamp(proto.getTimestamp());
    for (HddsProtos.KeyIntValue stat : proto.getStatList()) {
      report.setStat(stat.getKey(), stat.getValue());
    }
    for (HddsProtos.KeyContainerIDList sample : proto.getStatSampleList()) {
      report.setSample(sample.getKey(), sample.getContainerList()
          .stream()
          .map(ContainerID::getFromProtobuf)
          .collect(Collectors.toList()));
    }
    return report;
  }

  public ReplicationManagerReport(int sampleLimit) {
    this.sampleLimit = sampleLimit;
    stats = createStatsMap();
  }

  public int getSampleLimit() {
    return sampleLimit;
  }

  public void increment(HealthState stat) {
    increment(stat.toString());
  }

  public void increment(HddsProtos.LifeCycleState stat) {
    increment(stat.toString());
  }

  public void incrementAndSample(HealthState stat, ContainerID container) {
    incrementAndSample(stat.toString(), container);
  }

  public void setComplete() {
    reportTimeStamp = System.currentTimeMillis();
  }

  /**
   * The epoch time in milli-seconds when this report was completed.
   * @return epoch time in milli-seconds.
   */
  public long getReportTimeStamp() {
    return reportTimeStamp;
  }

  /**
   * Return a map of all stats and their value as a long.
   */
  public Map<String, Long> getStats() {
    Map<String, Long> result = new HashMap<>();
    for (Map.Entry<String, LongAdder> e : stats.entrySet()) {
      result.put(e.getKey(), e.getValue().longValue());
    }
    return result;
  }

  /**
   * Return a map of all samples, with the stat as the key and the samples
   * for the stat as a List of Long.
   */
  public Map<String, List<Long>> getSamples() {
    Map<String, List<Long>> result = new HashMap<>();
    for (Map.Entry<String, List<ContainerID>> e : containerSample.entrySet()) {
      result.put(e.getKey(),
          e.getValue().stream()
              .map(c -> c.getId())
              .collect(Collectors.toList()));
    }
    return result;
  }

  /**
   * Get the stat for the given LifeCycleState. If there is no stat available
   * for that stat -1 is returned.
   * @param stat The requested stat.
   * @return The stat value or -1 if it is not present
   */
  public long getStat(HddsProtos.LifeCycleState stat) {
    return getStat(stat.toString());
  }

  /**
   * Get the stat for the given HealthState. If there is no stat available
   * for that stat -1 is returned.
   * @param stat The requested stat.
   * @return The stat value or -1 if it is not present
   */
  public long getStat(HealthState stat) {
    return getStat(stat.toString());
  }

  /**
   * Returns the stat requested, or -1 if it does not exist.
   * @param stat The request stat
   * @return The value of the stat or -1 if it does not exist.
   */
  private long getStat(String stat) {
    LongAdder val = stats.get(stat);
    if (val == null) {
      return -1;
    }
    return val.longValue();
  }

  protected void setTimestamp(long timestamp) {
    this.reportTimeStamp = timestamp;
  }

  protected void setStat(String stat, long value) {
    LongAdder adder = stats.get(stat);
    if (adder == null) {
      // this is an unknown stat, so ignore it.
      return;
    }
    if (adder.longValue() != 0) {
      throw new IllegalStateException(stat + " is expected to be zero");
    }
    adder.add(value);
  }

  protected void setSample(String stat, List<ContainerID> sample) {
    LongAdder adder = stats.get(stat);
    if (adder == null) {
      // this is an unknown stat, so ignore it.
      return;
    }
    // Now check there is not already a sample for this stat
    List<ContainerID> existingSample = containerSample.get(stat);
    if (existingSample != null) {
      throw new IllegalStateException(stat
          + " is not expected to have existing samples");
    }
    containerSample.put(stat, sample);
  }

  public List<ContainerID> getSample(HealthState stat) {
    return getSample(stat.toString());
  }

  private List<ContainerID> getSample(String stat) {
    List<ContainerID> list = containerSample.get(stat);
    if (list == null) {
      return Collections.emptyList();
    }
    synchronized (list) {
      return new ArrayList<>(list);
    }
  }

  private void increment(String stat) {
    getStatAndEnsurePresent(stat).increment();
  }

  private LongAdder getStatAndEnsurePresent(String stat) {
    LongAdder adder = stats.get(stat);
    if (adder == null) {
      throw new IllegalArgumentException("Unexpected stat " + stat);
    }
    return adder;
  }

  private void incrementAndSample(String stat, ContainerID container) {
    increment(stat);
    List<ContainerID> list = containerSample
        .computeIfAbsent(stat, k -> new ArrayList<>());
    synchronized (list) {
      if (list.size() < sampleLimit) {
        list.add(container);
      }
    }
  }

  private Map<String, LongAdder> createStatsMap() {
    Map<String, LongAdder> map = new HashMap<>();
    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      map.put(s.toString(), new LongAdder());
    }
    for (HealthState s : HealthState.values()) {
      map.put(s.toString(), new LongAdder());
    }
    return map;
  }

  public HddsProtos.ReplicationManagerReportProto toProtobuf() {
    HddsProtos.ReplicationManagerReportProto.Builder proto =
        HddsProtos.ReplicationManagerReportProto.newBuilder();
    proto.setTimestamp(getReportTimeStamp());
    proto.setSampleLimit(getSampleLimit());

    for (Map.Entry<String, LongAdder> e : stats.entrySet()) {
      proto.addStat(HddsProtos.KeyIntValue.newBuilder()
          .setKey(e.getKey())
          .setValue(e.getValue().longValue())
          .build());
    }

    for (Map.Entry<String, List<ContainerID>> e : containerSample.entrySet()) {
      HddsProtos.KeyContainerIDList.Builder sample
          = HddsProtos.KeyContainerIDList.newBuilder();
      sample.setKey(e.getKey());
      for (ContainerID container : e.getValue()) {
        sample.addContainer(container.getProtobuf());
      }
      proto.addStatSample(sample.build());
    }
    return proto.build();
  }

  /**
   * Enum representing various health states a container can be in.
   */
  public enum HealthState {
    UNDER_REPLICATED("Containers with insufficient replicas",
        "UnderReplicatedContainers"),
    MIS_REPLICATED("Containers with insufficient racks",
        "MisReplicatedContainers"),
    OVER_REPLICATED("Containers with more replicas than required",
        "OverReplicatedContainers"),
    MISSING("Containers with no online replicas",
        "MissingContainers"),
    UNHEALTHY(
        "Containers Closed or Quasi_Closed having some replicas in " +
            "a different state", "UnhealthyContainers"),
    EMPTY("Containers having no blocks", "EmptyContainers"),
    OPEN_UNHEALTHY(
        "Containers open and having replicas with different states",
        "OpenUnhealthyContainers"),
    QUASI_CLOSED_STUCK(
        "Containers QuasiClosed with insufficient datanode origins",
        "StuckQuasiClosedContainers"),
    OPEN_WITHOUT_PIPELINE(
        "Containers in OPEN state without any healthy Pipeline",
        "OpenContainersWithoutPipeline");

    private String description;
    private String metricName;

    HealthState(String desc, String name) {
      this.description = desc;
      this.metricName = name;
    }

    public String getMetricName() {
      return this.metricName;
    }

    public String getDescription() {
      return this.description;
    }
  }
}
