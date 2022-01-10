/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

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
 * up to SAMPLE_LIMIT container IDs are recorded in the report for each of the
 * states.
 */
public class ReplicationManagerReport {

  public static final int SAMPLE_LIMIT = 100;
  private long reportTimeStamp;

  /**
   * Enum representing various health states a container can be in.
   */
  public enum HealthState {
    UNDER_REPLICATED("Containers with insufficient replicas",
        "NumUnderReplicatedContainers"),
    MIS_REPLICATED("Containers with insufficient racks",
        "NumMisReplicatedContainers"),
    OVER_REPLICATED("Containers with more replicas than required",
        "NumOverReplicatedContainers"),
    MISSING("Containers with no online replicas",
        "NumMissingContainers"),
    UNHEALTHY(
        "Containers Closed or Quasi_Closed having some replicas in " +
            "a different state", "NumUnhealthyContainers"),
    EMPTY("Containers having no blocks", "NumEmptyContainers"),
    OPEN_UNHEALTHY(
        "Containers open and having replicas with different states",
        "NumOpenUnhealthyContainers"),
    QUASI_CLOSED_STUCK(
        "Containers QuasiClosed with insufficient datanode origins",
        "NumStuckQuasiClosedContainers");

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

  private final Map<String, LongAdder> stats;
  private final Map<String, List<ContainerID>> containerSample
      = new ConcurrentHashMap<>();

  public ReplicationManagerReport() {
    stats = createStatsMap();
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

  public void incrementAndSample(HddsProtos.LifeCycleState stat,
      ContainerID container) {
    incrementAndSample(stat.toString(), container);
  }

  public void setComplete() {
    reportTimeStamp = System.currentTimeMillis();
  }

  public long getStat(HddsProtos.LifeCycleState stat) {
    return getStat(stat.toString());
  }

  public long getStat(HealthState stat) {
    return getStat(stat.toString());
  }

  private long getStat(String stat) {
    return stats.get(stat).longValue();
  }

  public List<ContainerID> getSample(HddsProtos.LifeCycleState stat) {
    return getSample(stat.toString());
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
    LongAdder adder = stats.get(stat);
    if (adder == null) {
      throw new RuntimeException("Unexpected stat " + stat);
    }
    adder.increment();
  }

  private void incrementAndSample(String stat, ContainerID container) {
    increment(stat);
    List<ContainerID> list = containerSample
        .computeIfAbsent(stat, k -> new ArrayList<>());
    synchronized(list) {
      if (list.size() < SAMPLE_LIMIT) {
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

}
