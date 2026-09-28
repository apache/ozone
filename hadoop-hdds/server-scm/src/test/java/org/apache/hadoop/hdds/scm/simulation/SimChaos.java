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

package org.apache.hadoop.hdds.scm.simulation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Injects faults during the chaos phase. Every fault is one a real cluster can suffer, and none destroys the last good
 * copy of any data, so after the faults stop SCM must be able to repair the cluster completely.
 */
final class SimChaos {

  private static final long MIN_SCM_RESTART_INTERVAL_MS = 20 * 60_000;

  private final ScmSimulation sim;
  private final List<SimScheduler.Timer> recoveries = new ArrayList<>();
  private final Set<SimDatanode> inMaintenance = new LinkedHashSet<>();
  private SimScheduler.Timer timer;
  private long lastScmRestart = Long.MIN_VALUE / 2;

  SimChaos(ScmSimulation sim) {
    this.sim = sim;
  }

  void start() {
    scheduleNext();
  }

  /** Stops injecting faults and brings every datanode back. */
  void stopAndHeal() {
    sim.scheduler().cancel(timer);
    recoveries.forEach(sim.scheduler()::cancel);
    recoveries.clear();
    for (SimDatanode dn : sim.datanodes()) {
      if (dn.isPartitioned()) {
        heal(dn);
      }
      if (!dn.isRunning()) {
        restart(dn);
      }
    }
    for (SimDatanode dn : inMaintenance) {
      endMaintenanceEventually(dn);
    }
    inMaintenance.clear();
  }

  /** Recommissions the node, retrying until SCM accepts it (it may not know the node yet after a restart). */
  private void endMaintenanceEventually(SimDatanode dn) {
    if (!sim.recommission(dn)) {
      sim.scheduler().schedule("chaos.recommissionRetry " + dn.getName(), 30_000, () -> endMaintenanceEventually(dn));
    }
  }

  private Random random() {
    return sim.random(SimRandom.FAULTS);
  }

  private void scheduleNext() {
    long delay = (long) (sim.config().getFaultIntervalMs() * (0.5 + random().nextDouble()));
    timer = sim.scheduler().schedule("chaos.inject", delay, () -> {
      inject();
      scheduleNext();
    });
  }

  private void inject() {
    int total = 0;
    for (Fault f : Fault.values()) {
      total += weight(f);
    }
    int pick = random().nextInt(total);
    Fault fault = null;
    for (Fault f : Fault.values()) {
      pick -= weight(f);
      if (pick < 0) {
        fault = f;
        break;
      }
    }
    boolean applied = apply(fault);
    sim.count("chaos." + fault + (applied ? "" : ".skipped"));
  }

  private boolean apply(Fault fault) {
    switch (fault) {
    case CRASH:
      return crash();
    case PARTITION:
      return partition();
    case LOSE_REPLICAS:
      return loseReplicas();
    case CORRUPT_REPLICA:
      return corruptReplica();
    case DECOMMISSION:
      return decommission();
    case DECOMMISSION_RACK:
      return decommissionRack();
    case MAINTENANCE:
      return maintenance();
    case ADD_DATANODE:
      return addDatanode();
    case RESTART_SCM:
      return restartScm();
    case RESTART_WITH_NEW_PORTS:
      return restartWithNewPorts();
    default:
      throw new IllegalStateException("Unknown fault " + fault);
    }
  }

  private boolean crash() {
    return outage("crash", SimDatanode::stop, "restart", this::restart);
  }

  private boolean partition() {
    return outage("partition", dn -> dn.setPartitioned(true), "heal", this::heal);
  }

  /** Takes an available datanode out for a while, unless too many are out already. */
  private boolean outage(String fault, Consumer<SimDatanode> apply, String recovery, Consumer<SimDatanode> recover) {
    SimDatanode dn = pick(available());
    if (dn == null || unavailableCount() >= sim.config().getMaxUnavailableDatanodes()) {
      return false;
    }
    sim.record("chaos." + fault, dn.getName());
    apply.accept(dn);
    recoveries.add(sim.scheduler().schedule("chaos." + recovery + " " + dn.getName(), outageMs(),
        () -> recover.accept(dn)));
    return true;
  }

  /** A failed disk loses replicas whose data another datanode also has. */
  private boolean loseReplicas() {
    SimDatanode dn = pick(available());
    if (dn == null) {
      return false;
    }
    List<Long> candidates = safeToDamage(dn);
    if (candidates.isEmpty()) {
      return false;
    }
    int count = 1 + random().nextInt(Math.min(3, candidates.size()));
    List<Long> lost = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      lost.add(candidates.remove(random().nextInt(candidates.size())));
    }
    sim.record("chaos.loseReplicas", dn.getName() + " " + lost);
    dn.loseReplicas(lost);
    return true;
  }

  private boolean corruptReplica() {
    SimDatanode dn = pick(available());
    if (dn == null) {
      return false;
    }
    List<Long> candidates = safeToDamage(dn);
    if (candidates.isEmpty()) {
      return false;
    }
    long containerId = candidates.get(random().nextInt(candidates.size()));
    sim.record("chaos.corrupt", dn.getName() + " #" + containerId);
    dn.markUnhealthy(containerId);
    return true;
  }

  private boolean decommission() {
    SimDatanode dn = pick(inService());
    if (dn == null || !enoughInServiceLeft()) {
      return false;
    }
    if (!sim.startDecommission(dn)) {
      return false;
    }
    if (random().nextBoolean()) {
      recoveries.add(sim.scheduler().schedule("chaos.recommission " + dn.getName(), longMs(),
          () -> sim.recommission(dn)));
    }
    return true;
  }

  /**
   * Decommissions every in-service datanode of a rack. They stay in the network topology, so placement keeps drawing
   * them and has to fall back to the other racks.
   */
  private boolean decommissionRack() {
    Map<String, List<SimDatanode>> racks = new TreeMap<>();
    for (SimDatanode dn : inService()) {
      racks.computeIfAbsent(dn.getRack(), k -> new ArrayList<>()).add(dn);
    }
    if (racks.size() < 2) {
      return false;
    }
    List<SimDatanode> rack = new ArrayList<>(racks.values()).get(random().nextInt(racks.size()));
    if (inService().size() - rack.size() < 3 + sim.config().getMaxUnavailableDatanodes()) {
      return false;
    }
    sim.record("chaos.decommissionRack", rack.get(0).getRack());
    List<SimDatanode> decommissioned = new ArrayList<>();
    for (SimDatanode dn : rack) {
      if (sim.startDecommission(dn)) {
        decommissioned.add(dn);
      }
    }
    if (!decommissioned.isEmpty() && random().nextBoolean()) {
      recoveries.add(sim.scheduler().schedule("chaos.recommissionRack " + rack.get(0).getRack(), longMs(),
          () -> decommissioned.forEach(sim::recommission)));
    }
    return !decommissioned.isEmpty();
  }

  private boolean maintenance() {
    SimDatanode dn = pick(inService());
    if (dn == null || !enoughInServiceLeft() || unavailableCount() >= sim.config().getMaxUnavailableDatanodes()) {
      return false;
    }
    if (!sim.startMaintenance(dn)) {
      return false;
    }
    inMaintenance.add(dn);
    if (random().nextBoolean()) {
      // Maintenance usually means the node is shut down for a while.
      recoveries.add(sim.scheduler().schedule("chaos.maintenanceShutdown " + dn.getName(),
          60_000 + random().nextInt(240_000), () -> {
            if (dn.isRunning() && unavailableCount() < sim.config().getMaxUnavailableDatanodes()) {
              sim.record("chaos.crash", dn.getName());
              dn.stop();
            }
          }));
    }
    recoveries.add(sim.scheduler().schedule("chaos.endMaintenance " + dn.getName(), longMs(), () -> {
      restart(dn);
      if (sim.recommission(dn)) {
        inMaintenance.remove(dn);
      }
    }));
    return true;
  }

  private boolean addDatanode() {
    if (sim.datanodes().size() >= sim.config().getMaxDatanodes()) {
      return false;
    }
    sim.addDatanode().start();
    return true;
  }

  /** SCM sees the datanode register again with other ports. */
  private boolean restartWithNewPorts() {
    SimDatanode dn = pick(available());
    if (dn == null) {
      return false;
    }
    sim.record("chaos.restartWithNewPorts", dn.getName());
    dn.restartWithNewPorts();
    return true;
  }

  private boolean restartScm() {
    if (sim.now() - lastScmRestart < MIN_SCM_RESTART_INTERVAL_MS) {
      return false;
    }
    lastScmRestart = sim.now();
    try {
      sim.restartScm();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return true;
  }

  private void restart(SimDatanode dn) {
    if (!dn.isRunning()) {
      sim.record("chaos.restart", dn.getName());
      dn.start();
    }
  }

  private void heal(SimDatanode dn) {
    if (dn.isPartitioned()) {
      sim.record("chaos.heal", dn.getName());
      dn.setPartitioned(false);
    }
  }

  /**
   * Outage length: short enough to go unnoticed, long enough to make the node stale or dead, or ending right when SCM
   * declares the node dead, so that the dead node handling races with the node coming back.
   */
  private long outageMs() {
    long stale = sim.config().getStaleNodeIntervalMs();
    long dead = sim.config().getDeadNodeIntervalMs();
    long check = sim.config().getHeartbeatProcessIntervalMs();
    switch (random().nextInt(4)) {
    case 0:
      return 1_000 + random().nextInt((int) stale - 1_000);
    case 1:
      return stale + random().nextInt((int) (dead - stale));
    case 2:
      return dead - check + random().nextInt((int) (3 * check));
    default:
      return dead + random().nextInt((int) (10 * dead));
    }
  }

  private long longMs() {
    return 300_000 + random().nextInt(1_500_000);
  }

  /** Closed replicas on the datanode whose data survives elsewhere if this copy is damaged. */
  private List<Long> safeToDamage(SimDatanode dn) {
    List<Long> result = new ArrayList<>();
    for (SimReplica replica : dn.getReplicas().values()) {
      if (replica.getState() != State.CLOSED && replica.getState() != State.QUASI_CLOSED) {
        continue;
      }
      for (SimDatanode other : sim.datanodes()) {
        SimReplica copy = other == dn ? null : other.getReplica(replica.getContainerId());
        if (copy != null && (copy.getState() == State.CLOSED || copy.getState() == State.QUASI_CLOSED)
            && copy.getBcsid() >= replica.getBcsid() && copy.getKeys() >= replica.getKeys()) {
          result.add(replica.getContainerId());
          break;
        }
      }
    }
    return result;
  }

  private List<SimDatanode> available() {
    List<SimDatanode> result = new ArrayList<>();
    for (SimDatanode dn : sim.datanodes()) {
      if (dn.isRunning() && !dn.isPartitioned()) {
        result.add(dn);
      }
    }
    return result;
  }

  private List<SimDatanode> inService() {
    List<SimDatanode> result = new ArrayList<>();
    for (SimDatanode dn : available()) {
      NodeStatus status = sim.nodeStatus(dn);
      if (status != null && status.getOperationalState() == NodeOperationalState.IN_SERVICE) {
        result.add(dn);
      }
    }
    return result;
  }

  /** Keeps enough in-service datanodes that SCM can re-replicate around outages. */
  private boolean enoughInServiceLeft() {
    return inService().size() - 1 >= 3 + sim.config().getMaxUnavailableDatanodes();
  }

  private int unavailableCount() {
    int count = 0;
    for (SimDatanode dn : sim.datanodes()) {
      if (!dn.isRunning() || dn.isPartitioned()) {
        count++;
      }
    }
    return count;
  }

  private int weight(Fault fault) {
    return sim.config().getFaultWeight(fault.name(), fault.weight);
  }

  private SimDatanode pick(List<SimDatanode> candidates) {
    return candidates.isEmpty() ? null : candidates.get(random().nextInt(candidates.size()));
  }

  /** Kinds of faults, with their default relative weights. */
  private enum Fault {
    CRASH(4),
    PARTITION(4),
    LOSE_REPLICAS(1),
    CORRUPT_REPLICA(1),
    DECOMMISSION(1),
    DECOMMISSION_RACK(1),
    MAINTENANCE(1),
    ADD_DATANODE(1),
    RESTART_SCM(1),
    // Off until HDDS-16630 is fixed: SCMNodeManager#register does not update the network topology when only the ports
    // change, so the datanode goes missing from it. With this fault on, the simulation can also catch HDDS-16632.
    RESTART_WITH_NEW_PORTS(0);

    private final int weight;

    Fault(int weight) {
      this.weight = weight;
    }
  }
}
