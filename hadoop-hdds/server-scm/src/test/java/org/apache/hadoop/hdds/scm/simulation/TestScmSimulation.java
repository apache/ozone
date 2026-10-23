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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.container.replication.UnhealthyReplicationProcessor;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Deterministic simulation tests of SCM: the real StorageContainerManager runs against simulated datanodes while faults
 * are injected, and invariants are checked (see {@link ScmSimulation}).
 * <p>
 * Each run is fully determined by its seed. To reproduce a failure, rerun the seed printed in the report (a
 * comma-separated list runs several):
 * <pre>
 *   mvn -pl :hdds-server-scm test -Dtest=TestScmSimulation#chaos -Dscm.simulation.seed=SEED
 * </pre>
 * To explore new seeds, pass {@code -Dscm.simulation.random.seeds=N}, and {@code -Dscm.simulation.chaos.hours=H} for
 * longer runs, and {@code -Dscm.simulation.racks=N} (or {@code random}, which picks it from the seed) for other
 * topologies than 3 racks, and {@code -Dscm.simulation.fault.weights=RESTART_WITH_NEW_PORTS:1,CRASH:0} to change how
 * often each kind of fault is injected. The trace of a failed run is written to {@code target/scm-simulation}; pass
 * {@code -Dscm.simulation.full.trace=true} to keep the whole trace of every run.
 * <p>
 * {@code -Dscm.simulation.scms=3} runs SCM HA. With it, {@code -Dscm.simulation.snapshot.threshold=N} makes the SCMs
 * take a snapshot every N log entries, and {@code -Dscm.simulation.purge.gap=G} makes them purge their Raft logs up to
 * their snapshots, at least G entries at a time, so that an SCM that falls behind installs a snapshot.
 */
public class TestScmSimulation {

  static final String SEED_PROPERTY = "scm.simulation.seed";
  static final String RANDOM_SEEDS_PROPERTY = "scm.simulation.random.seeds";
  static final String CHAOS_HOURS_PROPERTY = "scm.simulation.chaos.hours";
  static final String FULL_TRACE_PROPERTY = "scm.simulation.full.trace";
  static final String RACKS_PROPERTY = "scm.simulation.racks";
  static final String FAULT_WEIGHTS_PROPERTY = "scm.simulation.fault.weights";
  static final String WATCH_PROPERTY = "scm.simulation.watch";
  static final String SCMS_PROPERTY = "scm.simulation.scms";
  static final String SNAPSHOT_THRESHOLD_PROPERTY = "scm.simulation.snapshot.threshold";
  static final String PURGE_GAP_PROPERTY = "scm.simulation.purge.gap";
  static final String WORKAROUNDS_PROPERTY = "scm.simulation.workarounds";
  /** Rack counts for -Dscm.simulation.racks=random; 12 racks give prefix-sharing names like /rack-1 and /rack-10. */
  private static final int[] RANDOM_RACKS = {2, 3, 4, 12};

  private static final Logger LOG = LoggerFactory.getLogger(TestScmSimulation.class);
  private static final Logger SCM_LOG = LoggerFactory.getLogger("org.apache.hadoop.hdds.scm");

  @TempDir
  private Path dir;

  /**
   * SCM logs each command it sends and each placement it tries, megabytes per simulated hour, too much for a batch of
   * seeds. The trace of a failed run tells what happened.
   */
  @BeforeAll
  static void logOnlyScmWarnings() {
    GenericTestUtils.setLogLevel(SCM_LOG, Level.WARN);
    GenericTestUtils.setLogLevel(ScmSimulation.class, Level.INFO);
    GenericTestUtils.setLogLevel(TestScmSimulation.class, Level.INFO);
  }

  @AfterAll
  static void logScmInfo() {
    GenericTestUtils.setLogLevel(SCM_LOG, Level.INFO);
  }

  static Stream<Long> seeds() {
    String seed = System.getProperty(SEED_PROPERTY);
    if (seed != null) {
      return Arrays.stream(seed.split(",")).map(String::trim).map(Long::parseLong);
    }
    int randomSeeds = Integer.getInteger(RANDOM_SEEDS_PROPERTY, 0);
    if (randomSeeds > 0) {
      return LongStream.generate(System::nanoTime).limit(randomSeeds).boxed();
    }
    return Stream.of(1L, 2L, 3L);
  }

  @ParameterizedTest
  @MethodSource("seeds")
  void chaos(long seed) throws Exception {
    SimConfig config = new SimConfig(seed).setKeepFullTrace(Boolean.getBoolean(FULL_TRACE_PROPERTY))
        .setScmCount(Integer.getInteger(SCMS_PROPERTY, 1));
    String racks = System.getProperty(RACKS_PROPERTY);
    if ("random".equals(racks)) {
      config.setRacks(RANDOM_RACKS[new SplittableRandom(seed).nextInt(RANDOM_RACKS.length)]);
    } else if (racks != null) {
      config.setRacks(Integer.parseInt(racks));
    }
    String weights = System.getProperty(FAULT_WEIGHTS_PROPERTY);
    if (weights != null) {
      for (String weight : weights.split(",")) {
        String[] nameAndWeight = weight.split(":");
        config.setFaultWeight(nameAndWeight[0].trim(), Integer.parseInt(nameAndWeight[1].trim()));
      }
    }
    Long snapshotThreshold = Long.getLong(SNAPSHOT_THRESHOLD_PROPERTY);
    if (snapshotThreshold != null) {
      config.setScmSnapshotThreshold(snapshotThreshold);
    }
    Long purgeGap = Long.getLong(PURGE_GAP_PROPERTY);
    if (purgeGap != null) {
      config.setScmLogPurge(purgeGap);
    }
    String workarounds = System.getProperty(WORKAROUNDS_PROPERTY);
    if (workarounds != null) {
      Arrays.stream(workarounds.split(",")).map(String::trim).forEach(config::addWorkaround);
    }
    String hours = System.getProperty(CHAOS_HOURS_PROPERTY);
    if (hours != null) {
      config.setChaosDurationMs((long) (Double.parseDouble(hours) * TimeUnit.HOURS.toMillis(1)));
    }
    String watch = System.getProperty(WATCH_PROPERTY);
    if (watch != null) {
      for (String id : watch.split(",")) {
        config.watchContainer(Long.parseLong(id.trim()));
      }
    }
    SimResult result = run(config, dir.resolve("seed-" + seed));
    assertThat(result.getViolations()).as(result.report()).isEmpty();
    assertThat(result.getCounter("client.write.ok")).as(result.report()).isPositive();
  }

  /** Without faults nothing may go wrong, and the cluster must still be healthy at the end. */
  @Test
  void withoutFaults() throws Exception {
    SimConfig config = new SimConfig(7).setFaults(false).setChaosDurationMs(TimeUnit.MINUTES.toMillis(40))
        .setSettleDurationMs(TimeUnit.MINUTES.toMillis(20));
    SimResult result = run(config, dir.resolve("no-faults"));
    assertThat(result.getViolations()).as(result.report()).isEmpty();
    assertThat(result.getCounter("final.container.CLOSED")).as(result.report()).isPositive();
  }

  /**
   * SCM HA: three SCMs in a Ratis group, which crash, get cut off and change leaders. Until HDDS-16896 is fixed, an SCM
   * that crashes before its first DB flush restarts with an empty DB and leaves safe mode too early; that is counted.
   */
  @ParameterizedTest
  @MethodSource("seeds")
  void chaosWithScmHa(long seed) throws Exception {
    SimConfig config = new SimConfig(seed).setScmCount(3).addWorkaround("HDDS-16896")
        .setKeepFullTrace(Boolean.getBoolean(FULL_TRACE_PROPERTY));
    SimResult result = run(config, dir.resolve("ha-seed-" + seed));
    assertThat(result.getViolations()).as(result.report()).isEmpty();
    assertThat(result.getCounter("client.write.ok")).as(result.report()).isPositive();
  }

  /**
   * SCM HA with log purge: SCMs that fall behind install a snapshot of the leader's DB. Until HDDS-16930 is fixed,
   * every such install fails, so the simulation flushes the DB before it; HDDS-16896 as in {@link #chaosWithScmHa}.
   */
  @ParameterizedTest
  @MethodSource("seeds")
  void chaosWithScmSnapshots(long seed) throws Exception {
    SimConfig config = new SimConfig(seed).setScmCount(3).setScmSnapshotThreshold(200).setScmLogPurge(0)
        .addWorkaround("HDDS-16930").addWorkaround("HDDS-16896")
        .setKeepFullTrace(Boolean.getBoolean(FULL_TRACE_PROPERTY));
    SimResult result = run(config, dir.resolve("snapshots-seed-" + seed));
    assertThat(result.getViolations()).as(result.report()).isEmpty();
    assertThat(result.getCounter("scm.installSnapshot")).as(result.report()).isPositive();
  }

  /** The same seed must give the same run; anything else means some nondeterminism leaked in. */
  @Test
  void sameSeedSameTrace() throws Exception {
    SimResult first = run(shortRun(42), dir.resolve("first"));
    SimResult second = run(shortRun(42), dir.resolve("second"));
    assertEquals(first.getTraceHash(), second.getTraceHash(),
        "Runs with the same seed diverged:\n" + first.report() + "\n" + second.report());
    assertThat(first.getSteps()).isEqualTo(second.getSteps());
  }

  private static SimConfig shortRun(long seed) {
    return new SimConfig(seed).setChaosDurationMs(TimeUnit.MINUTES.toMillis(30))
        .setSettleDurationMs(TimeUnit.MINUTES.toMillis(15));
  }

  /**
   * Runs a simulation. Replication Manager, EC placement and EC block allocation log a warning, often with a stack
   * trace, for each placement they fail, on every pass. That adds up to gigabytes when repairs are stuck, so the
   * simulation counts failed placements instead.
   */
  private static SimResult run(SimConfig config, Path runDir) {
    SimResult[] result = new SimResult[1];
    withLogsDisabled(Arrays.asList(UnhealthyReplicationProcessor.class, SCMContainerPlacementRackScatter.class,
        WritableECContainerProvider.class), () -> {
          try (ScmSimulation simulation = new ScmSimulation(config, runDir)) {
            result[0] = simulation.run();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
    LOG.info("{}", result[0].report());
    return result[0];
  }

  private static void withLogsDisabled(List<Class<?>> classes, Runnable task) {
    if (classes.isEmpty()) {
      task.run();
    } else {
      GenericTestUtils.withLogDisabled(classes.get(0),
          () -> withLogsDisabled(classes.subList(1, classes.size()), task));
    }
  }
}
