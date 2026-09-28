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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Outcome of one simulation run. */
final class SimResult {

  private final SimConfig config;
  private final List<String> violations;
  private final Map<String, Long> counters;
  private final String traceHash;
  private final long steps;
  private final long simulatedMs;
  private final List<String> recentTrace;
  private final Path traceFile;

  @SuppressWarnings("checkstyle:parameterNumber")
  SimResult(SimConfig config, List<String> violations, Map<String, Long> counters, String traceHash, long steps,
      long simulatedMs, List<String> recentTrace, Path traceFile) {
    this.config = config;
    this.violations = violations;
    this.counters = counters;
    this.traceHash = traceHash;
    this.steps = steps;
    this.simulatedMs = simulatedMs;
    this.recentTrace = recentTrace;
    this.traceFile = traceFile;
  }

  List<String> getViolations() {
    return violations;
  }

  Map<String, Long> getCounters() {
    return counters;
  }

  long getCounter(String name) {
    return counters.getOrDefault(name, 0L);
  }

  String getTraceHash() {
    return traceHash;
  }

  long getSteps() {
    return steps;
  }

  /** Summary, and on failure what is needed to reproduce and debug the run. */
  String report() {
    StringBuilder sb = new StringBuilder();
    sb.append("SCM simulation ").append(config).append('\n')
        .append("  steps=").append(steps).append(", simulated=").append(simulatedMs / 1000).append('s')
        .append(", trace=").append(traceHash).append('\n')
        .append("  counters=").append(counters).append('\n');
    if (!violations.isEmpty()) {
      sb.append("  ").append(violations.size()).append(" violation(s):\n");
      violations.forEach(v -> sb.append("    ").append(v).append('\n'));
      sb.append("  Reproduce with -D").append(TestScmSimulation.SEED_PROPERTY).append('=').append(config.getSeed())
          .append(" -D").append(TestScmSimulation.RACKS_PROPERTY).append('=').append(config.getRacks());
      if (!config.getFaultWeights().isEmpty()) {
        sb.append(" -D").append(TestScmSimulation.FAULT_WEIGHTS_PROPERTY).append('=');
        config.getFaultWeights().forEach((fault, weight) -> sb.append(fault).append(':').append(weight).append(','));
        sb.setLength(sb.length() - 1);
      }
      sb.append('\n');
      if (traceFile != null) {
        sb.append("  Trace written to ").append(traceFile).append('\n');
      }
      sb.append("  Last trace lines:\n");
      int from = Math.max(0, recentTrace.size() - 40);
      recentTrace.subList(from, recentTrace.size()).forEach(l -> sb.append("    ").append(l).append('\n'));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return report();
  }
}
