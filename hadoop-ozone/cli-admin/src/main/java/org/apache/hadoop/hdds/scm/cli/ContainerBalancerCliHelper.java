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

package org.apache.hadoop.hdds.scm.cli;

import static org.apache.hadoop.util.StringUtils.byteDesc;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerEstimation;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerProfile;

/** Shared parsing and CLI output for container balancer subcommands. */
public final class ContainerBalancerCliHelper {

  /** Multiplier for recommended/planning iteration counts and CLI planning display (+30%). */
  public static final double PLANNING_ITERATION_BUFFER = 1.3d;

  private ContainerBalancerCliHelper() {
  }

  public static ContainerBalancerProfile parseProfile(String name) throws IOException {
    try {
      return ContainerBalancerProfile.valueOf(name.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid profile: " + name + ". Expected SLOW, MEDIUM, or FAST.");
    }
  }

  public static Set<String> parseNodeSet(String nodes) {
    if (StringUtils.isBlank(nodes)) {
      return Collections.emptySet();
    }
    return Arrays.stream(nodes.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }

  public static void printEstimation(PrintWriter out, ContainerBalancerEstimation estimation) {
    long estimatedIterations = estimation.getEstimatedIterations();
    long planningIterations = (long) Math.ceil(estimatedIterations * PLANNING_ITERATION_BUFFER);
    long cycleTimeMillis = estimation.getMoveTimeoutMillis() + estimation.getBalancingIntervalMillis();
    long baseDurationMillis = estimation.getEstimatedDurationMillis();
    long planningDurationMillis = planningIterations * cycleTimeMillis;
    out.printf(" Bytes to move:            %s%n", byteDesc(estimation.getBytesToMove()));
    out.printf(" Per iteration (estimate): ~%s%n", byteDesc(estimation.getPerIterationBytes()));
    out.printf(" Estimated iterations:     %d (planning estimate: %d, includes +30%% buffer)%n",
        estimatedIterations, planningIterations);
    out.printf(" Estimated duration:       upper bound %s (planning estimate: %s, includes +30%% buffer)%n",
        formatEstimatedDuration(baseDurationMillis),
        formatEstimatedDuration(planningDurationMillis));
    out.println("                           (assumes full move timeout + interval each cycle)");
    out.println();
  }

  public static String formatEstimatedDuration(long durationMillis) {
    double days = durationMillis / 86400000d;
    if (days >= 1) {
      return String.format(Locale.ENGLISH, "~%.1f days", days);
    }
    double hours = durationMillis / 3600000d;
    if (hours >= 1) {
      return String.format(Locale.ENGLISH, "~%.1f hours", hours);
    }
    long minutes = durationMillis / 60000;
    return String.format(Locale.ENGLISH, "~%d min", minutes);
  }
}
