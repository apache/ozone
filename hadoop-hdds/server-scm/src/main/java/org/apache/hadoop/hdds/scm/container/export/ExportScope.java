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

package org.apache.hadoop.hdds.scm.container.export;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;

/**
 * Container listing filters for an export job.
 * An export job filters containers by {@link ContainerHealthState}, {@link LifeCycleState} or both.
 * Example archive name:
 * {@code container-ids_health-MISSING_lifecycle-OPEN_20260101T120000Z_job{jobId}.tar.gz}
 */
public final class ExportScope {

  private static final String ANY = "ANY";
  private final LifeCycleState lifeCycleState;
  private final ContainerHealthState healthState;
  private final String value;

  private ExportScope(LifeCycleState lifeCycleState, ContainerHealthState healthState, String value) {
    this.lifeCycleState = lifeCycleState;
    this.healthState = healthState;
    this.value = value;
  }

  public static ExportScope of(LifeCycleState lifeCycleState, ContainerHealthState healthState) {
    String health = healthState != null ? healthState.name() : ANY;
    String lifecycle = lifeCycleState != null ? lifeCycleState.name() : ANY;
    String value = "health-" + health + "_lifecycle-" + lifecycle;
    return new ExportScope(lifeCycleState, healthState, value);
  }

  public LifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  public ContainerHealthState getHealthState() {
    return healthState;
  }

  /**
   * Stable filter name segment used in export TAR and shard file names.
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
