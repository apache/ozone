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

import static org.apache.hadoop.hdds.scm.container.SimplifiedContainerHealthStateForReporting.HEALTHY;
import static org.apache.hadoop.hdds.scm.container.SimplifiedContainerHealthStateForReporting.MISSING;
import static org.apache.hadoop.hdds.scm.container.SimplifiedContainerHealthStateForReporting.UNDER_REPLICATED;
import static org.apache.hadoop.hdds.scm.container.SimplifiedContainerHealthStateForReporting.UNHEALTHY_UNDER_REPLICATED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests for the SimplifiedContainerHealthStateForReporting enum.
 */
public class TestSimplifiedContainerHealthStateForReporting {

  @Test
  public void testDescriptions() {
    assertEquals("Containers with no problem found by ReplicationManager. Always 0, never counted",
        HEALTHY.getDescription());
    assertEquals("Containers with fewer replicas than required",
        UNDER_REPLICATED.getDescription());
    assertEquals("Containers with no replicas, or EC containers with fewer indexes than data blocks",
        MISSING.getDescription());
  }

  @Test
  public void testMetricNames() {
    assertEquals("HealthyContainers", HEALTHY.getMetricName());
    assertEquals("UnderReplicatedContainers", UNDER_REPLICATED.getMetricName());
    assertEquals("MissingContainers", MISSING.getMetricName());
    assertEquals("UnhealthyUnderReplicatedContainers", UNHEALTHY_UNDER_REPLICATED.getMetricName());
  }
}
