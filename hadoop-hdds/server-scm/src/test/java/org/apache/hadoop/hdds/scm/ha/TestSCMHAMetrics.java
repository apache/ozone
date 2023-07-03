/**
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
package org.apache.hadoop.hdds.scm.ha;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SCMHAMetrics}.
 */
class TestSCMHAMetrics {

  private static final MetricsCollectorImpl METRICS_COLLECTOR =
      new MetricsCollectorImpl();
  private static final String NODE_ID =
      "scm" + RandomStringUtils.randomNumeric(5);
  private String leaderId;
  private SCMHAMetrics scmhaMetrics;

  @AfterEach
  public void cleanup() {
    SCMHAMetrics.unRegister();
  }

  @Test
  public void testGetMetricsWithLeader() {
    // GIVEN
    leaderId = NODE_ID;

    // WHEN
    scmhaMetrics = SCMHAMetrics.create(NODE_ID, leaderId);
    scmhaMetrics.getMetrics(METRICS_COLLECTOR, true);

    // THEN
    Assertions.assertEquals(1, scmhaMetrics.getSCMHAMetricsInfoLeaderState());
  }

  @Test
  public void testGetMetricsWithFollower() {
    // GIVEN
    leaderId = "scm" + RandomStringUtils.randomNumeric(5);

    // WHEN
    scmhaMetrics = SCMHAMetrics.create(NODE_ID, leaderId);
    scmhaMetrics.getMetrics(METRICS_COLLECTOR, true);

    // THEN
    Assertions.assertEquals(0, scmhaMetrics.getSCMHAMetricsInfoLeaderState());
  }

}
