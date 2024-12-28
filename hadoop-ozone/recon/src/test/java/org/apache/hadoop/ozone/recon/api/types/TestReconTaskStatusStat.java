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

package org.apache.hadoop.ozone.recon.api.types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This files contains tests to validate the {@link ReconTaskStatusStat} class.
 */
public class TestReconTaskStatusStat {
  @Test
  void testIncrementSuccess() {
    ReconTaskStatusStat stat = new ReconTaskStatusStat();
    assertEquals(0, stat.getSuccessCount());
    stat.incrementSuccess();
    assertEquals(1, stat.getSuccessCount());
  }

  @Test
  void testIncrementFailure() {
    ReconTaskStatusStat stat = new ReconTaskStatusStat();
    assertEquals(0, stat.getFailureCount());
    stat.incrementFailure();
    assertEquals(1, stat.getFailureCount());
  }

  @Test
  void testReset() {
    ReconTaskStatusStat stat = new ReconTaskStatusStat();
    stat.incrementSuccess();
    stat.incrementFailure();
    assertTrue(stat.getSuccessCount() > 0);
    assertTrue(stat.getFailureCount() > 0);

    stat.reset();
    assertEquals(0, stat.getSuccessCount());
    assertEquals(0, stat.getFailureCount());
    assertTrue(stat.getInitializationTime() > 0);
  }
}
