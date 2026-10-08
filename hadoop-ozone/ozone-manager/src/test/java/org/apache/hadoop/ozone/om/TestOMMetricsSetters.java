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

package org.apache.hadoop.ozone.om;

import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMMetrics}.
 */
public class TestOMMetricsSetters {

  private OMMetrics metrics;

  @AfterEach
  public void cleanUp() {
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  @Test
  public void testSetTotals() {
    metrics = OMMetrics.create(new OzoneConfiguration());
    metrics.setNumVolumes(1);
    metrics.setNumBuckets(2);
    metrics.setNumKeys(3);
    metrics.setNumDirs(4);
    metrics.setNumFiles(5);

    MetricsRecordBuilder rb = getMetrics("OMMetrics");
    assertGauge("NumVolumes", 1L, rb);
    assertGauge("NumBuckets", 2L, rb);
    assertGauge("NumKeys", 3L, rb);
    assertGauge("NumDirs", 4L, rb);
    assertGauge("NumFiles", 5L, rb);
  }
}
