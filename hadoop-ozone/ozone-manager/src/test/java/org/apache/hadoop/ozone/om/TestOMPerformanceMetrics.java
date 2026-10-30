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

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMPerformanceMetrics}.
 */
public class TestOMPerformanceMetrics {

  /**
   * Base names of every {@link org.apache.hadoop.ozone.util.ConcurrentMutableRate}
   * latency counter in {@link OMPerformanceMetrics}. Each emits a
   * {@code <Name>NumOps} counter and a {@code <Name>AvgTime} gauge. This list
   * must mirror the {@code stat(...)} names in the metrics source one-for-one:
   * the parity test below fails if a hand-typed literal is renamed or dropped
   * (e.g. by a typo), which would otherwise silently rename a published metric.
   */
  private static final String[] LATENCY_STAT_NAMES = {
      "LookupLatencyNs",
      "LookupReadKeyInfoLatencyNs",
      "LookupGenerateBlockTokenLatencyNs",
      "LookupRefreshLocationLatencyNs",
      "LookupAclCheckLatencyNs",
      "LookupResolveBucketLatencyNs",
      "GetKeyInfoLatencyNs",
      "GetKeyInfoReadKeyInfoLatencyNs",
      "GetKeyInfoGenerateBlockTokenLatencyNs",
      "GetKeyInfoRefreshLocationLatencyNs",
      "GetKeyInfoAclCheckLatencyNs",
      "GetKeyInfoSortDatanodesLatencyNs",
      "AllocateBlockSortDatanodesLatencyNs",
      "GetKeyInfoResolveBucketLatencyNs",
      "S3VolumeContextLatencyNs",
      "ForceContainerCacheRefresh",
      "CheckAccessLatencyNs",
      "ListKeysLatencyNs",
      "ValidateRequestLatencyNs",
      "ValidateResponseLatencyNs",
      "PreExecuteLatencyNs",
      "SubmitToRatisLatencyNs",
      "CreateRatisRequestLatencyNs",
      "CreateOmResponseLatencyNs",
      "ValidateAndUpdateCacheLatencyNs",
      "ListKeysAveragePagination",
      "ListKeysAclCheckLatencyNs",
      "ListKeysResolveBucketLatencyNs",
      "DeleteKeyFailureLatencyNs",
      "DeleteKeySuccessLatencyNs",
      "DeleteKeysResolveBucketLatencyNs",
      "DeleteKeysAclCheckLatencyNs",
      "DeleteKeyResolveBucketAndAclCheckLatencyNs",
      "ListKeysReadFromRocksDbLatencyNs",
      "GetObjectTaggingResolveBucketLatencyNs",
      "GetObjectTaggingAclCheckLatencyNs",
      "GetBucketTaggingResolveBucketLatencyNs",
      "GetBucketTaggingAclCheckLatencyNs",
      "GetBucketTaggingLatencyNs",
      "CreateKeyResolveBucketAndAclCheckLatencyNs",
      "CreateKeyQuotaCheckLatencyNs",
      "CreateKeyAllocateBlockLatencyNs",
      "CreateKeyFailureLatencyNs",
      "CreateKeySuccessLatencyNs",
  };

  @AfterEach
  public void cleanUp() {
    OMPerformanceMetrics.unregister();
  }

  /**
   * Registers the source and asserts that every latency counter publishes the
   * expected {@code NumOps}/{@code AvgTime} metric names, guarding the 44
   * hand-typed name literals against a typo silently renaming a metric.
   */
  @Test
  public void testLatencyMetricNames() {
    OMPerformanceMetrics metrics = OMPerformanceMetrics.register();
    MetricsRecordBuilder rb = getMetrics(metrics);
    for (String name : LATENCY_STAT_NAMES) {
      assertCounter(name + "NumOps", 0L, rb);
      assertGauge(name + "AvgTime", 0.0, rb);
    }
  }
}
