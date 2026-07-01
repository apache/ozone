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

package org.apache.hadoop.hdds.scm;

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.Test;

/**
 * Unit test for XceiverClientMetrics.
 */
public class TestXceiverClientMetricsUnit {

  @Test
  public void testGrpcFailures() {
    XceiverClientMetrics metrics = XceiverClientMetrics.create();
    try {
      metrics.incGrpcAuthenticationFailures();
      metrics.incGrpcConnectionFailures();
      metrics.incGrpcConnectionFailures();

      MetricsRecordBuilder recordBuilder = getMetrics(XceiverClientMetrics.SOURCE_NAME);
      assertCounter("GrpcAuthenticationFailures", 1L, recordBuilder);
      assertCounter("GrpcConnectionFailures", 2L, recordBuilder);
    } finally {
      metrics.unRegister();
    }
  }
}
