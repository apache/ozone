/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.metadata;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;


/**
 * Testing of SCMMetadataStoreImpl.
 */
public class TestSCMMetadataStoreImpl {
  private OzoneConfiguration conf;
  private SCMMetadataStore scmMetadataStore;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    conf = SCMTestUtils.getConf();

    scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);
  }

  @Test
  public void testEstimatedKeyCount() {
    Assertions.assertEquals(0, getGauge("SequenceIdEstimatedKeyCount"));

    try {
      scmMetadataStore.getSequenceIdTable().put("TestKey", 1L);
    } catch (IOException e) {
      // Ignore
    }

    Assertions.assertEquals(1, getGauge("SequenceIdEstimatedKeyCount"));
  }

  private long getGauge(String metricName) {
    return getLongGauge(metricName,
        getMetrics(SCMMetadataStoreMetrics.METRICS_SOURCE_NAME));
  }
}
