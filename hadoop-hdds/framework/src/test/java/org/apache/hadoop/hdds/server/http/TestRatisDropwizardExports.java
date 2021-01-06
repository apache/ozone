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
package org.apache.hadoop.hdds.server.http;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test RatisDropwizardRexporter.
 */
public class TestRatisDropwizardExports {

  @Test
  public void export() throws IOException {
    //create Ratis metrics
    SegmentedRaftLogMetrics instance = new SegmentedRaftLogMetrics(
        RaftGroupMemberId.valueOf(
            RaftPeerId.valueOf("peerId"), RaftGroupId.randomId()));
    instance.getRaftLogSyncTimer().update(10, TimeUnit.MILLISECONDS);
    MetricRegistry dropWizardMetricRegistry =
        instance.getRegistry().getDropWizardMetricRegistry();

    //create and register prometheus collector
    RatisDropwizardExports exports =
        new RatisDropwizardExports(dropWizardMetricRegistry);

    CollectorRegistry collector = new CollectorRegistry();
    collector.register(new RatisDropwizardExports(dropWizardMetricRegistry));

    //export metrics to the string
    StringWriter writer = new StringWriter();
    TextFormat.write004(writer, collector.metricFamilySamples());

    System.out.println(writer.toString());

    Assert.assertFalse("Instance name is not moved to be a tag",
        writer.toString()
            .contains("ratis_core_ratis_log_worker_instance_syncTime"));

  }

}