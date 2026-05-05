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

package org.apache.hadoop.hdds.server.http;

import static org.apache.ratis.server.metrics.SegmentedRaftLogMetrics.RAFT_LOG_SYNC_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.metrics.dropwizard3.RatisMetricsUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.junit.jupiter.api.Test;

/**
 * Test {@link RatisDropwizardExports}.
 */
public class TestRatisDropwizardExports {
  static Timer findTimer(String name, MetricRegistry registry) {
    for (Map.Entry<String, Timer> e : registry.getTimers().entrySet()) {
      if (e.getKey().contains(name)) {
        return e.getValue();
      }
    }
    throw new IllegalStateException(name + " not found");
  }

  @Test
  public void export() throws IOException {
    //create Ratis metrics
    SegmentedRaftLogMetrics instance = new SegmentedRaftLogMetrics(
        RaftGroupMemberId.valueOf(
            RaftPeerId.valueOf("peerId"), RaftGroupId.randomId()));
    MetricRegistry dropWizardMetricRegistry =
        RatisMetricsUtils.getDropWizardMetricRegistry(instance.getRegistry());
    findTimer(RAFT_LOG_SYNC_TIME, dropWizardMetricRegistry)
        .update(10, TimeUnit.MILLISECONDS);

    //create and register prometheus collector
    RatisDropwizardExports exports =
        new RatisDropwizardExports(dropWizardMetricRegistry);

    CollectorRegistry collector = new CollectorRegistry();
    collector.register(exports);

    //export metrics to the string
    StringWriter writer = new StringWriter();
    TextFormat.write004(writer, collector.metricFamilySamples());

    System.out.println(writer);

    assertThat(writer.toString())
        .withFailMessage("Instance name is not moved to be a tag")
        .doesNotContain("ratis_core_ratis_log_worker_instance_syncTime");
  }

}
