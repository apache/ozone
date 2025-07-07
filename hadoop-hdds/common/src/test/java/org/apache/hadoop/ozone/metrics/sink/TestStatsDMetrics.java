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

package org.apache.hadoop.ozone.metrics.sink;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.ozone.metrics.AbstractMetric;
import org.apache.hadoop.ozone.metrics.MetricType;
import org.apache.hadoop.ozone.metrics.MetricsRecord;
import org.apache.hadoop.ozone.metrics.MetricsTag;
import org.apache.hadoop.ozone.metrics.impl.MetricsRecordImpl;
import org.apache.hadoop.ozone.metrics.impl.MsInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestStatsDMetrics {

  private AbstractMetric makeMetric(String name, Number value,
      MetricType type) {
    AbstractMetric metric = mock(AbstractMetric.class);
    when(metric.name()).thenReturn(name);
    when(metric.value()).thenReturn(value);
    when(metric.type()).thenReturn(type);
    return metric;
  }

  @Test
  @Timeout(value = 3)
  public void testPutMetrics() throws IOException, IllegalAccessException {
    final StatsDSink sink = new StatsDSink();
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Hostname, "host"));
    tags.add(new MetricsTag(MsInfo.Context, "jvm"));
    tags.add(new MetricsTag(MsInfo.ProcessName, "process"));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    metrics.add(makeMetric("foo1", 1.25, MetricType.COUNTER));
    metrics.add(makeMetric("foo2", 2.25, MetricType.GAUGE));
    final MetricsRecord record =
        new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

    try (DatagramSocket sock = new DatagramSocket()) {
      sock.setReceiveBufferSize(8192);
      final StatsDSink.StatsD mockStatsD =
          new StatsDSink.StatsD(sock.getLocalAddress().getHostName(),
              sock.getLocalPort());
      sink.setStatsd(mockStatsD);
      final DatagramPacket p = new DatagramPacket(new byte[8192], 8192);
      sink.putMetrics(record);
      sock.receive(p);

      String result =new String(p.getData(), 0, p.getLength(),
          StandardCharsets.UTF_8);
      assertTrue(result.equals("host.process.jvm.Context.foo1:1.25|c") ||
          result.equals("host.process.jvm.Context.foo2:2.25|g"),
          "Received data did not match data sent");

    } finally {
      sink.close();
    }
  }

  @Test
  @Timeout(value = 3)
  public void testPutMetrics2() throws IOException, IllegalAccessException {
    StatsDSink sink = new StatsDSink();
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Hostname, null));
    tags.add(new MetricsTag(MsInfo.Context, "jvm"));
    tags.add(new MetricsTag(MsInfo.ProcessName, "process"));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    metrics.add(makeMetric("foo1", 1, MetricType.COUNTER));
    metrics.add(makeMetric("foo2", 2, MetricType.GAUGE));
    MetricsRecord record =
        new MetricsRecordImpl(MsInfo.Context, (long) 10000, tags, metrics);

    try (DatagramSocket sock = new DatagramSocket()) {
      sock.setReceiveBufferSize(8192);
      final StatsDSink.StatsD mockStatsD =
          new StatsDSink.StatsD(sock.getLocalAddress().getHostName(),
              sock.getLocalPort());
      sink.setStatsd(mockStatsD);
      final DatagramPacket p = new DatagramPacket(new byte[8192], 8192);
      sink.putMetrics(record);
      sock.receive(p);
      String result =
          new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);

      assertTrue(result.equals("process.jvm.Context.foo1:1|c") ||
          result.equals("process.jvm.Context.foo2:2|g"),
          "Received data did not match data sent");
    } finally {
      sink.close();
    }
  }

}
