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

package org.apache.hadoop.ozone.metrics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.ozone.metrics.AbstractMetric;
import org.apache.hadoop.ozone.metrics.MetricsRecord;
import org.apache.hadoop.ozone.metrics.MetricsTag;
import org.apache.hadoop.ozone.metrics.annotation.Metric;
import org.apache.hadoop.ozone.metrics.annotation.Metrics;
import org.apache.hadoop.ozone.metrics.lib.MetricsRegistry;
import org.apache.hadoop.ozone.metrics.lib.MutableCounterLong;
import org.apache.hadoop.ozone.metrics.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.metrics.lib.MutableRate;
import org.apache.hadoop.ozone.metrics.sink.ganglia.AbstractGangliaSink;
import org.apache.hadoop.ozone.metrics.sink.ganglia.GangliaMetricsTestHelper;
import org.apache.hadoop.ozone.metrics.sink.ganglia.GangliaSink30;
import org.apache.hadoop.ozone.metrics.sink.ganglia.GangliaSink31;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGangliaMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestMetricsSystemImpl.class);
  // This is the prefix to locate the config file for this particular test
  // This is to avoid using the same config file with other test cases,
  // which can cause race conditions.
  private String testNamePrefix = "gangliametrics";
  private final String[] expectedMetrics = {
      testNamePrefix + ".s1rec.C1",
      testNamePrefix + ".s1rec.G1",
      testNamePrefix + ".s1rec.Xxx",
      testNamePrefix + ".s1rec.Yyy",
      testNamePrefix + ".s1rec.S1NumOps",
      testNamePrefix + ".s1rec.S1AvgTime"
  };

  @Test
  public void testTagsForPrefix() throws Exception {
    ConfigBuilder cb = new ConfigBuilder()
        .add(testNamePrefix + ".sink.ganglia.tagsForPrefix.all", "*")
        .add(testNamePrefix + ".sink.ganglia.tagsForPrefix.some",
          "NumActiveSinks, " + "NumActiveSources")
        .add(testNamePrefix + ".sink.ganglia.tagsForPrefix.none", "");
    GangliaSink30 sink = new GangliaSink30();
    sink.init(cb.subset(testNamePrefix + ".sink.ganglia"));

    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "all"));
    tags.add(new MetricsTag(MsInfo.NumActiveSources, "foo"));
    tags.add(new MetricsTag(MsInfo.NumActiveSinks, "bar"));
    tags.add(new MetricsTag(MsInfo.NumAllSinks, "haa"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host"));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 1, tags, metrics);

    StringBuilder sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals(".NumActiveSources=foo.NumActiveSinks=bar.NumAllSinks=haa", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "some"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals(".NumActiveSources=foo.NumActiveSinks=bar", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "none"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals("", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "nada"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals("", sb.toString());
  }
  
  @Test public void testGangliaMetrics2() throws Exception {
    // Setting long interval to avoid periodic publishing.
    // We manually publish metrics by MeticsSystem#publishMetricsNow here.
    ConfigBuilder cb = new ConfigBuilder().add("*.period", 120)
        .add(testNamePrefix
            + ".sink.gsink30.context", testNamePrefix) // filter out only "test"
        .add(testNamePrefix
            + ".sink.gsink31.context", testNamePrefix) // filter out only "test"
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-"
            + testNamePrefix));

    MetricsSystemImpl ms = new MetricsSystemImpl(testNamePrefix);
    ms.start();
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);

    final int expectedCountFromGanglia30 = expectedMetrics.length;
    final int expectedCountFromGanglia31 = 2 * expectedMetrics.length;

    // Setup test for GangliaSink30
    AbstractGangliaSink gsink30 = new GangliaSink30();
    gsink30.init(cb.subset(testNamePrefix));
    MockDatagramSocket mockds30 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink30, mockds30);

    // Setup test for GangliaSink31
    AbstractGangliaSink gsink31 = new GangliaSink31();
    gsink31.init(cb.subset(testNamePrefix));
    MockDatagramSocket mockds31 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink31, mockds31);

    // register the sinks
    ms.register("gsink30", "gsink30 desc", gsink30);
    ms.register("gsink31", "gsink31 desc", gsink31);
    ms.publishMetricsNow(); // publish the metrics

    ms.stop();

    // check GanfliaSink30 data
    checkMetrics(mockds30.getCapturedSend(), expectedCountFromGanglia30);

    // check GanfliaSink31 data
    checkMetrics(mockds31.getCapturedSend(), expectedCountFromGanglia31);
  }


  // check the expected against the actual metrics
  private void checkMetrics(List<byte[]> bytearrlist, int expectedCount) {
    boolean[] foundMetrics = new boolean[expectedMetrics.length];
    for (byte[] bytes : bytearrlist) {
      String binaryStr = new String(bytes, StandardCharsets.UTF_8);
      for (int index = 0; index < expectedMetrics.length; index++) {
        if (binaryStr.indexOf(expectedMetrics[index]) >= 0) {
          foundMetrics[index] = true;
          break;
        }
      }
    }

    for (int index = 0; index < foundMetrics.length; index++) {
      if (!foundMetrics[index]) {
        assertTrue(false, "Missing metrics: " + expectedMetrics[index]);
      }
    }

    assertEquals(expectedCount, bytearrlist.size(), "Mismatch in record count: ");
  }

  @SuppressWarnings("unused")
  @Metrics(context="gangliametrics")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  /**
   * This class is used to capture data send to Ganglia servers.
   *
   * Initial attempt was to use mockito to mock and capture but
   * while testing figured out that mockito is keeping the reference
   * to the byte array and since the sink code reuses the byte array
   * hence all the captured byte arrays were pointing to one instance.
   */
  private class MockDatagramSocket extends DatagramSocket {
    private List<byte[]> capture;

    /**
     * @throws SocketException
     */
    public MockDatagramSocket() throws SocketException {
      capture = new ArrayList<byte[]>();
    }

    /* (non-Javadoc)
     * @see java.net.DatagramSocket#send(java.net.DatagramPacket)
     */
    @Override
    public synchronized void send(DatagramPacket p) throws IOException {
      // capture the byte arrays
      byte[] bytes = new byte[p.getLength()];
      System.arraycopy(p.getData(), p.getOffset(), bytes, 0, p.getLength());
      capture.add(bytes);
    }

    /**
     * @return the captured byte arrays
     */
    synchronized List<byte[]> getCapturedSend() {
      return capture;
    }
  }
}
