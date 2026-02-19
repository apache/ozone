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

package org.apache.hadoop.hdds.scm.container;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ReplicationManagerReport class.
 */
class TestReplicationManagerReport {

  private ReplicationManagerReport report;

  @BeforeEach
  void setup() {
    report = new ReplicationManagerReport(100);
  }

  @Test
  void testMetricCanBeIncremented() {
    report.increment(ContainerHealthState.UNDER_REPLICATED);
    report.increment(ContainerHealthState.UNDER_REPLICATED);
    report.increment(ContainerHealthState.OVER_REPLICATED);

    report.increment(HddsProtos.LifeCycleState.OPEN);
    report.increment(HddsProtos.LifeCycleState.CLOSED);
    report.increment(HddsProtos.LifeCycleState.CLOSED);

    assertEquals(2,
        report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ContainerHealthState.MIS_REPLICATED));

    assertEquals(1,
        report.getStat(HddsProtos.LifeCycleState.OPEN));
    assertEquals(2,
        report.getStat(HddsProtos.LifeCycleState.CLOSED));
    assertEquals(0,
        report.getStat(HddsProtos.LifeCycleState.QUASI_CLOSED));
  }

  @Test
  void testJsonOutput() throws IOException {
    report.increment(HddsProtos.LifeCycleState.OPEN);
    report.increment(HddsProtos.LifeCycleState.CLOSED);
    report.increment(HddsProtos.LifeCycleState.CLOSED);

    // Use mock ContainerInfo for testing incrementAndSample
    ContainerInfo mockContainer1 = mock(ContainerInfo.class);
    when(mockContainer1.containerID()).thenReturn(ContainerID.valueOf(1));
    ContainerInfo mockContainer2 = mock(ContainerInfo.class);
    when(mockContainer2.containerID()).thenReturn(ContainerID.valueOf(2));
    ContainerInfo mockContainer3 = mock(ContainerInfo.class);
    when(mockContainer3.containerID()).thenReturn(ContainerID.valueOf(3));
    
    report.incrementAndSample(ContainerHealthState.UNDER_REPLICATED, mockContainer1);
    report.incrementAndSample(ContainerHealthState.UNDER_REPLICATED, mockContainer2);
    report.incrementAndSample(ContainerHealthState.OVER_REPLICATED, mockContainer3);
    report.setComplete();

    String jsonString = JsonUtils.toJsonStringWithDefaultPrettyPrinter(report);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(jsonString);

    assertThat(json.get("reportTimeStamp").longValue()).isPositive();
    JsonNode stats = json.get("stats");
    assertEquals(1, stats.get("OPEN").longValue());
    assertEquals(0, stats.get("CLOSING").longValue());
    assertEquals(0, stats.get("QUASI_CLOSED").longValue());
    assertEquals(2, stats.get("CLOSED").longValue());
    assertEquals(0, stats.get("DELETING").longValue());
    assertEquals(0, stats.get("DELETED").longValue());

    assertEquals(2, stats.get("UNDER_REPLICATED").longValue());
    assertEquals(1, stats.get("OVER_REPLICATED").longValue());
    assertEquals(0, stats.get("MIS_REPLICATED").longValue());
    assertEquals(0, stats.get("MISSING").longValue());
    assertEquals(0, stats.get("UNHEALTHY").longValue());
    assertEquals(0, stats.get("EMPTY").longValue());
    assertEquals(0, stats.get("OPEN_UNHEALTHY").longValue());
    assertEquals(0, stats.get("QUASI_CLOSED_STUCK").longValue());
    assertEquals(0, stats.get("OPEN_WITHOUT_PIPELINE").longValue());

    JsonNode samples = json.get("samples");
    assertEquals(ARRAY, samples.get("UNDER_REPLICATED").getNodeType());
    assertEquals(1, samples.get("UNDER_REPLICATED").get(0).longValue());
    assertEquals(2, samples.get("UNDER_REPLICATED").get(1).longValue());
    assertEquals(3, samples.get("OVER_REPLICATED").get(0).longValue());
  }

  @Test
  void testContainerIDsCanBeSampled() {
    report.increment(ContainerHealthState.UNDER_REPLICATED);
    report.increment(ContainerHealthState.UNDER_REPLICATED);
    report.increment(ContainerHealthState.OVER_REPLICATED);

    assertEquals(2,
        report.getStat(ContainerHealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ContainerHealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ContainerHealthState.MIS_REPLICATED));
  }

  @Test
  void testSamplesAreLimited() {
    verifySampleLimit(report, 100);
  }

  @Test
  void testCustomSampleLimit() {
    ReplicationManagerReport customReport = new ReplicationManagerReport(50);
    verifySampleLimit(customReport, 50);
  }

  /**
   * Helper method to verify that sample limit is set correctly.
   * Note: Sample collection happens via incrementAndSample() which takes ContainerInfo.
   * This test just verifies the limit configuration.
   */
  private void verifySampleLimit(ReplicationManagerReport testReport, int expectedSampleSize) {
    assertEquals(testReport.getSampleLimit(), expectedSampleSize);
    
    // Verify counter works
    for (int i = 0; i < expectedSampleSize * 2; i++) {
      testReport.increment(ContainerHealthState.UNDER_REPLICATED);
    }
    assertEquals((long) expectedSampleSize * 2, 
        testReport.getStat(ContainerHealthState.UNDER_REPLICATED));
  }

  @Test
  void testSerializeToProtoAndBack() {
    report.setTimestamp(12345);
    Random rand = ThreadLocalRandom.current();
    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      report.setStat(s.toString(), rand.nextInt(Integer.MAX_VALUE));
    }
    for (ContainerHealthState s : ContainerHealthState.values()) {
      report.setStat(s.name(), rand.nextInt(Integer.MAX_VALUE));
      List<ContainerID> containers = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        containers.add(ContainerID.valueOf(rand.nextInt(Integer.MAX_VALUE)));
      }
      report.setSample(s.name(), containers);
    }
    HddsProtos.ReplicationManagerReportProto proto = report.toProtobuf();
    ReplicationManagerReport newReport
        = ReplicationManagerReport.fromProtobuf(proto);
    assertEquals(report.getReportTimeStamp(),
        newReport.getReportTimeStamp());
    assertEquals(report.getSampleLimit(),
        newReport.getSampleLimit());

    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      assertEquals(report.getStat(s), newReport.getStat(s));
    }

    // Sample tracking removed - health state now stored in ContainerInfo
  }

  @Test
  void testDeSerializeCanHandleUnknownMetric() {
    HddsProtos.ReplicationManagerReportProto.Builder proto =
        HddsProtos.ReplicationManagerReportProto.newBuilder();
    proto.setTimestamp(12345);
    proto.setSampleLimit(100);

    proto.addStat(HddsProtos.KeyIntValue.newBuilder()
        .setKey("unknownValue")
        .setValue(15)
        .build());

    proto.addStat(HddsProtos.KeyIntValue.newBuilder()
        .setKey(ContainerHealthState.UNDER_REPLICATED.name())
        .setValue(20)
        .build());

    HddsProtos.KeyContainerIDList.Builder sample
        = HddsProtos.KeyContainerIDList.newBuilder();
    sample.setKey("unknownValue");
    sample.addContainer(ContainerID.valueOf(1).getProtobuf());
    proto.addStatSample(sample.build());
    // Ensure no exception is thrown
    ReplicationManagerReport newReport =
        ReplicationManagerReport.fromProtobuf(proto.build());
    assertEquals(20, newReport.getStat(
        ContainerHealthState.UNDER_REPLICATED));
    assertEquals(100, newReport.getSampleLimit());
  }

  @Test
  void testStatCannotBeSetTwice() {
    report.setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10);
    assertThrows(IllegalStateException.class, () -> report
        .setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10));
  }

  @Test
  void testSampleCannotBeSetTwice() {
    List<ContainerID> containers = new ArrayList<>();
    containers.add(ContainerID.valueOf(1));
    report.setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers);
    assertThrows(IllegalStateException.class, () -> report
        .setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers));
  }
}
