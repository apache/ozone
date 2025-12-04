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
    report.increment(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    report.increment(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    report.increment(ReplicationManagerReport.HealthState.OVER_REPLICATED);

    report.increment(HddsProtos.LifeCycleState.OPEN);
    report.increment(HddsProtos.LifeCycleState.CLOSED);
    report.increment(HddsProtos.LifeCycleState.CLOSED);

    assertEquals(2,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));

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

    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ContainerID.valueOf(1));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ContainerID.valueOf(2));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.OVER_REPLICATED,
        ContainerID.valueOf(3));
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
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ContainerID.valueOf(1));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        ContainerID.valueOf(2));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.OVER_REPLICATED,
        ContainerID.valueOf(3));

    assertEquals(2,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));

    List<ContainerID> sample =
        report.getSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    assertEquals(ContainerID.valueOf(1), sample.get(0));
    assertEquals(ContainerID.valueOf(2), sample.get(1));
    assertEquals(2, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.OVER_REPLICATED);
    assertEquals(ContainerID.valueOf(3), sample.get(0));
    assertEquals(1, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.MIS_REPLICATED);
    assertEquals(0, sample.size());
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
   * Helper method to verify that samples are limited to the expected size.
   */
  private void verifySampleLimit(ReplicationManagerReport testReport, int expectedSampleSize) {
    assertEquals(testReport.getSampleLimit(), expectedSampleSize);

    for (int i = 0; i < expectedSampleSize * 2; i++) {
      testReport.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          ContainerID.valueOf(i));
    }
    List<ContainerID> sample =
        testReport.getSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    assertEquals(expectedSampleSize, sample.size());
    for (int i = 0; i < expectedSampleSize; i++) {
      assertEquals(ContainerID.valueOf(i), sample.get(i));
    }
  }

  @Test
  void testSerializeToProtoAndBack() {
    report.setTimestamp(12345);
    Random rand = ThreadLocalRandom.current();
    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      report.setStat(s.toString(), rand.nextInt(Integer.MAX_VALUE));
    }
    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      report.setStat(s.toString(), rand.nextInt(Integer.MAX_VALUE));
      List<ContainerID> containers = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        containers.add(ContainerID.valueOf(rand.nextInt(Integer.MAX_VALUE)));
      }
      report.setSample(s.toString(), containers);
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

    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      assertEquals(report.getSample(s), newReport.getSample(s));
    }
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
        .setKey(ReplicationManagerReport.HealthState.UNDER_REPLICATED
            .toString())
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
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
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
