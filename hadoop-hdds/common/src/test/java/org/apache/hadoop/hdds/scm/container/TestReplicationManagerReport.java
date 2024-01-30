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
package org.apache.hadoop.hdds.scm.container;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the ReplicationManagerReport class.
 */
public class TestReplicationManagerReport {

  private ReplicationManagerReport report;

  @BeforeEach
  public void setup() {
    report = new ReplicationManagerReport();
  }

  @Test
  public void testMetricCanBeIncremented() {
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
  public void testJsonOutput() throws IOException {
    report.increment(HddsProtos.LifeCycleState.OPEN);
    report.increment(HddsProtos.LifeCycleState.CLOSED);
    report.increment(HddsProtos.LifeCycleState.CLOSED);

    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        new ContainerID(1));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        new ContainerID(2));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.OVER_REPLICATED,
        new ContainerID(3));
    report.setComplete();

    String jsonString = JsonUtils.toJsonStringWithDefaultPrettyPrinter(report);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(jsonString);

    assertTrue(json.get("reportTimeStamp").longValue() > 0);
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
  public void testContainerIDsCanBeSampled() {
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        new ContainerID(1));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED,
        new ContainerID(2));
    report.incrementAndSample(
        ReplicationManagerReport.HealthState.OVER_REPLICATED,
        new ContainerID(3));

    assertEquals(2,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
    assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));

    List<ContainerID> sample =
        report.getSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    assertEquals(new ContainerID(1), sample.get(0));
    assertEquals(new ContainerID(2), sample.get(1));
    assertEquals(2, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.OVER_REPLICATED);
    assertEquals(new ContainerID(3), sample.get(0));
    assertEquals(1, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.MIS_REPLICATED);
    assertEquals(0, sample.size());
  }

  @Test
  public void testSamplesAreLimited() {
    for (int i = 0; i < ReplicationManagerReport.SAMPLE_LIMIT * 2; i++) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          new ContainerID(i));
    }
    List<ContainerID> sample =
        report.getSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    assertEquals(ReplicationManagerReport.SAMPLE_LIMIT, sample.size());
    for (int i = 0; i < ReplicationManagerReport.SAMPLE_LIMIT; i++) {
      assertEquals(new ContainerID(i), sample.get(i));
    }
  }

  @Test
  public void testSerializeToProtoAndBack() {
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

    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      assertEquals(report.getStat(s), newReport.getStat(s));
    }

    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      assertEquals(report.getSample(s), newReport.getSample(s));
    }
  }

  @Test
  public void testDeSerializeCanHandleUnknownMetric() {
    HddsProtos.ReplicationManagerReportProto.Builder proto =
        HddsProtos.ReplicationManagerReportProto.newBuilder();
    proto.setTimestamp(12345);

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
  }

  @Test
  public void testStatCannotBeSetTwice() {
    report.setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10);
    assertThrows(IllegalStateException.class, () -> report
        .setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10));
  }

  @Test
  public void testSampleCannotBeSetTwice() {
    List<ContainerID> containers = new ArrayList<>();
    containers.add(ContainerID.valueOf(1));
    report.setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers);
    assertThrows(IllegalStateException.class, () -> report
        .setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers));
  }
}
