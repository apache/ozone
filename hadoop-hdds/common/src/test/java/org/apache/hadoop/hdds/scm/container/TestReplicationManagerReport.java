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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for the ReplicationManagerReport class.
 */
public class TestReplicationManagerReport {

  private ReplicationManagerReport report;

  @Before
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

    Assert.assertEquals(2,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));

    Assert.assertEquals(1,
        report.getStat(HddsProtos.LifeCycleState.OPEN));
    Assert.assertEquals(2,
        report.getStat(HddsProtos.LifeCycleState.CLOSED));
    Assert.assertEquals(0,
        report.getStat(HddsProtos.LifeCycleState.QUASI_CLOSED));
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

    Assert.assertEquals(2,
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1,
        report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED));
    Assert.assertEquals(0,
        report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED));

    List<ContainerID> sample =
        report.getSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    Assert.assertEquals(new ContainerID(1), sample.get(0));
    Assert.assertEquals(new ContainerID(2), sample.get(1));
    Assert.assertEquals(2, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.OVER_REPLICATED);
    Assert.assertEquals(new ContainerID(3), sample.get(0));
    Assert.assertEquals(1, sample.size());

    sample =
        report.getSample(ReplicationManagerReport.HealthState.MIS_REPLICATED);
    Assert.assertEquals(0, sample.size());
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
    Assert.assertEquals(ReplicationManagerReport.SAMPLE_LIMIT, sample.size());
    for (int i = 0; i < ReplicationManagerReport.SAMPLE_LIMIT; i++) {
      Assert.assertEquals(new ContainerID(i), sample.get(i));
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
    Assert.assertEquals(report.getReportTimeStamp(),
        newReport.getReportTimeStamp());

    for (HddsProtos.LifeCycleState s : HddsProtos.LifeCycleState.values()) {
      Assert.assertEquals(report.getStat(s), newReport.getStat(s));
    }

    for (ReplicationManagerReport.HealthState s :
        ReplicationManagerReport.HealthState.values()) {
      Assert.assertTrue(report.getSample(s).equals(newReport.getSample(s)));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStatCannotBeSetTwice() {
    report.setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10);
    report.setStat(HddsProtos.LifeCycleState.CLOSED.toString(), 10);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleCannotBeSetTwice() {
    List<ContainerID> containers = new ArrayList<>();
    containers.add(ContainerID.valueOf(1));
    report.setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers);
    report.setSample(HddsProtos.LifeCycleState.CLOSED.toString(), containers);
  }
}
