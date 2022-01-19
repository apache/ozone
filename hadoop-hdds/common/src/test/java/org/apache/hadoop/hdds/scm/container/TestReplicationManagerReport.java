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

import java.util.List;

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
}
