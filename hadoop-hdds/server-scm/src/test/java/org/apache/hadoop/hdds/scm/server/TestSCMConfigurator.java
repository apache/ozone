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

package org.apache.hadoop.hdds.scm.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.File;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.MockClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that SCM uses the components passed through {@link SCMConfigurator}.
 */
class TestSCMConfigurator {

  @Test
  void scmUsesConfiguredEventQueueClockAndPipelineIds(@TempDir File dir) throws Exception {
    EventQueue eventQueue = new EventQueue();
    MockClock clock = MockClock.newInstance();
    PipelineID pipelineId = PipelineID.randomId();
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.updateLeaderAndTerm(true, 1);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(scmContext);
    configurator.setScmNodeManager(new MockNodeManager(true, 3));
    configurator.setEventQueue(eventQueue);
    configurator.setSystemClock(clock);
    configurator.setPipelineIdGenerator(() -> pipelineId);

    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(dir), configurator);
    try {
      assertSame(eventQueue, scm.getEventQueue());
      assertSame(clock, scm.getSystemClock());
      Pipeline pipeline = scm.getPipelineManager().createPipeline(
          RatisReplicationConfig.getInstance(ReplicationFactor.ONE));
      assertEquals(pipelineId, pipeline.getId());
    } finally {
      scm.stop();
    }
  }
}
