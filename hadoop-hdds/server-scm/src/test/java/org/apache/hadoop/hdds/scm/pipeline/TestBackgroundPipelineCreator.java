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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.List;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;

/**
 * Tests for BackgroundPipelineCreator replication config selection.
 */
public class TestBackgroundPipelineCreator {

  @Test
  public void testEcDefaultReplicationAddsEcPipelineConfig() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.EC.name());
    conf.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024k");

    BackgroundPipelineCreator creator = new BackgroundPipelineCreator(
        mock(PipelineManager.class), conf, mock(SCMContext.class),
        Clock.systemUTC());

    List<ReplicationConfig> configs = creator.getReplicationConfigs(false);

    assertTrue(configs.stream().anyMatch(c ->
        c.getReplicationType() == HddsProtos.ReplicationType.EC));
    assertTrue(configs.contains(new ECReplicationConfig("rs-3-2-1024k")));
  }

  @Test
  public void testRatisDefaultReplicationBehaviorUnchanged() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.RATIS.name());

    BackgroundPipelineCreator creator = new BackgroundPipelineCreator(
        mock(PipelineManager.class), conf, mock(SCMContext.class),
        Clock.systemUTC());

    List<ReplicationConfig> configs = creator.getReplicationConfigs(false);

    assertEquals(1, configs.size());
    assertTrue(RatisReplicationConfig.hasFactor(configs.get(0),
        HddsProtos.ReplicationFactor.THREE));
  }

}
