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

package org.apache.hadoop.hdds.freon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Port;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to store pre-generated topology information for load-tests.
 */
@SuppressWarnings("java:S2245") // no need for secure random
public final class FakeClusterTopology {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FakeClusterTopology.class);

  public static final FakeClusterTopology INSTANCE = newFakeClusterTopology();

  private final List<DatanodeDetailsProto> datanodes;

  private final List<Pipeline> pipelines;

  private final Random random = new Random();

  private static FakeClusterTopology newFakeClusterTopology() {
    final int nodeCount = 9;
    final List<DatanodeDetailsProto> datanodes = new ArrayList<>(nodeCount);
    final List<Pipeline> pipelines = new ArrayList<>(nodeCount / 3);
    try {
      for (int i = 0; i < nodeCount; i++) {
        datanodes.add(createDatanode());
        if ((i + 1) % 3 == 0) {
          pipelines.add(Pipeline.newBuilder()
              .setId(PipelineID.randomId().getProtobuf())
              .setFactor(ReplicationFactor.THREE)
              .setType(ReplicationType.RATIS)
              .addMembers(datanodes.get(i - 2))
              .addMembers(datanodes.get(i - 1))
              .addMembers(datanodes.get(i))
              .build());
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Can't initialize FakeClusterTopology", ex);
    }
    return new FakeClusterTopology(datanodes, pipelines);
  }

  private FakeClusterTopology(List<DatanodeDetailsProto> datanodes, List<Pipeline> pipelines) {
    this.datanodes = Collections.unmodifiableList(datanodes);
    this.pipelines = Collections.unmodifiableList(pipelines);
  }

  private static DatanodeDetailsProto createDatanode() {
    return DatanodeDetailsProto.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPorts(
            Port.newBuilder().setName("RATIS").setValue(1234))
        .build();
  }

  public Pipeline getRandomPipeline() {
    return pipelines.get(random.nextInt(pipelines.size()));
  }

  public Iterable<DatanodeDetailsProto> getAllDatanodes() {
    return datanodes;
  }
}
