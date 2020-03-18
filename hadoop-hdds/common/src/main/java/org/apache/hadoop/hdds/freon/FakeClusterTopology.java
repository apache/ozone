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
package org.apache.hadoop.hdds.freon;

import java.util.ArrayList;
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
public class FakeClusterTopology {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FakeClusterTopology.class);

  public static final FakeClusterTopology INSTANCE = new FakeClusterTopology();

  private List<DatanodeDetailsProto> datanodes = new ArrayList<>();

  private List<Pipeline> pipelines = new ArrayList<>();

  private Random random = new Random();

  public FakeClusterTopology() {
    try {
      for (int i = 0; i < 9; i++) {
        datanodes.add(createDatanode(i));
        if ((i + 1) % 3 == 0) {
          pipelines.add(Pipeline.newBuilder()
              .setId(PipelineID.randomId().getProtobuf())
              .setFactor(ReplicationFactor.THREE)
              .setType(ReplicationType.RATIS)
              .addMembers(getDatanode(i - 2))
              .addMembers(getDatanode(i - 1))
              .addMembers(getDatanode(i))
              .build());
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Can't initialize FakeClusterTopology", ex);
    }
  }

  private DatanodeDetailsProto createDatanode(int index) {
    return DatanodeDetailsProto.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPorts(
            Port.newBuilder().setName("RATIS").setValue(1234))
        .build();
  }

  public DatanodeDetailsProto getDatanode(int i) {
    return datanodes.get(i);
  }

  public Pipeline getRandomPipeline() {
    return pipelines.get(random.nextInt(pipelines.size()));
  }

  public List<DatanodeDetailsProto> getAllDatanodes() {
    return datanodes;
  }
}
