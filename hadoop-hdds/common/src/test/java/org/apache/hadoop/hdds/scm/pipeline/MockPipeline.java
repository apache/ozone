/*
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
package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Provides {@link Pipeline} factory methods for testing.
 */
public final class MockPipeline {
  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   */
  public static Pipeline createSingleNodePipeline() throws IOException {
    return createPipeline(1);
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   */
  public static Pipeline createPipeline(int numNodes) throws IOException {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeDetails> ids = new ArrayList<>(numNodes);
    for(int i = 0; i < numNodes; i++) {
      ids.add(MockDatanodeDetails.randomLocalDatanodeDetails());
    }
    return createPipeline(ids);
  }

  public static Pipeline createPipeline(Iterable<DatanodeDetails> ids) {
    Objects.requireNonNull(ids, "ids == null");
    Preconditions.checkArgument(ids.iterator().hasNext());
    List<DatanodeDetails> dns = new ArrayList<>();
    ids.forEach(dns::add);
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .setReplication(1)
        .setNodes(dns)
        .build();
  }

  private MockPipeline() {
    throw new UnsupportedOperationException("no instances");
  }
}
