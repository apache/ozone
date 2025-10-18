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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Abstract class used as an interface for input streams related to Ozone
 * blocks.
 */
public abstract class BlockExtendedInputStream extends ExtendedInputStream
    implements PartInputStream {

  public abstract BlockID getBlockID();

  @Override
  public long getRemaining() {
    return getLength() - getPos();
  }

  @Override
  public abstract long getLength();

  @Override
  public abstract long getPos();

  protected Pipeline setPipeline(Pipeline pipeline) throws IOException {
    if (pipeline == null) {
      return null;
    }
    long replicaIndexes = pipeline.getNodes().stream().mapToInt(pipeline::getReplicaIndex).distinct().count();

    if (replicaIndexes > 1) {
      throw new IOException(String.format("Pipeline: %s has nodes containing different replica indexes.",
          pipeline));
    }

    // irrespective of the container state, we will always read via Standalone protocol.
    boolean okForRead = pipeline.getType() == HddsProtos.ReplicationType.STAND_ALONE
            || pipeline.getType() == HddsProtos.ReplicationType.EC;
    return okForRead ? pipeline : pipeline.copyForRead();
  }

}
