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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.GetScmInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake SCM client to return a simulated block location.
 */
public final class FakeScmBlockLocationProtocolClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FakeScmBlockLocationProtocolClient.class);

  public static final int BLOCK_PER_CONTAINER = 1000;

  private static AtomicLong counter = new AtomicLong();

  private FakeScmBlockLocationProtocolClient() {
  }

  public static SCMBlockLocationResponse submitRequest(
      SCMBlockLocationRequest req)
      throws IOException {
    try {
      if (req.getCmdType() == Type.GetScmInfo) {
        return SCMBlockLocationResponse.newBuilder()
            .setCmdType(req.getCmdType())
            .setStatus(Status.OK)
            .setSuccess(true)
            .setGetScmInfoResponse(
                GetScmInfoResponseProto.newBuilder()
                    .setScmId("scm-id")
                    .setClusterId("cluster-id")
                    .build()
            )
            .build();
      } else if (req.getCmdType() == Type.AllocateScmBlock) {
        Builder allocateBlockResponse =
            AllocateScmBlockResponseProto.newBuilder();
        for (int i = 0;
             i < req.getAllocateScmBlockRequest().getNumBlocks(); i++) {
          long seq = counter.incrementAndGet();

          allocateBlockResponse.addBlocks(AllocateBlockResponse.newBuilder()
              .setPipeline(FakeClusterTopology.INSTANCE.getRandomPipeline())
              .setContainerBlockID(ContainerBlockID.newBuilder()
                  .setContainerID(seq / BLOCK_PER_CONTAINER)
                  .setLocalID(seq))
          );
        }
        return SCMBlockLocationResponse.newBuilder()
            .setCmdType(req.getCmdType())
            .setStatus(Status.OK)
            .setSuccess(true)
            .setAllocateScmBlockResponse(
                allocateBlockResponse
            )
            .build();
      } else {
        throw new IllegalArgumentException(
            "Unsupported request. Fake answer is not implemented for " + req
                .getCmdType());
      }
    } catch (Exception ex) {
      LOGGER.error("Error on creating fake SCM response", ex);
      return null;
    }
  }

}
