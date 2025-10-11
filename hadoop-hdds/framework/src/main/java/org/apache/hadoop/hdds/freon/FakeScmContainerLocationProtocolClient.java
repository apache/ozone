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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Node;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake SCM client to return a simulated block location.
 */
public final class FakeScmContainerLocationProtocolClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FakeScmContainerLocationProtocolClient.class);

  private FakeScmContainerLocationProtocolClient() {
  }

  public static ScmContainerLocationResponse submitRequest(
      ScmContainerLocationRequest req)
      throws IOException {
    try {
      if (req.getCmdType() == Type.QueryNode) {
        Builder builder = NodeQueryResponseProto.newBuilder();
        for (DatanodeDetailsProto datanode : FakeClusterTopology.INSTANCE
            .getAllDatanodes()) {
          builder.addDatanodes(Node.newBuilder()
              .setNodeID(datanode)
              .addNodeStates(NodeState.HEALTHY)
              .build());
        }

        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(Type.QueryNode)
            .setStatus(Status.OK)
            .setNodeQueryResponse(builder.build())
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
