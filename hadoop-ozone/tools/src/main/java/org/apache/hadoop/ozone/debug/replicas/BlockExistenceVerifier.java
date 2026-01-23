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

package org.apache.hadoop.ozone.debug.replicas;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Verifies block existence by making getBlock calls to the datanode.
 */
public class BlockExistenceVerifier implements ReplicaVerifier {
  private final ContainerOperationClient containerClient;
  private final XceiverClientManager xceiverClientManager;
  private static final String CHECK_TYPE = "blockExistence";

  @Override
  public String getType() {
    return CHECK_TYPE;
  }

  public BlockExistenceVerifier(OzoneConfiguration conf) throws IOException {
    this.containerClient = new ContainerOperationClient(conf);
    this.xceiverClientManager = containerClient.getXceiverClientManager();
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OmKeyLocationInfo keyLocation) {
    XceiverClientSpi client = null;
    try {
      Pipeline pipeline = keyLocation.getPipeline().copyForReadFromNode(datanode);

      client = xceiverClientManager.acquireClientForReadData(pipeline);
      ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls.getBlock(
          client,
          keyLocation.getBlockID(),
          keyLocation.getToken(),
          pipeline.getReplicaIndexes()
      );

      boolean hasBlock = response != null && response.hasBlockData();

      if (hasBlock) {
        return BlockVerificationResult.pass();
      } else {
        return BlockVerificationResult.failCheck("Block does not exist on this replica");
      }
    } catch (IOException e) {
      return BlockVerificationResult.failIncomplete(e.getMessage());
    } finally {
      xceiverClientManager.releaseClient(client, false);
    }
  }
}
