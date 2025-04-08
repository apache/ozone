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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Checks block existence using GetBlock calls to the Datanodes.
 */
public class BlockExistenceVerifier implements ReplicaVerifier {

  private OzoneConfiguration conf;
  private static final String CHECKTYPE = "blockExistence";

  public BlockExistenceVerifier(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OzoneInputStream stream,
        OmKeyLocationInfo keyLocation) {
    try (ContainerOperationClient containerClient = new ContainerOperationClient(conf);
         XceiverClientManager xceiverClientManager = containerClient.getXceiverClientManager()) {

      Pipeline keyPipeline = keyLocation.getPipeline();
      boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;
      Pipeline pipeline = isECKey ? keyPipeline :
          Pipeline.newBuilder(keyPipeline)
              .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
              .build();

      try (XceiverClientSpi client = xceiverClientManager.acquireClientForReadData(pipeline)) {

        Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto> responses =
            ContainerProtocolCalls.getBlockFromAllNodes(
                client,
                keyLocation.getBlockID().getDatanodeBlockIDProtobuf(),
                keyLocation.getToken()
            );

        ContainerProtos.GetBlockResponseProto response = responses.get(datanode);
        boolean hasBlock = response != null && response.hasBlockData();

        return new BlockVerificationResult(CHECKTYPE, hasBlock, hasBlock ? Collections.emptyList() :
            Collections.singletonList(new BlockVerificationResult.FailureDetail(
            true, "Block does not exist on this replica")));
      }

    } catch (IOException | InterruptedException e) {
      BlockVerificationResult.FailureDetail failure = new BlockVerificationResult.FailureDetail(true, e.getMessage());
      return new BlockVerificationResult(CHECKTYPE, false, Collections.singletonList(failure));
    }
  }

}
