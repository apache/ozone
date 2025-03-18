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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
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
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;

/**
 * Checks block existence using GetBlock calls to the Datanodes.
 */
public class MetadataCheck implements ReplicaVerifier {

  private OzoneClient client;
  private Logger log;
  private PrintWriter printWriter;
  private OzoneConfiguration conf;

  public MetadataCheck(OzoneClient client, Logger log, PrintWriter printWriter, OzoneConfiguration conf) {
    this.client = client;
    this.log = log;
    this.printWriter = printWriter;
    this.conf = conf;
  }

  @Override
  public void verifyKey(OzoneKeyDetails keyDetails) {
    ObjectNode result = JsonUtils.createObjectNode(null);

    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(conf);
         XceiverClientManager xceiverClientManager = containerOperationClient.getXceiverClientManager()) {

      OzoneManagerProtocol ozoneManagerClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
      OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(keyDetails.getVolumeName())
          .setBucketName(keyDetails.getBucketName())
          .setKeyName(keyDetails.getName())
          .build();

      OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
      List<OmKeyLocationInfo> keyLocations = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();

      if (keyLocations.isEmpty()) {
        printJsonResult(keyDetails, "NO_BLOCKS", null, false, result);
        return;
      }

      String blockId = null;
      boolean allReplicasHaveBlock = true;
      for (OmKeyLocationInfo keyLocation : keyLocations) {
        Pipeline keyPipeline = keyLocation.getPipeline();
        boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;

        Pipeline pipeline = isECKey ? keyPipeline :
            Pipeline.newBuilder(keyPipeline).setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE)).build();

        XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);
        try {
          Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto> responses =
              ContainerProtocolCalls.getBlockFromAllNodes(xceiverClient,
                  keyLocation.getBlockID().getDatanodeBlockIDProtobuf(), keyLocation.getToken());

          blockId = keyLocation.getBlockID().toString();
          int totalExpectedReplicas = responses.size();
          int availableReplicas = 0;

          for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto> entry : responses.entrySet()) {
            if (entry.getValue() != null && entry.getValue().hasBlockData()) {
              availableReplicas++;
            }
          }

          if (availableReplicas < totalExpectedReplicas || totalExpectedReplicas == 0) {
            allReplicasHaveBlock = false;
          }

        } finally {
          xceiverClientManager.releaseClientForReadData(xceiverClient, false);
        }
      }

      if (allReplicasHaveBlock) {
        printJsonResult(keyDetails, "BLOCK_EXISTS", blockId, true, result);
      } else {
        printJsonResult(keyDetails, "MISSING_REPLICAS", blockId, false, result);
      }

    } catch (IOException | InterruptedException e) {
      log.error("Error checking block existence for key {}: {}", keyDetails.getName(), e.getMessage());
      printJsonError(keyDetails, e.getMessage(), false, result);
    }
  }

  /**
   * Helper method to print JSON results.
   */
  private void printJsonResult(OzoneKeyDetails keyParts, String status, String blockId,
                               boolean pass, ObjectNode result) {
    result.put("key", keyParts.getVolumeName() + "/" + keyParts.getBucketName() + "/" + keyParts.getName());
    result.put("blockID", blockId);
    result.put("status", status);
    result.put("pass", pass);

    printWriter.println(result);
  }

  /**
   * Helper method to print JSON error messages.
   */
  private void printJsonError(OzoneKeyDetails keyParts, String errorMessage, boolean pass, ObjectNode result) {
    result.put("key", keyParts.getVolumeName() + "/" + keyParts.getBucketName() + "/" + keyParts.getName());
    result.put("status", "ERROR");
    result.put("message", errorMessage);
    result.put("pass", pass);

    printWriter.println(result);
  }

}
