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

package org.apache.hadoop.ozone.debug.replicas.chunk;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.apache.ratis.thirdparty.com.google.protobuf.util.JsonFormat;
import picocli.CommandLine.Command;

/**
 * Class that gives chunk location given a specific key.
 */
@Command(name = "chunk-info",
        description = "Returns chunk location information about an existing key")
public class ChunkKeyHandler extends KeyHandler {

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException {
    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(getOzoneConf());
        XceiverClientManager xceiverClientManager = containerOperationClient.getXceiverClientManager()) {
      OzoneManagerProtocol ozoneManagerClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
      address.ensureKeyAddress();
      ObjectNode result = JsonUtils.createObjectNode(null);
      String volumeName = address.getVolumeName();
      String bucketName = address.getBucketName();
      String keyName = address.getKeyName();
      List<ContainerProtos.ChunkInfo> tempchunks;

      result.put("Volume-Name", volumeName);
      result.put("Bucket-Name", bucketName);
      result.put("Key-Name", keyName);

      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
          .setBucketName(bucketName).setKeyName(keyName).build();
      OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
      // querying  the keyLocations.The OM is queried to get containerID and
      // localID pertaining to a given key
      List<OmKeyLocationInfo> locationInfos =
          keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
      // for zero-sized key
      if (locationInfos.isEmpty()) {
        System.out.println("No Key Locations Found");
        return;
      }
      ContainerLayoutVersion containerLayoutVersion = ContainerLayoutVersion
          .getConfiguredVersion(getConf());
      ArrayNode responseArrayList = JsonUtils.createArrayNode();
      for (OmKeyLocationInfo keyLocation : locationInfos) {
        Pipeline keyPipeline = keyLocation.getPipeline();
        boolean isECKey =
            keyPipeline.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC;
        Pipeline pipeline;
        if (!isECKey && keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
          pipeline = Pipeline.newBuilder(keyPipeline)
              .setReplicationConfig(StandaloneReplicationConfig
                  .getInstance(ONE)).build();
        } else {
          pipeline = keyPipeline;
        }
        XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);
        try {
          Map<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
              responses =
              ContainerProtocolCalls.getBlockFromAllNodes(xceiverClient,
                  keyLocation.getBlockID().getDatanodeBlockIDProtobuf(),
                  keyLocation.getToken());
          Map<DatanodeDetails, ContainerProtos.ReadContainerResponseProto> readContainerResponses =
              containerOperationClient.readContainerFromAllNodes(
                  keyLocation.getContainerID(), pipeline);
          ArrayNode responseFromAllNodes = JsonUtils.createArrayNode();
          for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto> entry : responses.entrySet()) {
            ObjectNode jsonObj = JsonUtils.createObjectNode(null);
            if (entry.getValue() == null) {
              LOG.error("Can't execute getBlock on this node");
              continue;
            }
            tempchunks = entry.getValue().getBlockData().getChunksList();
            ContainerProtos.ContainerDataProto containerData =
                readContainerResponses.get(entry.getKey()).getContainerData();
            ArrayNode chunkFilePaths = JsonUtils.createArrayNode();
            for (ContainerProtos.ChunkInfo chunkInfo : tempchunks) {
              String fileName = containerLayoutVersion.getChunkFile(new File(
                      getChunkLocationPath(containerData.getContainerPath())),
                  keyLocation.getBlockID(),
                  chunkInfo.getChunkName()).toString();
              chunkFilePaths.add(fileName);
            }
            jsonObj.put("Datanode-HostName", entry.getKey().getHostName());
            jsonObj.put("Datanode-IP", entry.getKey().getIpAddress());

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode blockDataNode = objectMapper.readTree(JsonFormat.printer().print(entry.getValue().getBlockData()));
            jsonObj.set("Block-Data", blockDataNode);

            jsonObj.set("Chunk-Files", chunkFilePaths);

            if (isECKey) {
              ChunkType blockChunksType = isECParityBlock(keyPipeline, entry.getKey()) ?
                  ChunkType.PARITY : ChunkType.DATA;
              jsonObj.put("Chunk-Type", blockChunksType.name());
            }
            responseFromAllNodes.add(jsonObj);
          }
          responseArrayList.add(responseFromAllNodes);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          xceiverClientManager.releaseClientForReadData(xceiverClient, false);
        }
      }
      result.set("KeyLocations", responseArrayList);
      String prettyJson = JsonUtils.toJsonStringWithDefaultPrettyPrinter(result);
      System.out.println(prettyJson);
    }
  }

  private boolean isECParityBlock(Pipeline pipeline, DatanodeDetails dn) {
    //index is 1-based,
    //e.g. for RS-3-2 we will have data indexes 1,2,3 and parity indexes 4,5
    return pipeline.getReplicaIndex(dn) >
        ((ECReplicationConfig) pipeline.getReplicationConfig()).getData();
  }
}
