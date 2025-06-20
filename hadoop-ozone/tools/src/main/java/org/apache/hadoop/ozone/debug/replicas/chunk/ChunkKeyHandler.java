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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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

      result.put("volumeName", volumeName);
      result.put("bucketName", bucketName);
      result.put("name", keyName);

      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
          .setBucketName(bucketName).setKeyName(keyName).build();
      OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
      // querying  the keyLocations.The OM is queried to get containerID and
      // localID pertaining to a given key
      List<OmKeyLocationInfo> locationInfos = keyInfo.getLatestVersionLocations() != null ?
          keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly() : null;
      // if key has no replicas
      if (locationInfos == null) {
        System.err.println("No replica/s found.");
        return;
      }

      // for zero-sized key
      if (locationInfos.isEmpty()) {
        System.err.println("No key locations found.");
        return;
      }
      ContainerLayoutVersion containerLayoutVersion = ContainerLayoutVersion
          .getConfiguredVersion(getConf());
      ArrayNode responseArrayList = result.putArray("keyLocations");
      for (OmKeyLocationInfo keyLocation : locationInfos) {
        Pipeline keyPipeline = keyLocation.getPipeline();
        boolean isECKey =
            keyPipeline.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC;
        Pipeline pipeline;
        if (!isECKey && keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
          pipeline = keyPipeline.copyForRead();
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
          ArrayNode responseFromAllNodes = responseArrayList.addArray();
          for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto> entry : responses.entrySet()) {
            DatanodeDetails datanodeDetails = entry.getKey();
            GetBlockResponseProto blockResponse = entry.getValue();

            if (blockResponse == null || !blockResponse.hasBlockData()) {
              System.err.printf("GetBlock call failed on %s datanode and %s block.%n",
                  datanodeDetails.getHostName(), keyLocation.getBlockID());
              continue;
            }

            ContainerProtos.BlockData blockData = blockResponse.getBlockData();
            ContainerProtos.ChunkInfo chunkInfo = blockData.getChunksCount() > 0 ?
                blockData.getChunks(0) : null;

            String fileName = "";
            if (chunkInfo != null) {
              ContainerProtos.ContainerDataProto containerData =
                  readContainerResponses.get(datanodeDetails).getContainerData();
              fileName = containerLayoutVersion.getChunkFile(new File(
                      getChunkLocationPath(containerData.getContainerPath())),
                  keyLocation.getBlockID(),
                  chunkInfo.getChunkName()).toString();
            }

            ObjectNode jsonObj = responseFromAllNodes.addObject();
            ObjectNode dnObj = jsonObj.putObject("datanode");
            dnObj.put("hostname", datanodeDetails.getHostName());
            dnObj.put("ip", datanodeDetails.getIpAddress());
            dnObj.put("uuid", datanodeDetails.getUuidString());

            jsonObj.put("file", fileName);

            ObjectNode blockDataNode = jsonObj.putObject("blockData");
            ObjectNode blockIdNode = blockDataNode.putObject("blockID");
            blockIdNode.put("containerID", blockData.getBlockID().getContainerID());
            blockIdNode.put("localID", blockData.getBlockID().getLocalID());
            blockIdNode.put("blockCommitSequenceId", blockData.getBlockID().getBlockCommitSequenceId());
            blockDataNode.put("size", blockData.getSize());

            ArrayNode chunkArray = blockDataNode.putArray("chunks");
            for (ContainerProtos.ChunkInfo chunk : blockData.getChunksList()) {
              ObjectNode chunkNode = chunkArray.addObject();
              chunkNode.put("offset", chunk.getOffset());
              chunkNode.put("len", chunk.getLen());

              if (chunk.hasChecksumData()) {
                ArrayNode checksums = chunkNode.putArray("checksums");
                for (ByteString bs : chunk.getChecksumData().getChecksumsList()) {
                  checksums.add(StringUtils.byteToHexString(bs.toByteArray()));
                }
                chunkNode.put("checksumType", chunk.getChecksumData().getType().name());
                chunkNode.put("bytesPerChecksum", chunk.getChecksumData().getBytesPerChecksum());
              }

              if (chunk.hasStripeChecksum()) {
                byte[] stripeBytes = chunk.getStripeChecksum().toByteArray();
                int checksumLen = chunk.getChecksumData().getChecksumsList().get(0).size();

                ArrayNode stripeChecksums = chunkNode.putArray("stripeChecksum");
                for (int i = 0; i <= stripeBytes.length - checksumLen; i += checksumLen) {
                  byte[] slice = Arrays.copyOfRange(stripeBytes, i, i + checksumLen);
                  stripeChecksums.add(StringUtils.byteToHexString(slice));
                }
              }
            }

            if (isECKey) {
              int replicaIndex = keyPipeline.getReplicaIndex(entry.getKey());
              int dataCount = ((ECReplicationConfig) keyPipeline.getReplicationConfig()).getData();
              // Index is 1-based,
              // e.g. for RS-3-2 we will have data indexes 1,2,3 and parity indexes 4,5
              ChunkType chunkType = (replicaIndex > dataCount) ? ChunkType.PARITY : ChunkType.DATA;
              jsonObj.put("chunkType", chunkType.name());
              jsonObj.put("replicaIndex", replicaIndex);
            }
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          xceiverClientManager.releaseClientForReadData(xceiverClient, false);
        }
      }
      String prettyJson = JsonUtils.toJsonStringWithDefaultPrettyPrinter(result);
      System.out.println(prettyJson);
    }
  }
}
