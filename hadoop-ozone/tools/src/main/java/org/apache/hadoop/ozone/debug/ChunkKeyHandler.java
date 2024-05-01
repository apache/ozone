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

package org.apache.hadoop.ozone.debug;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
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
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

/**
 * Class that gives chunk location given a specific key.
 */
@Command(name = "chunkinfo",
        description = "returns chunk location"
                + " information about an existing key")
@MetaInfServices(SubcommandWithParent.class)
public class ChunkKeyHandler extends KeyHandler implements
    SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException, OzoneClientException {
    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(parent.getOzoneConf());
        XceiverClientManager xceiverClientManager = containerOperationClient.getXceiverClientManager()) {
      OzoneManagerProtocol ozoneManagerClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
      address.ensureKeyAddress();
      ObjectNode result = JsonUtils.createObjectNode(null);
      String volumeName = address.getVolumeName();
      String bucketName = address.getBucketName();
      String keyName = address.getKeyName();
      List<ContainerProtos.ChunkInfo> tempchunks;
      List<ChunkDetails> chunkDetailsList = new ArrayList<>();
      HashSet<String> chunkPaths = new HashSet<>();
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
        ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();
        ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
        long containerId = keyLocation.getContainerID();
        chunkPaths.clear();
        Pipeline keyPipeline = keyLocation.getPipeline();
        boolean isECKey =
            keyPipeline.getReplicationConfig().getReplicationType() ==
                HddsProtos.ReplicationType.EC;
        Pipeline pipeline;
        if (keyPipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
          pipeline = Pipeline.newBuilder(keyPipeline)
              .setReplicationConfig(StandaloneReplicationConfig
                  .getInstance(ONE)).build();
        } else {
          pipeline = keyPipeline;
        }
        XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);
        try {
          // Datanode is queried to get chunk information.Thus querying the
          // OM,SCM and datanode helps us get chunk location information
          ContainerProtos.DatanodeBlockID datanodeBlockID =
              keyLocation.getBlockID().getDatanodeBlockIDProtobuf();
          // doing a getBlock on all nodes
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
            chunkPaths.clear();
            ObjectNode jsonObj = JsonUtils.createObjectNode(null);
            if (entry.getValue() == null) {
              LOG.error("Cant execute getBlock on this node");
              continue;
            }
            tempchunks = entry.getValue().getBlockData().getChunksList();
            ContainerProtos.ContainerDataProto containerData =
                readContainerResponses.get(entry.getKey()).getContainerData();
            for (ContainerProtos.ChunkInfo chunkInfo : tempchunks) {
              String fileName = containerLayoutVersion.getChunkFile(new File(
                      getChunkLocationPath(containerData.getContainerPath())),
                  keyLocation.getBlockID(),
                  chunkInfo.getChunkName()).toString();
              chunkPaths.add(fileName);
              ChunkDetails chunkDetails = new ChunkDetails();
              chunkDetails.setChunkName(fileName);
              chunkDetails.setChunkOffset(chunkInfo.getOffset());
              chunkDetailsList.add(chunkDetails);
            }
            containerChunkInfoVerbose.setContainerPath(containerData
                .getContainerPath());
            containerChunkInfoVerbose.setPipeline(keyPipeline);
            containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
            containerChunkInfo.setFiles(chunkPaths);
            containerChunkInfo.setPipelineID(keyPipeline.getId().getId());
            if (isECKey) {
              ChunkType blockChunksType =
                  isECParityBlock(keyPipeline, entry.getKey()) ?
                      ChunkType.PARITY : ChunkType.DATA;
              containerChunkInfoVerbose.setChunkType(blockChunksType);
              containerChunkInfo.setChunkType(blockChunksType);
            }

            if (isVerbose()) {
              jsonObj.set("Locations",
                  JsonUtils.createObjectNode(containerChunkInfoVerbose));
            } else {
              jsonObj.set("Locations",
                  JsonUtils.createObjectNode(containerChunkInfo));
            }
            jsonObj.put("Datanode-HostName", entry.getKey().getHostName());
            jsonObj.put("Datanode-IP", entry.getKey().getIpAddress());
            jsonObj.put("Container-ID", containerId);
            jsonObj.put("Block-ID", keyLocation.getLocalID());
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

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

}
