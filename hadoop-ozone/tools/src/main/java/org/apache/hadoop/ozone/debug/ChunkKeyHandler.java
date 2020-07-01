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
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Class that gives chunk location given a specific key.
 */
@Command(name = "chunkinfo",
        description = "returns chunk location"
                + " information about an existing key")
public class ChunkKeyHandler  extends KeyHandler {

  @Parameters(arity = "1..1", description = "key to be located")
    private String uri;

  private ContainerOperationClient containerOperationClient;
  private  XceiverClientManager xceiverClientManager;
  private XceiverClientSpi xceiverClient;
  private OzoneManagerProtocol ozoneManagerClient;

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException, OzoneClientException{
    containerOperationClient = new
        ContainerOperationClient(createOzoneConfiguration());
    xceiverClientManager = containerOperationClient
        .getXceiverClientManager();
    ozoneManagerClient = client.getObjectStore().getClientProxy()
            .getOzoneManagerClient();
    address.ensureKeyAddress();
    JsonElement element;
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    List<ContainerProtos.ChunkInfo> tempchunks = null;
    List<ChunkDetails> chunkDetailsList = new ArrayList<ChunkDetails>();
    HashSet<String> chunkPaths = new HashSet<>();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setRefreshPipeline(true)
            .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    // querying  the keyLocations.The OM is queried to get containerID and
    // localID pertaining to a given key
    List<OmKeyLocationInfo> locationInfos = keyInfo
            .getLatestVersionLocations().getBlocksLatestVersionOnly();
    // for zero-sized key
    if(locationInfos.isEmpty()){
      System.out.println("No Key Locations Found");
      return;
    }
    ChunkLayOutVersion chunkLayOutVersion = ChunkLayOutVersion
            .getConfiguredVersion(getConf());
    for (OmKeyLocationInfo keyLocation:locationInfos) {
      JsonObject jsonObj = new JsonObject();
      ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();
      ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
      long containerId = keyLocation.getContainerID();
      chunkPaths.clear();
      Token<OzoneBlockTokenIdentifier> token = keyLocation.getToken();
      Pipeline pipeline = keyLocation.getPipeline();
      if (pipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
        pipeline = Pipeline.newBuilder(pipeline)
                .setType(HddsProtos.ReplicationType.STAND_ALONE).build();
      }
      xceiverClient = xceiverClientManager
              .acquireClientForReadData(pipeline);
      // Datanode is queried to get chunk information.Thus querying the
      // OM,SCM and datanode helps us get chunk location information
      if (token != null) {
        UserGroupInformation.getCurrentUser().addToken(token);
      }
      ContainerProtos.DatanodeBlockID datanodeBlockID = keyLocation.getBlockID()
              .getDatanodeBlockIDProtobuf();
      // doing a getBlock on all nodes
      HashMap<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
              responses = null;
      try {
        responses = ContainerProtocolCalls
                .getBlockFromAllNodes(xceiverClient, datanodeBlockID);
      } catch (InterruptedException e) {
        LOG.error("Execution interrupted due to " + e);
      }
      for (Map.Entry<DatanodeDetails, ContainerProtos.GetBlockResponseProto>
              entry: responses.entrySet()) {
        if(entry.getValue() == null){
          LOG.error("Cant execute getBlock on this node");
          continue;
        }
        tempchunks = entry.getValue().getBlockData().getChunksList();
        ContainerProtos.ContainerDataProto containerData =
                containerOperationClient.readContainer(
                        keyLocation.getContainerID(),
                        keyLocation.getPipeline());
        for (ContainerProtos.ChunkInfo chunkInfo : tempchunks) {
          ChunkDetails chunkDetails = new ChunkDetails();
          chunkDetails.setChunkName(chunkInfo.getChunkName());
          chunkDetails.setChunkOffset(chunkInfo.getOffset());
          chunkDetailsList.add(chunkDetails);
          chunkPaths.add(chunkLayOutVersion.getChunkFile(new File(
              getChunkLocationPath(containerData.getContainerPath())),
                  keyLocation.getBlockID(),
                  ChunkInfo.getFromProtoBuf(chunkInfo)).toString());
        }
        containerChunkInfoVerbose
                .setContainerPath(containerData.getContainerPath());
        containerChunkInfoVerbose.setPipeline(keyLocation.getPipeline());
        containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
        containerChunkInfo.setFiles(chunkPaths);
        containerChunkInfo.setPipelineID(
                keyLocation.getPipeline().getId().getId());
        Gson gson = new GsonBuilder().create();
        if (isVerbose()) {
          element = gson.toJsonTree(containerChunkInfoVerbose);
        } else {
          element = gson.toJsonTree(containerChunkInfo);
        }
        jsonObj.addProperty("Datanode HostName ", entry.getKey().getHostName());
        jsonObj.addProperty("Datanode IP", entry.getKey().getIpAddress());
        jsonObj.addProperty("Container ID", containerId);
        jsonObj.addProperty("Block ID", keyLocation.getLocalID());
        jsonObj.add("Locations", element);
        xceiverClientManager.releaseClientForReadData(xceiverClient, false);
        Gson gson2 = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson2.toJson(jsonObj);
        System.out.println(prettyJson);
      }
    }
  }


}
