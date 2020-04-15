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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.keys.KeyHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.protocol.ClientId;
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
  private final ClientId clientId = ClientId.randomId();
  private OzoneManagerProtocol ozoneManagerClient;

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException, OzoneClientException {
    containerOperationClient = new
            ContainerOperationClient(createOzoneConfiguration());
    xceiverClientManager = containerOperationClient
            .getXceiverClientManager();
    ozoneManagerClient = TracingUtil.createProxy(
            new OzoneManagerProtocolClientSideTranslatorPB(
            getConf(), clientId.toString(),
            null, UserGroupInformation.getCurrentUser()),
            OzoneManagerProtocol.class, getConf());
    address.ensureKeyAddress();
    JsonObject jsonObj = new JsonObject();
    JsonElement element;
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    List<ContainerProtos.ChunkInfo> tempchunks = null;
    List<ChunkDetails> chunkDetailsList = new ArrayList<ChunkDetails>();
    List<String> chunkPaths = new ArrayList<String>();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setRefreshPipeline(true)
            .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    List<OmKeyLocationInfo> locationInfos = keyInfo
            .getLatestVersionLocations().getBlocksLatestVersionOnly();
    // querying  the keyLocations.The OM is queried to get containerID and
    // localID pertaining to a given key
    for (OmKeyLocationInfo keyLocation:locationInfos) {
      ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();
      ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
      long containerId = keyLocation.getContainerID();
      Token<OzoneBlockTokenIdentifier> token = keyLocation.getToken();
      xceiverClient = xceiverClientManager
              .acquireClient(keyLocation.getPipeline());
      // Datanode is queried to get chunk information.Thus querying the
      // OM,SCM and datanode helps us get chunk location information
      if (token != null) {
        UserGroupInformation.getCurrentUser().addToken(token);
      }
      ContainerProtos.DatanodeBlockID datanodeBlockID = keyLocation.getBlockID()
              .getDatanodeBlockIDProtobuf();
      ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls
                 .getBlock(xceiverClient, datanodeBlockID);
      tempchunks = response.getBlockData().getChunksList();
      ContainerProtos.ContainerDataProto containerData =
              containerOperationClient.readContainer(
                      keyLocation.getContainerID(),
                      keyLocation.getPipeline());
      for (ContainerProtos.ChunkInfo chunkInfo:tempchunks) {
        ChunkDetails chunkDetails = new ChunkDetails();
        chunkDetails.setChunkName(chunkInfo.getChunkName());
        chunkDetails.setChunkOffset(chunkInfo.getOffset());
        chunkDetailsList.add(chunkDetails);
        chunkPaths.add(getChunkLocationPath(containerData.getContainerPath())
              + File.separator
              + chunkInfo.getChunkName());
      }
      containerChunkInfoVerbose
              .setContainerPath(containerData.getContainerPath());
      containerChunkInfoVerbose
              .setDataNodeList(keyLocation.getPipeline().getNodes());
      containerChunkInfoVerbose.setPipeline(keyLocation.getPipeline());
      containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
      containerChunkInfo.setChunks(chunkPaths);
      List<ChunkDataNodeDetails> chunkDataNodeDetails = new
              ArrayList<ChunkDataNodeDetails>();
      for (DatanodeDetails datanodeDetails:keyLocation
              .getPipeline().getNodes()) {
        chunkDataNodeDetails.add(
                new ChunkDataNodeDetails(datanodeDetails.getIpAddress(),
                datanodeDetails.getHostName()));
      }
      containerChunkInfo.setChunkDataNodeDetails(chunkDataNodeDetails);
      containerChunkInfo.setPipelineID(
              keyLocation.getPipeline().getId().getId());
      Gson gson = new GsonBuilder().create();
      if (isVerbose()) {
        element = gson.toJsonTree(containerChunkInfoVerbose);
        jsonObj.add("container Id :" + containerId + ""
                + "blockId :" + keyLocation.getLocalID() + "", element);
      } else {
        element = gson.toJsonTree(containerChunkInfo);
        jsonObj.add("container Id :" + containerId + ""
                + "blockId :" + keyLocation.getLocalID() + "", element);
      }
    }
    xceiverClientManager.releaseClient(xceiverClient, false);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(jsonObj);
    System.out.println(prettyJson);
  }


}
