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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.keys.KeyHandler;
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

  private String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException, OzoneClientException {
    containerOperationClient = new
            ContainerOperationClient(createOzoneConfiguration());
    xceiverClientManager = new
            XceiverClientManager(createOzoneConfiguration());
    address.ensureKeyAddress();
    JsonObject jsonObj = new JsonObject();
    JsonElement element;
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    List<ContainerProtos.ChunkInfo> tempchunks = null;
    List<ChunkDetails> chunkDetailsList = new ArrayList<ChunkDetails>();
    List<String> chunkPaths = new ArrayList<String>();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    OzoneKeyDetails key = bucket.getKey(keyName);
    // querying  the keyLocations.The OM is queried to get containerID and
    // localID pertaining to a given key
    List<OzoneKeyLocation> keyLocationList = key.getOzoneKeyLocations();
    for (OzoneKeyLocation keyLocation:keyLocationList) {
      ContainerChunkInfo containerChunkInfoVerbose = new ContainerChunkInfo();
      ContainerChunkInfo containerChunkInfo = new ContainerChunkInfo();
      long containerId = keyLocation.getContainerID();
      long blockId = keyLocation.getLocalID();
      // scmClient is used to get container specific
      // information like ContainerPath amd so on
      ContainerWithPipeline container = containerOperationClient
              .getContainerWithPipeline(containerId);
      xceiverClient = xceiverClientManager
              .acquireClient(container.getPipeline());
      // Datanode is queried to get chunk information.Thus querying the
      // OM,SCM and datanode helps us get chunk location information
      ContainerProtos.DatanodeBlockID datanodeBlockID =
              new BlockID(containerId, keyLocation.getLocalID())
                .getDatanodeBlockIDProtobuf();
      ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls
                 .getBlock(xceiverClient, datanodeBlockID);
      tempchunks = response.getBlockData().getChunksList();
      Preconditions.checkNotNull(container, "Container cannot be null");
      ContainerProtos.ContainerDataProto containerData =
              containerOperationClient.readContainer(container
              .getContainerInfo().getContainerID(), container.getPipeline());
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
              .setDataNodeList(container.getPipeline().getNodes());
      containerChunkInfoVerbose.setPipeline(container.getPipeline());
      containerChunkInfoVerbose.setChunkInfos(chunkDetailsList);
      containerChunkInfo.setChunks(chunkPaths);
      List<ChunkDataNodeDetails> chunkDataNodeDetails = new
              ArrayList<ChunkDataNodeDetails>();
      for (DatanodeDetails datanodeDetails:container
              .getPipeline().getNodes()) {
        chunkDataNodeDetails.add(
                new ChunkDataNodeDetails(datanodeDetails.getIpAddress(),
                datanodeDetails.getHostName()));
      }
      containerChunkInfo.setChunkDataNodeDetails(chunkDataNodeDetails);
      containerChunkInfo.setPipelineID(container
              .getPipeline().getId().getId());
      Gson gson = new GsonBuilder().create();
      if (isVerbose()) {
        element = gson.toJsonTree(containerChunkInfoVerbose);
        jsonObj.add("container Id :" + containerId + ""
                + "blockId :" + blockId + "", element);
      } else {
        element = gson.toJsonTree(containerChunkInfo);
        jsonObj.add("container Id :" + containerId + ""
                + "blockId :" + blockId + "", element);
      }
    }
    xceiverClientManager.releaseClient(xceiverClient, false);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(jsonObj);
    System.out.println(prettyJson);
  }


}
