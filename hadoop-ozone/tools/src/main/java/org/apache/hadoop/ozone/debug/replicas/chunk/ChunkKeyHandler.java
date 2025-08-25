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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
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
  @SuppressWarnings("checkstyle:methodlength")
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException {
    try (ContainerOperationClient containerOperationClient = new ContainerOperationClient(getOzoneConf());
        XceiverClientManager xceiverClientManager = containerOperationClient.getXceiverClientManager()) {
      OzoneManagerProtocol ozoneManagerClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
      address.ensureKeyAddress();
      String volumeName = address.getVolumeName();
      String bucketName = address.getBucketName();
      String keyName = address.getKeyName();

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

      // Use Jackson streaming for all JSON generation
      ObjectMapper mapper = new ObjectMapper();
      JsonFactory jsonFactory = mapper.getFactory();

      try (JsonGenerator jsonGen = jsonFactory.createGenerator(System.out)) {
        jsonGen.useDefaultPrettyPrinter();

        jsonGen.writeStartObject();
        jsonGen.writeStringField("volumeName", volumeName);
        jsonGen.writeStringField("bucketName", bucketName);
        jsonGen.writeStringField("name", keyName);

        // Start keyLocations array
        jsonGen.writeArrayFieldStart("keyLocations");
        for (OmKeyLocationInfo keyLocation : locationInfos) {
          jsonGen.writeStartArray();

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
            Map<DatanodeDetails, ContainerProtos.ReadContainerResponseProto> readContainerResponses =
                containerOperationClient.readContainerFromAllNodes(keyLocation.getContainerID(), pipeline);

            // Process each datanode individually
            for (DatanodeDetails datanodeDetails : pipeline.getNodes()) {
              try {
                // Get block from THIS ONE datanode only
                ContainerProtos.GetBlockResponseProto blockResponse =
                    ContainerProtocolCalls.getBlock(xceiverClient,
                        keyLocation.getBlockID(),
                        keyLocation.getToken(),
                        pipeline.getReplicaIndexes());

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

                // Start writing this datanode's response object
                jsonGen.writeStartObject();

                jsonGen.writeObjectFieldStart("datanode");
                jsonGen.writeStringField("hostname", datanodeDetails.getHostName());
                jsonGen.writeStringField("ip", datanodeDetails.getIpAddress());
                jsonGen.writeStringField("uuid", datanodeDetails.getUuidString());
                jsonGen.writeEndObject();

                jsonGen.writeStringField("file", fileName);

                // Write block data
                jsonGen.writeObjectFieldStart("blockData");
                jsonGen.writeObjectFieldStart("blockID");
                jsonGen.writeNumberField("containerID", blockData.getBlockID().getContainerID());
                jsonGen.writeNumberField("localID", blockData.getBlockID().getLocalID());
                jsonGen.writeNumberField("blockCommitSequenceId", blockData.getBlockID().getBlockCommitSequenceId());
                jsonGen.writeEndObject();

                jsonGen.writeNumberField("size", blockData.getSize());

                // Write chunks array
                jsonGen.writeArrayFieldStart("chunks");
                for (ContainerProtos.ChunkInfo chunk : blockData.getChunksList()) {
                  jsonGen.writeStartObject();
                  jsonGen.writeNumberField("offset", chunk.getOffset());
                  jsonGen.writeNumberField("len", chunk.getLen());

                  if (chunk.hasChecksumData()) {
                    jsonGen.writeArrayFieldStart("checksums");
                    for (ByteString bs : chunk.getChecksumData().getChecksumsList()) {
                      jsonGen.writeString(StringUtils.byteToHexString(bs.toByteArray()));
                    }

                    jsonGen.writeEndArray();
                    jsonGen.writeStringField("checksumType", chunk.getChecksumData().getType().name());
                    jsonGen.writeNumberField("bytesPerChecksum", chunk.getChecksumData().getBytesPerChecksum());
                  }

                  if (chunk.hasStripeChecksum()) {
                    byte[] stripeBytes = chunk.getStripeChecksum().toByteArray();
                    int checksumLen = chunk.getChecksumData().getChecksumsList().get(0).size();

                    jsonGen.writeArrayFieldStart("stripeChecksum");
                    for (int i = 0; i <= stripeBytes.length - checksumLen; i += checksumLen) {
                      byte[] slice = Arrays.copyOfRange(stripeBytes, i, i + checksumLen);
                      jsonGen.writeString(StringUtils.byteToHexString(slice));
                    }
                    jsonGen.writeEndArray();
                  }
                  jsonGen.writeEndObject();
                }

                jsonGen.writeEndArray(); // End chunks array
                jsonGen.writeEndObject(); // End blockData object

                if (isECKey) {
                  int replicaIndex = keyPipeline.getReplicaIndex(datanodeDetails);
                  int dataCount = ((ECReplicationConfig) keyPipeline.getReplicationConfig()).getData();
                  // Index is 1-based,
                  // e.g. for RS-3-2 we will have data indexes 1,2,3 and parity indexes 4,5
                  ChunkType chunkType = (replicaIndex > dataCount) ? ChunkType.PARITY : ChunkType.DATA;
                  jsonGen.writeStringField("chunkType", chunkType.name());
                  jsonGen.writeNumberField("replicaIndex", replicaIndex);
                }
                jsonGen.writeEndObject(); // End this datanode's response object

                jsonGen.flush();
              } catch (Exception e) {
                System.err.printf("Error getting block from datanode %s: %s%n",
                    datanodeDetails.getHostName(), e.getMessage());
              }
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } finally {
            xceiverClientManager.releaseClientForReadData(xceiverClient, false);
          }

          jsonGen.writeEndArray();
        }

        jsonGen.writeEndArray(); // End keyLocations array
        jsonGen.writeEndObject(); // End root object
        jsonGen.flush();
        System.out.println();
      }
    }
  }
}
