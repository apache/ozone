/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.client.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager for EC writes.
 */
public class ECBlockOutputStreamEntryPool extends BlockOutputStreamEntryPool {
  private final List<BlockOutputStreamEntry> finishedStreamEntries;
  private final ECReplicationConfig ecReplicationConfig;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public ECBlockOutputStreamEntryPool(OzoneClientConfig config,
      OzoneManagerProtocol omClient,
      String requestId,
      ReplicationConfig replicationConfig,
      String uploadID,
      int partNumber,
      boolean isMultipart,
      OmKeyInfo info,
      boolean unsafeByteBufferConversion,
      XceiverClientFactory xceiverClientFactory,
      long openID) {
    super(config, omClient, requestId, replicationConfig, uploadID, partNumber,
        isMultipart, info, unsafeByteBufferConversion, xceiverClientFactory,
        openID);
    this.finishedStreamEntries = new ArrayList<>();
    assert replicationConfig instanceof ECReplicationConfig;
    this.ecReplicationConfig = (ECReplicationConfig) replicationConfig;
  }

  @Override
  void addKeyLocationInfo(OmKeyLocationInfo subKeyInfo) {
    Preconditions.checkNotNull(subKeyInfo.getPipeline());
    List<DatanodeDetails> nodes = subKeyInfo.getPipeline().getNodes();
    for (int i = 0; i < nodes.size(); i++) {
      List<DatanodeDetails> nodeStatus = new ArrayList<>();
      nodeStatus.add(nodes.get(i));
      Map<DatanodeDetails, Integer> nodeVsIdx = new HashMap<>();
      nodeVsIdx.put(nodes.get(i), i + 1);
      Pipeline pipeline =
          Pipeline.newBuilder().setId(subKeyInfo.getPipeline().getId())
              .setReplicationConfig(
                  subKeyInfo.getPipeline().getReplicationConfig())
              .setState(subKeyInfo.getPipeline().getPipelineState())
              .setNodes(nodeStatus).setReplicaIndexes(nodeVsIdx).build();

      ECBlockOutputStreamEntry.Builder builder =
          new ECBlockOutputStreamEntry.Builder()
              .setBlockID(subKeyInfo.getBlockID()).setKey(getKeyName())
              .setXceiverClientManager(getXceiverClientFactory())
              .setPipeline(pipeline).setConfig(getConfig())
              .setLength(subKeyInfo.getLength()).setBufferPool(getBufferPool())
              .setToken(subKeyInfo.getToken())
              .setIsParityStreamEntry(i >= ecReplicationConfig.getData());
      getStreamEntries().add(builder.build());
    }
  }

  public List<OmKeyLocationInfo> getLocationInfoList() {
    List<OmKeyLocationInfo> locationInfoList;
    List<OmKeyLocationInfo> currBlocksLocationInfoList =
        getOmKeyLocationInfos(getStreamEntries());
    List<OmKeyLocationInfo> prevBlksKeyLocationInfos =
        getOmKeyLocationInfos(finishedStreamEntries);
    prevBlksKeyLocationInfos.addAll(currBlocksLocationInfoList);
    locationInfoList = prevBlksKeyLocationInfos;
    return locationInfoList;
  }

  @Override
  long getKeyLength() {
    long totalLength = getStreamEntries().stream()
        .filter(c -> !((ECBlockOutputStreamEntry) c).isParityStreamEntry())
        .mapToLong(BlockOutputStreamEntry::getCurrentPosition).sum();

    totalLength += finishedStreamEntries.stream()
        .filter(c -> !((ECBlockOutputStreamEntry) c).isParityStreamEntry())
        .mapToLong(BlockOutputStreamEntry::getCurrentPosition).sum();
    return totalLength;
  }

  @Override
  List<OmKeyLocationInfo> getOmKeyLocationInfos(
      List<BlockOutputStreamEntry> streams) {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    Map<BlockID, ArrayList<ECBlockOutputStreamEntry>> blkIdVsStream =
        new LinkedHashMap<>();

    for (BlockOutputStreamEntry streamEntry : streams) {
      BlockID blkID = streamEntry.getBlockID();
      final ArrayList<ECBlockOutputStreamEntry> stream =
          blkIdVsStream.getOrDefault(blkID, new ArrayList<>());
      stream.add((ECBlockOutputStreamEntry) streamEntry);
      blkIdVsStream.put(blkID, stream);
    }

    final Iterator<Map.Entry<BlockID, ArrayList<ECBlockOutputStreamEntry>>>
        iterator = blkIdVsStream.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<BlockID, ArrayList<ECBlockOutputStreamEntry>>
          blkGrpIDVsStreams = iterator.next();
      final ArrayList<ECBlockOutputStreamEntry> blkGrpStreams =
          blkGrpIDVsStreams.getValue();
      List<DatanodeDetails> nodeStatus = new ArrayList<>();
      Map<DatanodeDetails, Integer> nodeVsIdx = new HashMap<>();

      // Assumption: Irrespective of failures, stream entries must have updated
      // the lengths.
      long blkGRpLen = 0;
      for (ECBlockOutputStreamEntry internalBlkStream : blkGrpStreams) {
        blkGRpLen += !(internalBlkStream).isParityStreamEntry() ?
            internalBlkStream.getCurrentPosition() :
            0;
        // In EC, only one node per internal block stream.
        final DatanodeDetails nodeDetails =
            internalBlkStream.getPipeline().getNodeSet().iterator().next();
        nodeStatus.add(nodeDetails);
        nodeVsIdx.put(nodeDetails,
            internalBlkStream.getPipeline().getReplicaIndex(nodeDetails));
      }
      nodeStatus.sort((o1, o2) -> nodeVsIdx.get(o1) - nodeVsIdx.get(o2));
      final BlockOutputStreamEntry firstStreamInBlockGrp = blkGrpStreams.get(0);
      Pipeline blockGrpPipeline = Pipeline.newBuilder()
          .setId(firstStreamInBlockGrp.getPipeline().getId())
          .setReplicationConfig(
              firstStreamInBlockGrp.getPipeline().getReplicationConfig())
          .setState(firstStreamInBlockGrp.getPipeline().getPipelineState())
          .setNodes(nodeStatus).setReplicaIndexes(nodeVsIdx).build();
      // Commit only those blocks to OzoneManager which are not empty
      if (blkGRpLen != 0) {
        OmKeyLocationInfo info = new OmKeyLocationInfo.Builder()
            .setBlockID(blkGrpIDVsStreams.getKey()).setLength(blkGRpLen)
            .setOffset(0).setToken(firstStreamInBlockGrp.getToken())
            .setPipeline(blockGrpPipeline).build();
        locationInfoList.add(info);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("block written " + firstStreamInBlockGrp
            .getBlockID() + ", length " + blkGRpLen + " bcsID "
            + firstStreamInBlockGrp.getBlockID().getBlockCommitSequenceId());
      }
    }
    return locationInfoList;
  }

  public void endECBlock() throws IOException {
    List<BlockOutputStreamEntry> entries = getStreamEntries();
    if (entries.size() > 0) {
      for (int i = entries.size() - 1; i >= 0; i--) {
        finishedStreamEntries.add(entries.remove(i));
      }
    }

    for (BlockOutputStreamEntry entry : finishedStreamEntries) {
      entry.close();
    }
    super.cleanup();
  }

  void executePutBlockForAll() throws IOException {
    List<BlockOutputStreamEntry> streamEntries = getStreamEntries();
    int failedStreams = 0;
    for (int i = 0; i < streamEntries.size(); i++) {
      ECBlockOutputStreamEntry ecBlockOutputStreamEntry =
          (ECBlockOutputStreamEntry) streamEntries.get(i);
      if (!ecBlockOutputStreamEntry.isClosed()) {
        if(!ecBlockOutputStreamEntry.isInitialized()){
          // Stream not initialized. Means this stream was not used to write.
          continue;
        }
        ecBlockOutputStreamEntry.executePutBlock();
      }else{
        failedStreams++;
      }
    }
    if(failedStreams > ecReplicationConfig.getParity()) {
      throw new IOException(
          "There are " + failedStreams + " failures than supported tolerance: "
              + ecReplicationConfig.getParity());
    }
  }

  void cleanupAll() {
    super.cleanup();
    if (finishedStreamEntries != null) {
      finishedStreamEntries.clear();
    }
  }

  public void updateToNextStream(int rotation) {
    super.setCurrIdx((getCurrIdx() + 1) % rotation);
  }

}
