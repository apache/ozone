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
import java.util.List;
import java.util.Map;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager for EC writes.
 */
public class ECBlockOutputStreamEntryPool extends BlockOutputStreamEntryPool {
  private final List<List<BlockOutputStreamEntry>> finishedStreamEntries;
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

  @Override
  public List<OmKeyLocationInfo> getLocationInfoList() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (List<BlockOutputStreamEntry> blk : finishedStreamEntries) {
      locationInfoList.add(buildKeyLocationForBlock(blk));
    }
    List<BlockOutputStreamEntry> currentEntries = getStreamEntries();
    if (currentEntries.size() > 0) {
      locationInfoList.add(buildKeyLocationForBlock(getStreamEntries()));
    }
    return locationInfoList;
  }

  private OmKeyLocationInfo buildKeyLocationForBlock(
      List<BlockOutputStreamEntry> blocks) {
    long length = 0;
    BlockOutputStreamEntry first = blocks.get(0);
    for (BlockOutputStreamEntry e : blocks) {
      if (((ECBlockOutputStreamEntry)e).isParityStreamEntry()) {
        continue;
      }
      length += e.getLength();
    }
    OmKeyLocationInfo info =
        new OmKeyLocationInfo.Builder().setBlockID(first.getBlockID())
            .setLength(length).setOffset(0)
            .setToken(first.getToken())
            .setPipeline(first.getPipeline()).build();
    return info;
  }

  @Override
  long getKeyLength() {
    long totalLength = getStreamEntries().stream()
        .filter(c -> !((ECBlockOutputStreamEntry) c).isParityStreamEntry())
        .mapToLong(BlockOutputStreamEntry::getCurrentPosition).sum();

    for (List<BlockOutputStreamEntry> subList : finishedStreamEntries) {
      totalLength += subList.stream()
          .filter(c -> !((ECBlockOutputStreamEntry) c).isParityStreamEntry())
          .mapToLong(BlockOutputStreamEntry::getCurrentPosition).sum();
    }
    return totalLength;
  }

  public void endECBlock() throws IOException {
    List<BlockOutputStreamEntry> entries = getStreamEntries();
    List<BlockOutputStreamEntry> finishedBlock = new ArrayList<>();
    if (entries.size() > 0) {
      for (int i = entries.size() - 1; i >= 0; i--) {
        finishedBlock.add(entries.remove(i));
      }
    }

    for (BlockOutputStreamEntry entry : finishedBlock) {
      entry.close();
    }
    finishedStreamEntries.add(finishedBlock);
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
