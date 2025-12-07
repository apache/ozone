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

package org.apache.hadoop.ozone.client.io;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.StreamBuffer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager.
 */
public class BlockDataStreamOutputEntryPool implements KeyMetadataAware {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDataStreamOutputEntryPool.class);

  private final List<BlockDataStreamOutputEntry> streamEntries;
  private final OzoneClientConfig config;
  private int currentStreamIndex;
  private final OzoneManagerProtocol omClient;
  private final OmKeyArgs keyArgs;
  private final XceiverClientFactory xceiverClientFactory;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  private final long openID;
  private final ExcludeList excludeList;
  private List<StreamBuffer> bufferList;
  private ContainerBlockID lastUpdatedBlockId = new ContainerBlockID(-1, -1);

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public BlockDataStreamOutputEntryPool(
      OzoneClientConfig config,
      OzoneManagerProtocol omClient,
      ReplicationConfig replicationConfig,
      String uploadID, int partNumber,
      boolean isMultipart, OmKeyInfo info,
      boolean unsafeByteBufferConversion,
      XceiverClientFactory xceiverClientFactory, long openID
  ) {
    this.config = config;
    this.xceiverClientFactory = xceiverClientFactory;
    streamEntries = new ArrayList<>();
    currentStreamIndex = 0;
    this.omClient = omClient;
    this.keyArgs = new OmKeyArgs.Builder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setKeyName(info.getKeyName())
        .setReplicationConfig(replicationConfig).setDataSize(info.getDataSize())
        .setIsMultipartKey(isMultipart).setMultipartUploadID(uploadID)
        .setMultipartUploadPartNumber(partNumber)
        .setSortDatanodesInPipeline(true).build();
    this.openID = openID;
    this.excludeList = createExcludeList();
    this.bufferList = new ArrayList<>();
  }

  /**
   * When a key is opened, it is possible that there are some blocks already
   * allocated to it for this open session. In this case, to make use of these
   * blocks, we need to add these blocks to stream entries. But, a key's version
   * also includes blocks from previous versions, we need to avoid adding these
   * old blocks to stream entries, because these old blocks should not be picked
   * for write. To do this, the following method checks that, only those
   * blocks created in this particular open version are added to stream entries.
   *
   * @param version the set of blocks that are pre-allocated.
   * @param openVersion the version corresponding to the pre-allocation.
   * @throws IOException
   */
  public void addPreallocateBlocks(OmKeyLocationInfoGroup version,
      long openVersion) throws IOException {
    // server may return any number of blocks, (0 to any)
    // only the blocks allocated in this open session (block createVersion
    // equals to open session version)
    for (OmKeyLocationInfo subKeyInfo : version.getLocationList(openVersion)) {
      addKeyLocationInfo(subKeyInfo);
    }
  }

  private void addKeyLocationInfo(OmKeyLocationInfo subKeyInfo) {
    Objects.requireNonNull(subKeyInfo.getPipeline(), "subKeyInfo.getPipeline() == null");
    BlockDataStreamOutputEntry.Builder builder =
        new BlockDataStreamOutputEntry.Builder()
            .setBlockID(subKeyInfo.getBlockID())
            .setKey(keyArgs.getKeyName())
            .setXceiverClientManager(xceiverClientFactory)
            .setPipeline(subKeyInfo.getPipeline())
            .setConfig(config)
            .setLength(subKeyInfo.getLength())
            .setToken(subKeyInfo.getToken())
            .setBufferList(bufferList);
    streamEntries.add(builder.build());
  }

  public List<OmKeyLocationInfo> getLocationInfoList()  {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockDataStreamOutputEntry streamEntry : streamEntries) {
      long length = streamEntry.getCurrentPosition();

      // Commit only those blocks to OzoneManager which are not empty
      if (length != 0) {
        OmKeyLocationInfo info =
            new OmKeyLocationInfo.Builder().setBlockID(streamEntry.getBlockID())
                .setLength(streamEntry.getCurrentPosition()).setOffset(0)
                .setToken(streamEntry.getToken())
                .setPipeline(streamEntry.getPipeline()).build();
        locationInfoList.add(info);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "block written " + streamEntry.getBlockID() + ", length " + length
                + " bcsID " + streamEntry.getBlockID()
                .getBlockCommitSequenceId());
      }
    }
    return locationInfoList;
  }

  void hsyncKey(long offset) throws IOException {
    if (keyArgs != null) {
      // in test, this could be null
      keyArgs.setDataSize(offset);
      keyArgs.setLocationInfoList(getLocationInfoList());
      // When the key is multipart upload part file upload, we should not
      // commit the key, as this is not an actual key, this is a just a
      // partial key of a large file.
      if (keyArgs.getIsMultipartKey()) {
        throw new IOException("Hsync is unsupported for multipart keys.");
      } else {
        if (keyArgs.getLocationInfoList().isEmpty()) {
          omClient.hsyncKey(keyArgs, openID);
        } else {
          ContainerBlockID lastBLockId = keyArgs.getLocationInfoList().get(keyArgs.getLocationInfoList().size() - 1)
              .getBlockID().getContainerBlockID();
          if (!lastUpdatedBlockId.equals(lastBLockId)) {
            omClient.hsyncKey(keyArgs, openID);
            lastUpdatedBlockId = lastBLockId;
          }
        }
      }
    } else {
      LOG.warn("Closing KeyOutputStream, but key args is null");
    }
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   */
  void discardPreallocatedBlocks(long containerID, PipelineID pipelineId) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (currentStreamIndex + 1 < streamEntries.size()) {
      ListIterator<BlockDataStreamOutputEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex + 1);
      while (streamEntryIterator.hasNext()) {
        BlockDataStreamOutputEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if ((streamEntry.getPipeline().getId().equals(pipelineId)) ||
            (containerID != -1 &&
                streamEntry.getBlockID().getContainerID() == containerID)) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  List<BlockDataStreamOutputEntry> getStreamEntries() {
    return streamEntries;
  }

  XceiverClientFactory getXceiverClientFactory() {
    return xceiverClientFactory;
  }

  String getKeyName() {
    return keyArgs.getKeyName();
  }

  long getKeyLength() {
    return streamEntries.stream().mapToLong(
        BlockDataStreamOutputEntry::getCurrentPosition).sum();
  }

  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockDataStreamOutputEntry to write.
   *
   * @throws IOException
   */
  private void allocateNewBlock() throws IOException {
    if (!excludeList.isEmpty()) {
      LOG.debug("Allocating block with {}", excludeList);
    }
    OmKeyLocationInfo subKeyInfo =
        omClient.allocateBlock(keyArgs, openID, excludeList);
    addKeyLocationInfo(subKeyInfo);
  }

  void commitKey(long offset) throws IOException {
    if (keyArgs != null) {
      // in test, this could be null
      long length = getKeyLength();
      Preconditions.checkArgument(offset == length);
      keyArgs.setDataSize(length);
      keyArgs.setLocationInfoList(getLocationInfoList());
      // When the key is multipart upload part file upload, we should not
      // commit the key, as this is not an actual key, this is a just a
      // partial key of a large file.
      if (keyArgs.getIsMultipartKey()) {
        commitUploadPartInfo =
            omClient.commitMultipartUploadPart(keyArgs, openID);
      } else {
        omClient.commitKey(keyArgs, openID);
      }
    } else {
      LOG.warn("Closing KeyDataStreamOutput, but key args is null");
    }
  }

  public BlockDataStreamOutputEntry getCurrentStreamEntry() {
    if (streamEntries.isEmpty() || streamEntries.size() <= currentStreamIndex) {
      return null;
    } else {
      return streamEntries.get(currentStreamIndex);
    }
  }

  BlockDataStreamOutputEntry allocateBlockIfNeeded() throws IOException {
    BlockDataStreamOutputEntry streamEntry = getCurrentStreamEntry();
    if (streamEntry != null && streamEntry.isClosed()) {
      // a stream entry gets closed either by :
      // a. If the stream gets full
      // b. it has encountered an exception
      currentStreamIndex++;
    }
    if (streamEntries.size() <= currentStreamIndex) {
      Objects.requireNonNull(omClient, "omClient == null");
      // allocate a new block, if a exception happens, log an error and
      // throw exception to the caller directly, and the write fails.
      allocateNewBlock();
    }
    // in theory, this condition should never violate due the check above
    // still do a sanity check.
    Preconditions.checkArgument(currentStreamIndex < streamEntries.size());
    return streamEntries.get(currentStreamIndex);
  }

  void cleanup() {
    if (excludeList != null) {
      excludeList.clear();
    }

    if (streamEntries != null) {
      streamEntries.clear();
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return commitUploadPartInfo;
  }

  public ExcludeList getExcludeList() {
    return excludeList;
  }

  boolean isEmpty() {
    return streamEntries.isEmpty();
  }

  long computeBufferData() {
    long totalDataLen = 0;
    for (StreamBuffer b : bufferList) {
      totalDataLen += b.position();
    }
    return totalDataLen;
  }

  OzoneClientConfig getConfig() {
    return config;
  }

  ExcludeList createExcludeList() {
    return new ExcludeList(getConfig().getExcludeNodesExpiryTime(),
        Clock.system(ZoneOffset.UTC));
  }

  public long getDataSize() {
    return keyArgs.getDataSize();
  }

  @Override
  public Map<String, String> getMetadata() {
    return this.keyArgs.getMetadata();
  }
}
