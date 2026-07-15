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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BlockOutputStreamEntryPool manages the communication with OM during writing
 * a Key to Ozone with {@link KeyOutputStream}.
 * Block allocation, handling of pre-allocated blocks, and managing stream
 * entries that represent a writing channel towards DataNodes are the main
 * responsibility of this class.
 */
public class BlockOutputStreamEntryPool implements KeyMetadataAware {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStreamEntryPool.class);

  /**
   * List of stream entries that are used to write a block of data.
   */
  private final List<BlockOutputStreamEntry> streamEntries = new ArrayList<>();
  private final OzoneClientConfig config;
  /**
   * The actual stream entry we are writing into. Note that a stream entry is
   * allowed to manage more streams, as for example in the EC write case, where
   * an entry represents an EC block group.
   */
  private int currentStreamIndex;
  private final OzoneManagerProtocol omClient;
  private final OmKeyArgs.Builder keyArgs;
  private final Map<String, String> metadata = new HashMap<>();
  private final XceiverClientFactory xceiverClientFactory;
  /**
   * A {@link BufferPool} shared between all
   * {@link org.apache.hadoop.hdds.scm.storage.BlockOutputStream}s managed by
   * the entries in the pool.
   */
  private final BufferPool bufferPool;
  private OmMultipartCommitUploadPartInfo commitUploadPartInfo;
  private final long openID;
  private final ExcludeList excludeList;
  private final ContainerClientMetrics clientMetrics;
  private final StreamBufferArgs streamBufferArgs;
  private final Supplier<ExecutorService> executorServiceSupplier;
  // update blocks on OM
  private ContainerBlockID lastUpdatedBlockId = new ContainerBlockID(-1, -1);

  public BlockOutputStreamEntryPool(KeyOutputStream.Builder b) {
    this.config = b.getClientConfig();
    this.xceiverClientFactory = b.getXceiverManager();
    currentStreamIndex = 0;
    this.omClient = b.getOmClient();
    final OmKeyInfo info = b.getOpenHandler().getKeyInfo();
    this.keyArgs = new OmKeyArgs.Builder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setKeyName(info.getKeyName())
        .setReplicationConfig(b.getReplicationConfig())
        .setDataSize(info.getDataSize())
        .setIsMultipartKey(b.isMultipartKey())
        .setMultipartUploadID(b.getMultipartUploadID())
        .setMultipartUploadPartNumber(b.getMultipartNumber());
    this.openID = b.getOpenHandler().getId();
    this.excludeList = createExcludeList();

    this.streamBufferArgs = b.getStreamBufferArgs();
    this.bufferPool =
        new BufferPool(streamBufferArgs.getStreamBufferSize(),
            (int) (streamBufferArgs.getStreamBufferMaxSize() / streamBufferArgs
                .getStreamBufferSize()),
            ByteStringConversion
                .createByteBufferConversion(b.isUnsafeByteBufferConversionEnabled()));
    this.clientMetrics = b.getClientMetrics();
    this.executorServiceSupplier = b.getExecutorServiceSupplier();
  }

  ExcludeList createExcludeList() {
    return new ExcludeList(getConfig().getExcludeNodesExpiryTime(),
        Clock.system(ZoneOffset.UTC));
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
   */
  public synchronized void addPreallocateBlocks(OmKeyLocationInfoGroup version, long openVersion) {
    // server may return any number of blocks, (0 to any)
    // only the blocks allocated in this open session (block createVersion
    // equals to open session version)
    for (OmKeyLocationInfo subKeyInfo : version.getLocationList(openVersion)) {
      addKeyLocationInfo(subKeyInfo, false);
    }
  }

  /**
   * Method to create a stream entry instance based on the
   * {@link OmKeyLocationInfo}.
   * If implementations require additional data to create the entry, they need
   * to get that data before starting to create entries.
   * @param subKeyInfo the {@link OmKeyLocationInfo} object that describes the
   *                   key to be written.
   * @return a BlockOutputStreamEntry instance that handles how data is written.
   */
  BlockOutputStreamEntry createStreamEntry(OmKeyLocationInfo subKeyInfo, boolean forRetry) {
    return
        new BlockOutputStreamEntry.Builder()
            .setBlockID(subKeyInfo.getBlockID())
            .setKey(getKeyName())
            .setXceiverClientManager(xceiverClientFactory)
            .setPipeline(subKeyInfo.getPipeline())
            .setConfig(config)
            .setLength(subKeyInfo.getLength())
            .setBufferPool(bufferPool)
            .setToken(subKeyInfo.getToken())
            .setClientMetrics(clientMetrics)
            .setStreamBufferArgs(streamBufferArgs)
            .setExecutorServiceSupplier(executorServiceSupplier)
            .setForRetry(forRetry)
            .build();
  }

  private synchronized void addKeyLocationInfo(OmKeyLocationInfo subKeyInfo, boolean forRetry) {
    Objects.requireNonNull(subKeyInfo.getPipeline(), "subKeyInfo.getPipeline() == null");
    streamEntries.add(createStreamEntry(subKeyInfo, forRetry));
  }

  /**
   * Returns the list of {@link OmKeyLocationInfo} object that describes to OM
   * where the blocks of the key have been written.
   * @return the location info list of written blocks.
   */
  @VisibleForTesting
  public List<OmKeyLocationInfo> getLocationInfoList() {
    List<OmKeyLocationInfo> locationInfoList;
    List<OmKeyLocationInfo> currBlocksLocationInfoList =
        getOmKeyLocationInfos(streamEntries);
    locationInfoList = currBlocksLocationInfoList;
    return locationInfoList;
  }

  private List<OmKeyLocationInfo> getOmKeyLocationInfos(
      List<BlockOutputStreamEntry> streams) {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (BlockOutputStreamEntry streamEntry : streams) {
      long length = streamEntry.getCurrentPosition();
      // Commit only those blocks to OzoneManager which are not empty
      if (length != 0) {
        OmKeyLocationInfo info =
            new OmKeyLocationInfo.Builder()
                .setBlockID(streamEntry.getBlockID())
                .setLength(streamEntry.getCurrentPosition())
                .setOffset(0)
                .setToken(streamEntry.getToken())
                .setPipeline(streamEntry.getPipeline())
                .build();
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

  /**
   * Retrieves the {@link BufferPool} instance shared between managed block
   * output stream entries.
   * @return the shared buffer pool.
   */
  public BufferPool getBufferPool() {
    return this.bufferPool;
  }

  OzoneClientConfig getConfig() {
    return config;
  }

  ContainerClientMetrics getClientMetrics() {
    return clientMetrics;
  }

  StreamBufferArgs getStreamBufferArgs() {
    return streamBufferArgs;
  }

  public Supplier<ExecutorService> getExecutorServiceSupplier() {
    return executorServiceSupplier;
  }

  /**
   * Discards the subsequent pre allocated blocks and removes the streamEntries
   * from the streamEntries list for the container which is closed.
   * @param containerID id of the closed container
   * @param pipelineId id of the associated pipeline
   */
  synchronized void discardPreallocatedBlocks(long containerID, PipelineID pipelineId) {
    // currentStreamIndex < streamEntries.size() signifies that, there are still
    // pre allocated blocks available.

    // This will be called only to discard the next subsequent unused blocks
    // in the streamEntryList.
    if (currentStreamIndex + 1 < streamEntries.size()) {
      ListIterator<BlockOutputStreamEntry> streamEntryIterator =
          streamEntries.listIterator(currentStreamIndex + 1);
      while (streamEntryIterator.hasNext()) {
        BlockOutputStreamEntry streamEntry = streamEntryIterator.next();
        Preconditions.checkArgument(streamEntry.getCurrentPosition() == 0);
        if ((streamEntry.getPipeline().getId().equals(pipelineId)) ||
            (containerID != -1 &&
                streamEntry.getBlockID().getContainerID() == containerID)) {
          streamEntryIterator.remove();
        }
      }
    }
  }

  @VisibleForTesting
  List<BlockOutputStreamEntry> getStreamEntries() {
    return streamEntries;
  }

  @VisibleForTesting
  XceiverClientFactory getXceiverClientFactory() {
    return xceiverClientFactory;
  }

  String getKeyName() {
    return keyArgs.getKeyName();
  }

  synchronized long getKeyLength() {
    return streamEntries.stream()
        .mapToLong(BlockOutputStreamEntry::getCurrentPosition).sum();
  }

  /**
   * Contact OM to get a new block. Set the new block with the index (e.g.
   * first block has index = 0, second has index = 1 etc.)
   *
   * The returned block is made to new BlockOutputStreamEntry to write.
   *
   * @throws IOException
   */
  private void allocateNewBlock(boolean forRetry) throws IOException {
    if (!excludeList.isEmpty()) {
      LOG.debug("Allocating block with {}", excludeList);
    }
    OmKeyLocationInfo subKeyInfo =
        omClient.allocateBlock(buildKeyArgs(), openID, excludeList);
    addKeyLocationInfo(subKeyInfo, forRetry);
  }

  /**
   * Commits the keys with Ozone Manager(s).
   * At the end of the write committing the key from client side lets the OM
   * know that the data has been written and to where. With this info OM can
   * register the metadata stored in OM and SCM about the key that was written.
   * @param offset the offset on which the key writer stands at the time of
   *               finishing data writes. (Has to be equal and checked against
   *               the actual length written by the stream entries.)
   * @throws IOException in case there is an I/O problem during communication.
   */
  void commitKey(long offset) throws IOException {
    if (keyArgs != null) {
      // in test, this could be null
      long length = getKeyLength();
      Preconditions.checkArgument(offset == length,
          "Expected offset: " + offset + " expected len: " + length);
      keyArgs.setDataSize(length);
      keyArgs.setLocationInfoList(getLocationInfoList());
      // When the key is multipart upload part file upload, we should not
      // commit the key, as this is not an actual key, this is a just a
      // partial key of a large file.
      if (keyArgs.getIsMultipartKey()) {
        commitUploadPartInfo =
            omClient.commitMultipartUploadPart(buildKeyArgs(), openID);
      } else {
        omClient.commitKey(buildKeyArgs(), openID);
      }
    } else {
      LOG.warn("Closing KeyOutputStream, but key args is null");
    }
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
          MetricUtil.captureLatencyNs(clientMetrics::addOMHsyncLatency,
              () -> omClient.hsyncKey(buildKeyArgs(), openID));
        } else {
          ContainerBlockID lastBLockId = keyArgs.getLocationInfoList().get(keyArgs.getLocationInfoList().size() - 1)
              .getBlockID().getContainerBlockID();
          if (!lastUpdatedBlockId.equals(lastBLockId)) {
            MetricUtil.captureLatencyNs(clientMetrics::addOMHsyncLatency,
                () -> omClient.hsyncKey(buildKeyArgs(), openID));
            lastUpdatedBlockId = lastBLockId;
          }
        }
      }
    } else {
      LOG.warn("Closing KeyOutputStream, but key args is null");
    }
  }

  BlockOutputStreamEntry getCurrentStreamEntry() {
    if (streamEntries.isEmpty() || streamEntries.size() <= currentStreamIndex) {
      return null;
    } else {
      return streamEntries.get(currentStreamIndex);
    }
  }

  /**
   * Allocates a new block with OM if the current stream is closed, and new
   * writes are to be handled.
   * @return the new current open stream to write to
   * @throws IOException if the block allocation failed.
   */
  synchronized BlockOutputStreamEntry allocateBlockIfNeeded(boolean forRetry) throws IOException {
    BlockOutputStreamEntry streamEntry = getCurrentStreamEntry();
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
      allocateNewBlock(forRetry);
    }
    // in theory, this condition should never violate due the check above
    // still do a sanity check.
    Preconditions.checkArgument(currentStreamIndex < streamEntries.size(),
        "currentStreamIndex(%s) must be < streamEntries.size(%s)", currentStreamIndex, streamEntries.size());
    return streamEntries.get(currentStreamIndex);
  }

  long computeBufferData() {
    return bufferPool.computeBufferData();
  }

  void cleanup() {
    if (excludeList != null) {
      excludeList.clear();
    }
    if (bufferPool != null) {
      bufferPool.clearBufferPool();
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

  @Override
  public Map<String, String> getMetadata() {
    return metadata;
  }

  long getDataSize() {
    return keyArgs.getDataSize();
  }

  private OmKeyArgs buildKeyArgs() {
    keyArgs.addAllMetadata(metadata);
    return keyArgs.build();
  }
}
