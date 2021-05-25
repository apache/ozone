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

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.util.AutoCloseableLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DELETE_ON_OPEN_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.GET_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.PUT_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockDataResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockLengthResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getGetSmallFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getPutFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadContainerResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getSuccessResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.malformedRequest;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.putBlockResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.unsupportedRequest;
import static org.apache.hadoop.hdds.scm.utils.ClientCommandsUtils.getReadChunkVersion;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for KeyValue Container type.
 */
public class KeyValueHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueHandler.class);

  private final ContainerType containerType;
  private final BlockManager blockManager;
  private final ChunkManager chunkManager;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long maxContainerSize;
  private final Function<ByteBuffer, ByteString> byteBufferToByteString;

  // A lock that is held during container creation.
  private final AutoCloseableLock containerCreationLock;

  public KeyValueHandler(ConfigurationSource config, String datanodeId,
      ContainerSet contSet, VolumeSet volSet, ContainerMetrics metrics,
      Consumer<ContainerReplicaProto> icrSender) {
    super(config, datanodeId, contSet, volSet, metrics, icrSender);
    containerType = ContainerType.KeyValueContainer;
    blockManager = new BlockManagerImpl(config);
    chunkManager = ChunkManagerFactory.createChunkManager(config, blockManager,
        volSet);
    try {
      volumeChoosingPolicy = conf.getClass(
          HDDS_DATANODE_VOLUME_CHOOSING_POLICY, RoundRobinVolumeChoosingPolicy
              .class, VolumeChoosingPolicy.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    maxContainerSize = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    // this handler lock is used for synchronizing createContainer Requests,
    // so using a fair lock here.
    containerCreationLock = new AutoCloseableLock(new ReentrantLock(true));

    boolean isUnsafeByteBufferConversionEnabled =
        conf.getBoolean(
            OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
            OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED_DEFAULT);

    byteBufferToByteString =
        ByteStringConversion
            .createByteBufferConversion(isUnsafeByteBufferConversionEnabled);
  }

  @VisibleForTesting
  public VolumeChoosingPolicy getVolumeChoosingPolicyForTesting() {
    return volumeChoosingPolicy;
  }

  @Override
  public void stop() {
  }

  @Override
  public ContainerCommandResponseProto handle(
      ContainerCommandRequestProto request, Container container,
      DispatcherContext dispatcherContext) {

    return KeyValueHandler
        .dispatchRequest(this, request, (KeyValueContainer) container,
            dispatcherContext);
  }

  @VisibleForTesting
  static ContainerCommandResponseProto dispatchRequest(KeyValueHandler handler,
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {
    Type cmdType = request.getCmdType();

    switch(cmdType) {
    case CreateContainer:
      return handler.handleCreateContainer(request, kvContainer);
    case ReadContainer:
      return handler.handleReadContainer(request, kvContainer);
    case UpdateContainer:
      return handler.handleUpdateContainer(request, kvContainer);
    case DeleteContainer:
      return handler.handleDeleteContainer(request, kvContainer);
    case ListContainer:
      return handler.handleUnsupportedOp(request);
    case CloseContainer:
      return handler.handleCloseContainer(request, kvContainer);
    case PutBlock:
      return handler.handlePutBlock(request, kvContainer, dispatcherContext);
    case GetBlock:
      return handler.handleGetBlock(request, kvContainer);
    case DeleteBlock:
      return handler.handleDeleteBlock(request, kvContainer);
    case ListBlock:
      return handler.handleUnsupportedOp(request);
    case ReadChunk:
      return handler.handleReadChunk(request, kvContainer, dispatcherContext);
    case DeleteChunk:
      return handler.handleDeleteChunk(request, kvContainer);
    case WriteChunk:
      return handler.handleWriteChunk(request, kvContainer, dispatcherContext);
    case ListChunk:
      return handler.handleUnsupportedOp(request);
    case CompactChunk:
      return handler.handleUnsupportedOp(request);
    case PutSmallFile:
      return handler
          .handlePutSmallFile(request, kvContainer, dispatcherContext);
    case GetSmallFile:
      return handler.handleGetSmallFile(request, kvContainer);
    case GetCommittedBlockLength:
      return handler.handleGetCommittedBlockLength(request, kvContainer);
    default:
      return null;
    }
  }

  @VisibleForTesting
  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  @VisibleForTesting
  public BlockManager getBlockManager() {
    return this.blockManager;
  }

  /**
   * Handles Create Container Request. If successful, adds the container to
   * ContainerSet and sends an ICR to the SCM.
   */
  ContainerCommandResponseProto handleCreateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasCreateContainer()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Create Container request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }
    // Create Container request should be passed a null container as the
    // container would be created here.
    Preconditions.checkArgument(kvContainer == null);

    long containerID = request.getContainerID();

    ChunkLayOutVersion layoutVersion =
        ChunkLayOutVersion.getConfiguredVersion(conf);
    KeyValueContainerData newContainerData = new KeyValueContainerData(
        containerID, layoutVersion, maxContainerSize, request.getPipelineID(),
        getDatanodeId());
    // TODO: Add support to add metadataList to ContainerData. Add metadata
    // to container during creation.
    KeyValueContainer newContainer = new KeyValueContainer(
        newContainerData, conf);

    boolean created = false;
    try (AutoCloseableLock l = containerCreationLock.acquire()) {
      if (containerSet.getContainer(containerID) == null) {
        newContainer.create(volumeSet, volumeChoosingPolicy, clusterId);
        created = containerSet.addContainer(newContainer);
      } else {
        // The create container request for an already existing container can
        // arrive in case the ContainerStateMachine reapplies the transaction
        // on datanode restart. Just log a warning msg here.
        LOG.debug("Container already exists. container Id {}", containerID);
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }

    if (created) {
      try {
        sendICR(newContainer);
      } catch (StorageContainerException ex) {
        return ContainerUtils.logAndReturnError(LOG, ex, request);
      }
    }
    return getSuccessResponse(request);
  }

  private void populateContainerPathFields(KeyValueContainer container)
      throws IOException {
    volumeSet.readLock();
    try {
      HddsVolume containerVolume = volumeChoosingPolicy.chooseVolume(volumeSet
          .getVolumesList(), container.getContainerData().getMaxSize());
      String hddsVolumeDir = containerVolume.getHddsRootDir().toString();
      container.populatePathFields(clusterId, containerVolume, hddsVolumeDir);
    } finally {
      volumeSet.readUnlock();
    }
  }

  /**
   * Handles Read Container Request. Returns the ContainerData as response.
   */
  ContainerCommandResponseProto handleReadContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasReadContainer()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Read Container request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    KeyValueContainerData containerData = kvContainer.getContainerData();
    return getReadContainerResponse(
        request, containerData.getProtoBufMessage());
  }


  /**
   * Handles Update Container Request. If successful, the container metadata
   * is updated.
   */
  ContainerCommandResponseProto handleUpdateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasUpdateContainer()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Update Container request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    boolean forceUpdate = request.getUpdateContainer().getForceUpdate();
    List<KeyValue> keyValueList =
        request.getUpdateContainer().getMetadataList();
    Map<String, String> metadata = new HashMap<>();
    for (KeyValue keyValue : keyValueList) {
      metadata.put(keyValue.getKey(), keyValue.getValue());
    }

    try {
      if (!metadata.isEmpty()) {
        kvContainer.update(metadata, forceUpdate);
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }
    return getSuccessResponse(request);
  }

  /**
   * Handles Delete Container Request.
   * Open containers cannot be deleted.
   * Holds writeLock on ContainerSet till the container is removed from
   * containerMap. On disk deletion of container files will happen
   * asynchronously without the lock.
   */
  ContainerCommandResponseProto handleDeleteContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteContainer()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Delete container request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    boolean forceDelete = request.getDeleteContainer().getForceDelete();
    try {
      deleteInternal(kvContainer, forceDelete);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }
    return getSuccessResponse(request);
  }

  /**
   * Handles Close Container Request. An open container is closed.
   * Close Container call is idempotent.
   */
  ContainerCommandResponseProto handleCloseContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasCloseContainer()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Update Container request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }
    try {
      markContainerForClose(kvContainer);
      closeContainer(kvContainer);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Close Container failed", ex,
              IO_EXCEPTION), request);
    }

    return getSuccessResponse(request);
  }

  /**
   * Handle Put Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handlePutBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasPutBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Put Key request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    final ContainerProtos.BlockData blockDataProto;
    try {
      checkContainerOpen(kvContainer);

      ContainerProtos.BlockData data = request.getPutBlock().getBlockData();
      BlockData blockData = BlockData.getFromProtoBuf(data);
      Preconditions.checkNotNull(blockData);

      boolean incrKeyCount = false;
      if (!request.getPutBlock().hasEof() || request.getPutBlock().getEof()) {
        chunkManager.finishWriteChunks(kvContainer, blockData);
        incrKeyCount = true;
      }

      long bcsId =
          dispatcherContext == null ? 0 : dispatcherContext.getLogIndex();
      blockData.setBlockCommitSequenceId(bcsId);
      blockManager.putBlock(kvContainer, blockData, incrKeyCount);

      blockDataProto = blockData.getProtoBufMessage();

      final long numBytes = blockDataProto.getSerializedSize();
      metrics.incContainerBytesStats(Type.PutBlock, numBytes);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Put Key failed", ex, IO_EXCEPTION),
          request);
    }

    return putBlockResponseSuccess(request, blockDataProto);
  }

  /**
   * Handle Get Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleGetBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Get Key request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    ContainerProtos.BlockData responseData;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getGetBlock().getBlockID());
      checkContainerIsHealthy(kvContainer, blockID, Type.GetBlock);
      responseData = blockManager.getBlock(kvContainer, blockID)
          .getProtoBufMessage();
      final long numBytes = responseData.getSerializedSize();
      metrics.incContainerBytesStats(Type.GetBlock, numBytes);

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Get Key failed", ex, IO_EXCEPTION),
          request);
    }

    return getBlockDataResponse(request, responseData);
  }

  /**
   * Handles GetCommittedBlockLength operation.
   * Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleGetCommittedBlockLength(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasGetCommittedBlockLength()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Get Key request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    long blockLength;
    try {
      BlockID blockID = BlockID
          .getFromProtobuf(request.getGetCommittedBlockLength().getBlockID());
      checkContainerIsHealthy(kvContainer, blockID,
          Type.GetCommittedBlockLength);
      BlockUtils.verifyBCSId(kvContainer, blockID);
      blockLength = blockManager.getCommittedBlockLength(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("GetCommittedBlockLength failed", ex,
              IO_EXCEPTION), request);
    }

    return getBlockLengthResponse(request, blockLength);
  }

  /**
   * Handle Delete Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Delete Key request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteBlock().getBlockID());

      blockManager.deleteBlock(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Key failed", ex, IO_EXCEPTION),
          request);
    }

    return getBlockResponseSuccess(request);
  }

  /**
   * Handle Read Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleReadChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasReadChunk()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Read Chunk request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    ChunkBuffer data;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getReadChunk().getBlockID());
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(request.getReadChunk()
          .getChunkData());
      Preconditions.checkNotNull(chunkInfo);

      checkContainerIsHealthy(kvContainer, blockID, Type.ReadChunk);
      BlockUtils.verifyBCSId(kvContainer, blockID);
      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }

      boolean isReadChunkV0 = getReadChunkVersion(request.getReadChunk())
          .equals(ContainerProtos.ReadChunkVersion.V0);
      if (isReadChunkV0) {
        // For older clients, set ReadDataIntoSingleBuffer to true so that
        // all the data read from chunk file is returned as a single
        // ByteString. Older clients cannot process data returned as a list
        // of ByteStrings.
        chunkInfo.setReadDataIntoSingleBuffer(true);
      }

      data = chunkManager.readChunk(kvContainer, blockID, chunkInfo,
          dispatcherContext);
      metrics.incContainerBytesStats(Type.ReadChunk, chunkInfo.getLen());
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    Preconditions.checkNotNull(data, "Chunk data is null");

    return getReadChunkResponse(request, data, byteBufferToByteString);
  }

  /**
   * Throw an exception if the container is unhealthy.
   *
   * @throws StorageContainerException if the container is unhealthy.
   */
  @VisibleForTesting
  void checkContainerIsHealthy(KeyValueContainer kvContainer, BlockID blockID,
      Type cmd) {
    kvContainer.readLock();
    try {
      if (kvContainer.getContainerData().getState() == State.UNHEALTHY) {
        LOG.warn("{} request {} for UNHEALTHY container {} replica", cmd,
            blockID, kvContainer.getContainerData().getContainerID());
      }
    } finally {
      kvContainer.readUnlock();
    }
  }

  /**
   * Handle Delete Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteChunk()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Delete Chunk request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteChunk().getBlockID());
      ContainerProtos.ChunkInfo chunkInfoProto = request.getDeleteChunk()
          .getChunkData();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Preconditions.checkNotNull(chunkInfo);

      chunkManager.deleteChunk(kvContainer, blockID, chunkInfo);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Chunk failed", ex,
              IO_EXCEPTION), request);
    }

    return getSuccessResponse(request);
  }

  /**
   * Handle Write Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleWriteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasWriteChunk()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Write Chunk request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      WriteChunkRequestProto writeChunk = request.getWriteChunk();
      BlockID blockID = BlockID.getFromProtobuf(writeChunk.getBlockID());
      ContainerProtos.ChunkInfo chunkInfoProto = writeChunk.getChunkData();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Preconditions.checkNotNull(chunkInfo);

      ChunkBuffer data = null;
      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }
      WriteChunkStage stage = dispatcherContext.getStage();
      if (stage == WriteChunkStage.WRITE_DATA ||
          stage == WriteChunkStage.COMBINED) {
        data =
            ChunkBuffer.wrap(writeChunk.getData().asReadOnlyByteBufferList());
      }

      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);

      // We should increment stats after writeChunk
      if (stage == WriteChunkStage.WRITE_DATA||
          stage == WriteChunkStage.COMBINED) {
        metrics.incContainerBytesStats(Type.WriteChunk, writeChunk
            .getChunkData().getLen());
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return getSuccessResponse(request);
  }

  /**
   * Handle Put Small File operation. Writes the chunk and associated key
   * using a single RPC. Calls BlockManager and ChunkManager to process the
   * request.
   */
  ContainerCommandResponseProto handlePutSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {

    if (!request.hasPutSmallFile()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Put Small File request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    PutSmallFileRequestProto putSmallFileReq = request.getPutSmallFile();
    final ContainerProtos.BlockData blockDataProto;
    try {
      checkContainerOpen(kvContainer);

      BlockData blockData = BlockData.getFromProtoBuf(
          putSmallFileReq.getBlock().getBlockData());
      Preconditions.checkNotNull(blockData);

      ContainerProtos.ChunkInfo chunkInfoProto = putSmallFileReq.getChunkInfo();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Preconditions.checkNotNull(chunkInfo);

      ChunkBuffer data = ChunkBuffer.wrap(
          putSmallFileReq.getData().asReadOnlyByteBufferList());
      if (dispatcherContext == null) {
        dispatcherContext = new DispatcherContext.Builder().build();
      }

      BlockID blockID = blockData.getBlockID();

      // chunks will be committed as a part of handling putSmallFile
      // here. There is no need to maintain this info in openContainerBlockMap.
      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);
      chunkManager.finishWriteChunks(kvContainer, blockData);

      List<ContainerProtos.ChunkInfo> chunks = new LinkedList<>();
      chunks.add(chunkInfoProto);
      blockData.setChunks(chunks);
      blockData.setBlockCommitSequenceId(dispatcherContext.getLogIndex());

      blockManager.putBlock(kvContainer, blockData);

      blockDataProto = blockData.getProtoBufMessage();
      metrics.incContainerBytesStats(Type.PutSmallFile, chunkInfo.getLen());
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex,
              PUT_SMALL_FILE_ERROR), request);
    }

    return getPutFileResponseSuccess(request, blockDataProto);
  }

  /**
   * Handle Get Small File operation. Gets a data stream using a key. This
   * helps in reducing the RPC overhead for small files. Calls BlockManager and
   * ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleGetSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetSmallFile()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Get Small File request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    GetSmallFileRequestProto getSmallFileReq = request.getGetSmallFile();

    try {
      BlockID blockID = BlockID.getFromProtobuf(getSmallFileReq.getBlock()
          .getBlockID());
      checkContainerIsHealthy(kvContainer, blockID, Type.GetSmallFile);
      BlockData responseData = blockManager.getBlock(kvContainer, blockID);

      ContainerProtos.ChunkInfo chunkInfoProto = null;
      List<ByteString> dataBuffers = new ArrayList<>();
      DispatcherContext dispatcherContext =
          new DispatcherContext.Builder().build();
      for (ContainerProtos.ChunkInfo chunk : responseData.getChunks()) {
        // if the block is committed, all chunks must have been committed.
        // Tmp chunk files won't exist here.
        ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunk);
        boolean isReadChunkV0 = getReadChunkVersion(request.getGetSmallFile())
            .equals(ContainerProtos.ReadChunkVersion.V0);
        if (isReadChunkV0) {
          // For older clients, set ReadDataIntoSingleBuffer to true so that
          // all the data read from chunk file is returned as a single
          // ByteString. Older clients cannot process data returned as a list
          // of ByteStrings.
          chunkInfo.setReadDataIntoSingleBuffer(true);
        }
        ChunkBuffer data = chunkManager.readChunk(kvContainer, blockID,
            chunkInfo, dispatcherContext);
        dataBuffers.addAll(data.toByteStringList(byteBufferToByteString));
        chunkInfoProto = chunk;
      }
      metrics.incContainerBytesStats(Type.GetSmallFile,
          BufferUtils.getBuffersLen(dataBuffers));
      return getGetSmallFileResponseSuccess(request, dataBuffers,
          chunkInfoProto);
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex,
              GET_SMALL_FILE_ERROR), request);
    }
  }

  /**
   * Handle unsupported operation.
   */
  ContainerCommandResponseProto handleUnsupportedOp(
      ContainerCommandRequestProto request) {
    // TODO : remove all unsupported operations or handle them.
    return unsupportedRequest(request);
  }

  /**
   * Check if container is open. Throw exception otherwise.
   * @param kvContainer
   * @throws StorageContainerException
   */
  private void checkContainerOpen(KeyValueContainer kvContainer)
      throws StorageContainerException {

    final State containerState = kvContainer.getContainerState();

    /*
     * In a closing state, follower will receive transactions from leader.
     * Once the leader is put to closing state, it will reject further requests
     * from clients. Only the transactions which happened before the container
     * in the leader goes to closing state, will arrive here even the container
     * might already be in closing state here.
     */
    if (containerState == State.OPEN || containerState == State.CLOSING) {
      return;
    }

    final ContainerProtos.Result result;
    switch (containerState) {
    case QUASI_CLOSED:
      result = CLOSED_CONTAINER_IO;
      break;
    case CLOSED:
      result = CLOSED_CONTAINER_IO;
      break;
    case UNHEALTHY:
      result = CONTAINER_UNHEALTHY;
      break;
    case INVALID:
      result = INVALID_CONTAINER_STATE;
      break;
    default:
      result = CONTAINER_INTERNAL_ERROR;
    }
    String msg = "Requested operation not allowed as ContainerState is " +
        containerState;
    throw new StorageContainerException(msg, result);
  }

  @Override
  public Container importContainer(ContainerData originalContainerData,
      final InputStream rawContainerStream,
      final TarContainerPacker packer)
      throws IOException {

    KeyValueContainerData containerData =
        new KeyValueContainerData(originalContainerData);

    KeyValueContainer container = new KeyValueContainer(containerData,
        conf);

    populateContainerPathFields(container);
    container.importContainerData(rawContainerStream, packer);
    sendICR(container);
    return container;

  }

  @Override
  public void exportContainer(final Container container,
      final OutputStream outputStream,
      final TarContainerPacker packer)
      throws IOException{
    final KeyValueContainer kvc = (KeyValueContainer) container;
    kvc.exportContainerData(outputStream, packer);
  }

  @Override
  public void markContainerForClose(Container container)
      throws IOException {
    container.writeLock();
    try {
      // Move the container to CLOSING state only if it's OPEN
      if (container.getContainerState() == State.OPEN) {
        container.markContainerForClose();
        sendICR(container);
      }
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void markContainerUnhealthy(Container container)
      throws IOException {
    container.writeLock();
    try {
      if (container.getContainerState() != State.UNHEALTHY) {
        try {
          container.markContainerUnhealthy();
        } catch (IOException ex) {
          // explicitly catch IOException here since the this operation
          // will fail if the Rocksdb metadata is corrupted.
          long id = container.getContainerData().getContainerID();
          LOG.warn("Unexpected error while marking container " + id
              + " as unhealthy", ex);
        } finally {
          sendICR(container);
        }
      }
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void quasiCloseContainer(Container container)
      throws IOException {
    container.writeLock();
    try {
      final State state = container.getContainerState();
      // Quasi close call is idempotent.
      if (state == State.QUASI_CLOSED) {
        return;
      }
      // The container has to be in CLOSING state.
      if (state != State.CLOSING) {
        ContainerProtos.Result error =
            state == State.INVALID ? INVALID_CONTAINER_STATE :
                CONTAINER_INTERNAL_ERROR;
        throw new StorageContainerException(
            "Cannot quasi close container #" + container.getContainerData()
                .getContainerID() + " while in " + state + " state.", error);
      }
      container.quasiClose();
      sendICR(container);
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void closeContainer(Container container)
      throws IOException {
    container.writeLock();
    try {
      final State state = container.getContainerState();
      // Close call is idempotent.
      if (state == State.CLOSED) {
        return;
      }
      if (state == State.UNHEALTHY) {
        throw new StorageContainerException(
            "Cannot close container #" + container.getContainerData()
                .getContainerID() + " while in " + state + " state.",
            ContainerProtos.Result.CONTAINER_UNHEALTHY);
      }
      // The container has to be either in CLOSING or in QUASI_CLOSED state.
      if (state != State.CLOSING && state != State.QUASI_CLOSED) {
        ContainerProtos.Result error =
            state == State.INVALID ? INVALID_CONTAINER_STATE :
                CONTAINER_INTERNAL_ERROR;
        throw new StorageContainerException(
            "Cannot close container #" + container.getContainerData()
                .getContainerID() + " while in " + state + " state.", error);
      }
      container.close();
      sendICR(container);
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void deleteContainer(Container container, boolean force)
      throws IOException {
    deleteInternal(container, force);
  }

  @Override
  public void deleteBlock(Container container, BlockData blockData)
      throws IOException {
    chunkManager.deleteChunks(container, blockData);
    for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
      ChunkInfo info = ChunkInfo.getFromProtoBuf(chunkInfo);
      if (LOG.isDebugEnabled()) {
        LOG.debug("block {} chunk {} deleted", blockData.getBlockID(), info);
      }
    }
  }

  private void deleteInternal(Container container, boolean force)
      throws StorageContainerException {
    container.writeLock();
    try {
    // If force is false, we check container state.
      if (!force) {
        // Check if container is open
        if (container.getContainerData().isOpen()) {
          throw new StorageContainerException(
              "Deletion of Open Container is not allowed.",
              DELETE_ON_OPEN_CONTAINER);
        }
      }
      long containerId = container.getContainerData().getContainerID();
      containerSet.removeContainer(containerId);
    } finally {
      container.writeUnlock();
    }
    // Avoid holding write locks for disk operations
    container.delete();
    container.getContainerData().setState(State.DELETED);
    sendICR(container);
  }
}
