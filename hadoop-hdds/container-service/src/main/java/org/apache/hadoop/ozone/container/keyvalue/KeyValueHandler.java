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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DELETE_ON_NON_EMPTY_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DELETE_ON_OPEN_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.GET_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.PUT_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockDataResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockLengthResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getGetSmallFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getListBlockResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getPutFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadContainerResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getSuccessResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getSuccessResponseBuilder;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.malformedRequest;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.putBlockResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.unsupportedRequest;
import static org.apache.hadoop.hdds.scm.utils.ClientCommandsUtils.getReadChunkVersion;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;

import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for KeyValue Container type.
 */
public class KeyValueHandler extends Handler {

  public static final Logger LOG = LoggerFactory.getLogger(
      KeyValueHandler.class);

  private final BlockManager blockManager;
  private final ChunkManager chunkManager;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long maxContainerSize;
  private final Function<ByteBuffer, ByteString> byteBufferToByteString;
  private final boolean validateChunkChecksumData;
  // A striped lock that is held during container creation.
  private final Striped<Lock> containerCreationLocks;

  public KeyValueHandler(ConfigurationSource config,
                         String datanodeId,
                         ContainerSet contSet,
                         VolumeSet volSet,
                         ContainerMetrics metrics,
                         IncrementalReportSender<Container> icrSender) {
    super(config, datanodeId, contSet, volSet, metrics, icrSender);
    blockManager = new BlockManagerImpl(config);
    validateChunkChecksumData = conf.getObject(
        DatanodeConfiguration.class).isChunkDataValidationCheck();
    chunkManager = ChunkManagerFactory.createChunkManager(config, blockManager,
        volSet);
    try {
      volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    maxContainerSize = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    // this striped handler lock is used for synchronizing createContainer
    // Requests.
    final int threadCountPerDisk = conf.getInt(
        OzoneConfigKeys
            .HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_KEY,
        OzoneConfigKeys
            .HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_DEFAULT);
    final int numberOfDisks =
        HddsServerUtil.getDatanodeStorageDirs(conf).size();
    containerCreationLocks = Striped.lazyWeakLock(
        threadCountPerDisk * numberOfDisks);

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
  public StateMachine.DataChannel getStreamDataChannel(
      Container container, ContainerCommandRequestProto msg)
      throws StorageContainerException {
    KeyValueContainer kvContainer = (KeyValueContainer) container;
    checkContainerOpen(kvContainer);

    if (msg.hasWriteChunk()) {
      BlockID blockID =
          BlockID.getFromProtobuf(msg.getWriteChunk().getBlockID());

      return chunkManager.getStreamDataChannel(kvContainer,
          blockID, metrics);
    } else {
      throw new StorageContainerException("Malformed request.",
          ContainerProtos.Result.IO_EXCEPTION);
    }
  }

  @Override
  public void stop() {
    chunkManager.shutdown();
    blockManager.shutdown();
  }

  @Override
  public ContainerCommandResponseProto handle(
      ContainerCommandRequestProto request, Container container,
      DispatcherContext dispatcherContext) {

    try {
      return KeyValueHandler
          .dispatchRequest(this, request, (KeyValueContainer) container,
              dispatcherContext);
    } catch (RuntimeException e) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException(e, CONTAINER_INTERNAL_ERROR),
          request);
    }
  }

  @VisibleForTesting
  static ContainerCommandResponseProto dispatchRequest(KeyValueHandler handler,
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {
    Type cmdType = request.getCmdType();

    switch (cmdType) {
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
      return handler.handleListBlock(request, kvContainer);
    case ReadChunk:
      return handler.handleReadChunk(request, kvContainer, dispatcherContext);
    case DeleteChunk:
      return handler.handleDeleteChunk(request, kvContainer);
    case WriteChunk:
      return handler.handleWriteChunk(request, kvContainer, dispatcherContext);
    case StreamInit:
      return handler.handleStreamInit(request, kvContainer, dispatcherContext);
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

  ContainerCommandResponseProto handleStreamInit(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer,
      DispatcherContext dispatcherContext) {
    final BlockID blockID;
    if (request.hasWriteChunk()) {
      WriteChunkRequestProto writeChunk = request.getWriteChunk();
      blockID = BlockID.getFromProtobuf(writeChunk.getBlockID());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed {} request. trace ID: {}",
            request.getCmdType(), request.getTraceID());
      }
      return malformedRequest(request);
    }

    String path = null;
    try {
      checkContainerOpen(kvContainer);
      path = chunkManager
          .streamInit(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }

    return getSuccessResponseBuilder(request)
        .setMessage(path)
        .build();
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
    if (kvContainer != null) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException(
              "Container creation failed because " + "key value container" +
                  " already exists", null, CONTAINER_ALREADY_EXISTS), request);
    }

    long containerID = request.getContainerID();

    ContainerLayoutVersion layoutVersion =
        ContainerLayoutVersion.getConfiguredVersion(conf);
    KeyValueContainerData newContainerData = new KeyValueContainerData(
        containerID, layoutVersion, maxContainerSize, request.getPipelineID(),
        getDatanodeId());
    State state = request.getCreateContainer().getState();
    if (state != null) {
      newContainerData.setState(state);
    }
    newContainerData.setReplicaIndex(request.getCreateContainer()
        .getReplicaIndex());

    // TODO: Add support to add metadataList to ContainerData. Add metadata
    // to container during creation.
    KeyValueContainer newContainer = new KeyValueContainer(
        newContainerData, conf);

    boolean created = false;
    Lock containerIdLock = containerCreationLocks.get(containerID);
    containerIdLock.lock();
    try {
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
    } finally {
      containerIdLock.unlock();
    }

    if (created) {
      ContainerLogger.logOpen(newContainerData);
      try {
        sendICR(newContainer);
      } catch (StorageContainerException ex) {
        return ContainerUtils.logAndReturnError(LOG, ex, request);
      }
    }

    return getSuccessResponse(request);
  }

  private void populateContainerPathFields(KeyValueContainer container,
      HddsVolume hddsVolume) throws IOException {
    volumeSet.readLock();
    try {
      String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
          hddsVolume, clusterId);
      container.populatePathFields(idDir, hddsVolume);
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

      boolean endOfBlock = false;
      if (!request.getPutBlock().hasEof() || request.getPutBlock().getEof()) {
        // in EC, we will be doing empty put block.
        // So, let's flush only when there are any chunks
        if (!request.getPutBlock().getBlockData().getChunksList().isEmpty()) {
          chunkManager.finishWriteChunks(kvContainer, blockData);
        }
        endOfBlock = true;
      }

      long bcsId =
          dispatcherContext == null ? 0 : dispatcherContext.getLogIndex();
      blockData.setBlockCommitSequenceId(bcsId);
      blockManager.putBlock(kvContainer, blockData, endOfBlock);

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
   * Handle List Block operation. Calls BlockManager to process the request.
   */
  ContainerCommandResponseProto handleListBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasListBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed list block request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    List<ContainerProtos.BlockData> returnData = new ArrayList<>();
    try {
      int count = request.getListBlock().getCount();
      long startLocalId = -1;
      if (request.getListBlock().hasStartLocalID()) {
        startLocalId = request.getListBlock().getStartLocalID();
      }
      List<BlockData> responseData =
          blockManager.listBlock(kvContainer, startLocalId, count);
      for (BlockData responseDatum : responseData) {
        returnData.add(responseDatum.getProtoBufMessage());
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("List blocks failed", ex, IO_EXCEPTION),
          request);
    }

    return getListBlockResponse(request, returnData);
  }

  /**
   * Handle Delete Block operation. Calls BlockManager to process the request.
   */
  @Deprecated
  ContainerCommandResponseProto handleDeleteBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    // Block/ Chunk Deletion is handled by BlockDeletingService.
    // SCM sends Block Deletion commands directly to Datanodes and not
    // through a Pipeline.
    throw new UnsupportedOperationException("Datanode handles block deletion " +
        "using BlockDeletingService");
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

      BlockUtils.verifyBCSId(kvContainer, blockID);
      if (dispatcherContext == null) {
        dispatcherContext = DispatcherContext.getHandleReadChunk();
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
      // Validate data only if the read chunk is issued by Ratis for its
      // internal logic.
      //  For client reads, the client is expected to validate.
      if (DispatcherContext.op(dispatcherContext).readFromTmpFile()) {
        validateChunkChecksumData(data, chunkInfo);
        metrics.incBytesReadStateMachine(chunkInfo.getLen());
        metrics.incNumReadStateMachine();
      }
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
   * Handle Delete Chunk operation. Calls ChunkManager to process the request.
   */
  @Deprecated
  ContainerCommandResponseProto handleDeleteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    // Block/ Chunk Deletion is handled by BlockDeletingService.
    // SCM sends Block Deletion commands directly to Datanodes and not
    // through a Pipeline.
    throw new UnsupportedOperationException("Datanode handles chunk deletion " +
        "using BlockDeletingService");
  }

  private void validateChunkChecksumData(ChunkBuffer data, ChunkInfo info)
      throws StorageContainerException {
    if (validateChunkChecksumData) {
      try {
        Checksum.verifyChecksum(data.duplicate(data.position(), data.limit()), info.getChecksumData(), 0);
      } catch (OzoneChecksumException ex) {
        throw ChunkUtils.wrapInStorageContainerException(ex);
      }
    }
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
        dispatcherContext = DispatcherContext.getHandleWriteChunk();
      }
      final boolean isWrite = dispatcherContext.getStage().isWrite();
      if (isWrite) {
        data =
            ChunkBuffer.wrap(writeChunk.getData().asReadOnlyByteBufferList());
        validateChunkChecksumData(data, chunkInfo);
      }
      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);

      // We should increment stats after writeChunk
      if (isWrite) {
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
        dispatcherContext = DispatcherContext.getHandlePutSmallFile();
      }

      BlockID blockID = blockData.getBlockID();

      // chunks will be committed as a part of handling putSmallFile
      // here. There is no need to maintain this info in openContainerBlockMap.
      validateChunkChecksumData(data, chunkInfo);
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
      BlockData responseData = blockManager.getBlock(kvContainer, blockID);

      ContainerProtos.ChunkInfo chunkInfoProto = null;
      List<ByteString> dataBuffers = new ArrayList<>();
      final DispatcherContext dispatcherContext
          = DispatcherContext.getHandleGetSmallFile();
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
    if (containerState == State.OPEN || containerState == State.CLOSING
        || containerState == State.RECOVERING) {
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
      final TarContainerPacker packer) throws IOException {
    Preconditions.checkState(originalContainerData instanceof
        KeyValueContainerData, "Should be KeyValueContainerData instance");

    KeyValueContainerData containerData = new KeyValueContainerData(
        (KeyValueContainerData) originalContainerData);

    KeyValueContainer container = new KeyValueContainer(containerData,
        conf);

    HddsVolume targetVolume = originalContainerData.getVolume();
    populateContainerPathFields(container, targetVolume);
    container.importContainerData(rawContainerStream, packer);
    ContainerLogger.logImported(containerData);
    sendICR(container);
    return container;

  }

  @Override
  public void exportContainer(final Container container,
      final OutputStream outputStream,
      final TarContainerPacker packer)
      throws IOException {
    final KeyValueContainer kvc = (KeyValueContainer) container;
    kvc.exportContainerData(outputStream, packer);
    ContainerLogger.logExported(container.getContainerData());
  }

  @Override
  public void markContainerForClose(Container container)
      throws IOException {
    container.writeLock();
    try {
      ContainerProtos.ContainerDataProto.State state =
          container.getContainerState();
      // Move the container to CLOSING state only if it's OPEN/RECOVERING
      if (HddsUtils.isOpenToWriteState(state)) {
        if (state == RECOVERING) {
          containerSet.removeRecoveringContainer(
              container.getContainerData().getContainerID());
          ContainerLogger.logRecovered(container.getContainerData());
        }
        container.markContainerForClose();
        ContainerLogger.logClosing(container.getContainerData());
        sendICR(container);
      }
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void markContainerUnhealthy(Container container, ScanResult reason)
      throws StorageContainerException {
    container.writeLock();
    try {
      long containerID = container.getContainerData().getContainerID();
      if (container.getContainerState() == State.UNHEALTHY) {
        LOG.debug("Call to mark already unhealthy container {} as unhealthy",
            containerID);
        return;
      }
      // If the volume is unhealthy, no action is needed. The container has
      // already been discarded and SCM notified. Once a volume is failed, it
      // cannot be restored without a restart.
      HddsVolume containerVolume = container.getContainerData().getVolume();
      if (containerVolume.isFailed()) {
        LOG.debug("Ignoring unhealthy container {} detected on an " +
            "already failed volume {}", containerID, containerVolume);
        return;
      }

      try {
        container.markContainerUnhealthy();
      } catch (StorageContainerException ex) {
        LOG.warn("Unexpected error while marking container {} unhealthy",
            containerID, ex);
      } finally {
        // Even if the container file is corrupted/missing and the unhealthy
        // update fails, the unhealthy state is kept in memory and sent to
        // SCM. Write a corresponding entry to the container log as well.
        ContainerLogger.logUnhealthy(container.getContainerData(), reason);
        sendICR(container);
      }
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void quasiCloseContainer(Container container, String reason)
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
      ContainerLogger.logQuasiClosed(container.getContainerData(), reason);
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
      ContainerLogger.logClosed(container.getContainerData());
      sendICR(container);
    } finally {
      container.writeUnlock();
    }
  }

  @Override
  public void copyContainer(final Container container, Path destinationPath)
      throws IOException {
    final KeyValueContainer kvc = (KeyValueContainer) container;
    kvc.copyContainerData(destinationPath);
  }

  @Override
  public Container importContainer(ContainerData originalContainerData,
      final Path containerPath) throws IOException {
    Preconditions.checkState(originalContainerData instanceof
        KeyValueContainerData, "Should be KeyValueContainerData instance");

    KeyValueContainerData containerData = new KeyValueContainerData(
        (KeyValueContainerData) originalContainerData);

    KeyValueContainer container = new KeyValueContainer(containerData,
        conf);

    HddsVolume volume = HddsVolumeUtil.matchHddsVolume(
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
        containerPath.toString());
    if (volume == null ||
        !containerPath.startsWith(volume.getVolumeRootDir())) {
      throw new IOException("ContainerPath " + containerPath
          + " doesn't match volume " + volume);
    }
    container.populatePathFields(volume, containerPath);
    container.importContainerData(containerPath);
    return container;
  }

  @Override
  public void deleteContainer(Container container, boolean force)
      throws IOException {
    deleteInternal(container, force);
  }

  /**
   * Called by BlockDeletingService to delete all the chunks in a block
   * before proceeding to delete the block info from DB.
   */
  @Override
  public void deleteBlock(Container container, BlockData blockData)
      throws IOException {
    chunkManager.deleteChunks(container, blockData);
    if (LOG.isDebugEnabled()) {
      for (ContainerProtos.ChunkInfo chunkInfo : blockData.getChunks()) {
        ChunkInfo info = ChunkInfo.getFromProtoBuf(chunkInfo);
        LOG.debug("block {} chunk {} deleted", blockData.getBlockID(), info);
      }
    }
  }

  @Override
  public void deleteUnreferenced(Container container, long localID)
      throws IOException {
    // Since the block/chunk is already checked that is unreferenced, no
    // need to lock the container here.
    StringBuilder prefixBuilder = new StringBuilder();
    ContainerLayoutVersion layoutVersion = container.getContainerData().
        getLayoutVersion();
    long containerID = container.getContainerData().getContainerID();
    // Only supports the default chunk/block name format now
    switch (layoutVersion) {
    case FILE_PER_BLOCK:
      prefixBuilder.append(localID).append(".block");
      break;
    case FILE_PER_CHUNK:
      prefixBuilder.append(localID).append("_chunk_");
      break;
    default:
      throw new IOException("Unsupported container layout version " +
          layoutVersion + " for the container " + containerID);
    }
    String prefix = prefixBuilder.toString();
    File chunkDir = ContainerUtils.getChunkDir(container.getContainerData());
    // chunkNames here is an array of file/dir name, so if we cannot find any
    // matching one, it means the client did not write any chunk into the block.
    // Since the putBlock request may fail, we don't know if the chunk exists,
    // thus we need to check it when receiving the request to delete such blocks
    String[] chunkNames = getFilesWithPrefix(prefix, chunkDir);
    if (chunkNames.length == 0) {
      LOG.warn("Missing delete block(Container = {}, Block = {}",
          containerID, localID);
      return;
    }
    for (String name: chunkNames) {
      File file = new File(chunkDir, name);
      if (!file.isFile()) {
        continue;
      }
      FileUtil.fullyDelete(file);
      LOG.info("Deleted unreferenced chunk/block {} in container {}", name,
          containerID);
    }
  }

  private String[] getFilesWithPrefix(String prefix, File chunkDir) {
    FilenameFilter filter = (dir, name) -> name.startsWith(prefix);
    return chunkDir.list(filter);
  }

  private boolean logBlocksIfNonZero(Container container)
      throws IOException {
    boolean nonZero = false;
    try (DBHandle dbHandle
             = BlockUtils.getDB(
        (KeyValueContainerData) container.getContainerData(),
        conf)) {
      StringBuilder stringBuilder = new StringBuilder();
      try (BlockIterator<BlockData>
          blockIterator = dbHandle.getStore().
          getBlockIterator(container.getContainerData().getContainerID())) {
        while (blockIterator.hasNext()) {
          nonZero = true;
          stringBuilder.append(blockIterator.nextBlock());
          if (stringBuilder.length() > StorageUnit.KB.toBytes(32)) {
            break;
          }
        }
      }
      if (nonZero) {
        LOG.error("blocks in rocksDB on container delete: {}",
            stringBuilder.toString());
      }
    }
    return nonZero;
  }

  private boolean logBlocksFoundOnDisk(Container container) throws IOException {
    // List files left over
    File chunksPath = new
        File(container.getContainerData().getChunksPath());
    Preconditions.checkArgument(chunksPath.isDirectory());
    boolean notEmpty = false;
    try (DirectoryStream<Path> dir
             = Files.newDirectoryStream(chunksPath.toPath())) {
      StringBuilder stringBuilder = new StringBuilder();
      for (Path block : dir) {
        if (notEmpty) {
          stringBuilder.append(",");
        }
        stringBuilder.append(block);
        notEmpty = true;
        if (stringBuilder.length() > StorageUnit.KB.toBytes(16)) {
          break;
        }
      }
      if (notEmpty) {
        LOG.error("Files still part of the container on delete: {}",
            stringBuilder.toString());
      }
    }
    return notEmpty;
  }

  private void deleteInternal(Container container, boolean force)
      throws StorageContainerException {
    container.writeLock();
    try {
      if (container.getContainerData().getVolume().isFailed()) {
        // if the  volume in which the container resides fails
        // don't attempt to delete/move it. When a volume fails,
        // failedVolumeListener will pick it up and clear the container
        // from the container set.
        LOG.info("Delete container issued on containerID {} which is in a " +
                "failed volume. Skipping", container.getContainerData()
            .getContainerID());
        return;
      }
      // If force is false, we check container state.
      if (!force) {
        // Check if container is open
        if (container.getContainerData().isOpen()) {
          throw new StorageContainerException(
              "Deletion of Open Container is not allowed.",
              DELETE_ON_OPEN_CONTAINER);
        }
        // Safety check that the container is empty.
        // If the container is not empty, it should not be deleted unless the
        // container is being forcefully deleted (which happens when
        // container is unhealthy or over-replicated).
        if (container.hasBlocks()) {
          metrics.incContainerDeleteFailedNonEmpty();
          LOG.error("Received container deletion command for container {} but" +
                  " the container is not empty with blockCount {}",
              container.getContainerData().getContainerID(),
              container.getContainerData().getBlockCount());
          // blocks table for future debugging.
          // List blocks
          logBlocksIfNonZero(container);
          // Log chunks
          logBlocksFoundOnDisk(container);
          throw new StorageContainerException("Non-force deletion of " +
              "non-empty container is not allowed.",
              DELETE_ON_NON_EMPTY_CONTAINER);
        }
      } else {
        metrics.incContainersForceDelete();
      }
      if (container.getContainerData() instanceof KeyValueContainerData) {
        KeyValueContainerData keyValueContainerData =
            (KeyValueContainerData) container.getContainerData();
        HddsVolume hddsVolume = keyValueContainerData.getVolume();

        // Steps to delete
        // 1. container marked deleted
        // 2. container is removed from container set
        // 3. container db handler and content removed from db
        // 4. container moved to tmp folder
        // 5. container content deleted from tmp folder
        try {
          container.markContainerForDelete();
          long containerId = container.getContainerData().getContainerID();
          containerSet.removeContainer(containerId);
          ContainerLogger.logDeleted(container.getContainerData(), force);
          KeyValueContainerUtil.removeContainer(keyValueContainerData, conf);
        } catch (IOException ioe) {
          LOG.error("Failed to move container under " + hddsVolume
              .getDeletedContainerDir());
          String errorMsg =
              "Failed to move container" + container.getContainerData()
                  .getContainerID();
          triggerVolumeScanAndThrowException(container, errorMsg,
              CONTAINER_INTERNAL_ERROR);
        }
      }
    } catch (StorageContainerException e) {
      throw e;
    } catch (IOException e) {
      // All other IO Exceptions should be treated as if the container is not
      // empty as a defensive check.
      LOG.error("Could not determine if the container {} is empty",
          container.getContainerData().getContainerID(), e);
      String errorMsg =
          "Failed to read container dir" + container.getContainerData()
              .getContainerID();
      triggerVolumeScanAndThrowException(container, errorMsg,
          CONTAINER_INTERNAL_ERROR);
    } finally {
      container.writeUnlock();
    }
    // Avoid holding write locks for disk operations
    container.delete();
    sendICR(container);
  }

  private void triggerVolumeScanAndThrowException(Container container,
      String msg, ContainerProtos.Result result)
      throws StorageContainerException {
    // Trigger a volume scan as exception occurred.
    StorageVolumeUtil.onFailure(container.getContainerData().getVolume());
    throw new StorageContainerException(msg, result);
  }

  public static Logger getLogger() {
    return LOG;
  }

}
