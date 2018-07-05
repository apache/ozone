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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CreateContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Type;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.SmallFileUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.KeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.DELETE_ON_OPEN_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.GET_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.PUT_SMALL_FILE_ERROR;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Stage;

/**
 * Handler for KeyValue Container type.
 */
public class KeyValueHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueHandler.class);

  private final ContainerType containerType;
  private final KeyManager keyManager;
  private final ChunkManager chunkManager;
  private VolumeChoosingPolicy volumeChoosingPolicy;
  private final int maxContainerSizeGB;


  public KeyValueHandler(Configuration config, ContainerSet contSet,
      VolumeSet volSet, ContainerMetrics metrics) {
    super(config, contSet, volSet, metrics);
    containerType = ContainerType.KeyValueContainer;
    keyManager = new KeyManagerImpl(config);
    chunkManager = new ChunkManagerImpl();
    // TODO: Add supoort for different volumeChoosingPolicies.
    volumeChoosingPolicy = new RoundRobinVolumeChoosingPolicy();
    maxContainerSizeGB = config.getInt(ScmConfigKeys
            .OZONE_SCM_CONTAINER_SIZE_GB, ScmConfigKeys
        .OZONE_SCM_CONTAINER_SIZE_DEFAULT);
  }

  @Override
  public ContainerCommandResponseProto handle(
      ContainerCommandRequestProto request, Container container) {

    Type cmdType = request.getCmdType();
    KeyValueContainer kvContainer = (KeyValueContainer) container;
    switch(cmdType) {
    case CreateContainer:
      return handleCreateContainer(request, kvContainer);
    case ReadContainer:
      return handleReadContainer(request, kvContainer);
    case UpdateContainer:
      return handleUpdateContainer(request, kvContainer);
    case DeleteContainer:
      return handleDeleteContainer(request, kvContainer);
    case ListContainer:
      return handleUnsupportedOp(request);
    case CloseContainer:
      return handleCloseContainer(request, kvContainer);
    case PutKey:
      return handlePutKey(request, kvContainer);
    case GetKey:
      return handleGetKey(request, kvContainer);
    case DeleteKey:
      return handleDeleteKey(request, kvContainer);
    case ListKey:
      return handleUnsupportedOp(request);
    case ReadChunk:
      return handleReadChunk(request, kvContainer);
    case DeleteChunk:
      return handleDeleteChunk(request, kvContainer);
    case WriteChunk:
      return handleWriteChunk(request, kvContainer);
    case ListChunk:
      return handleUnsupportedOp(request);
    case CompactChunk:
      return handleUnsupportedOp(request);
    case PutSmallFile:
      return handlePutSmallFile(request, kvContainer);
    case GetSmallFile:
      return handleGetSmallFile(request, kvContainer);
    }

    return null;
  }

  @VisibleForTesting
  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  @VisibleForTesting
  public KeyManager getKeyManager() {
    return this.keyManager;
  }

  /**
   * Handles Create Container Request. If successful, adds the container to
   * ContainerSet.
   */
  ContainerCommandResponseProto handleCreateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasCreateContainer()) {
      LOG.debug("Malformed Create Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }
    // Create Container request should be passed a null container as the
    // container would be created here.
    Preconditions.checkArgument(kvContainer == null);

    CreateContainerRequestProto createContainerReq =
        request.getCreateContainer();
    long containerID = createContainerReq.getContainerID();
    if (createContainerReq.hasContainerType()) {
      Preconditions.checkArgument(createContainerReq.getContainerType()
          .equals(ContainerType.KeyValueContainer));
    }

    KeyValueContainerData newContainerData = new KeyValueContainerData(
        containerID, maxContainerSizeGB);
    // TODO: Add support to add metadataList to ContainerData. Add metadata
    // to container during creation.
    KeyValueContainer newContainer = new KeyValueContainer(
        newContainerData, conf);

    try {
      newContainer.create(volumeSet, volumeChoosingPolicy, scmID);
      containerSet.addContainer(newContainer);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }

    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handles Read Container Request. Returns the ContainerData as response.
   */
  ContainerCommandResponseProto handleReadContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    if (!request.hasReadContainer()) {
      LOG.debug("Malformed Read Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    KeyValueContainerData containerData = kvContainer.getContainerData();
    return KeyValueContainerUtil.getReadContainerResponse(
        request, containerData);
  }


  /**
   * Handles Update Container Request. If successful, the container metadata
   * is updated.
   */
  ContainerCommandResponseProto handleUpdateContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasUpdateContainer()) {
      LOG.debug("Malformed Update Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
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
    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handles Delete Container Request.
   * Open containers cannot be deleted.
   * Holds writeLock on ContainerSet till the container is removed from
   * containerMap. On disk deletion of container files will happen
   * asynchornously without the lock.
   */
  ContainerCommandResponseProto handleDeleteContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteContainer()) {
      LOG.debug("Malformed Delete container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    boolean forceDelete = request.getDeleteContainer().getForceDelete();
    kvContainer.writeLock();
    try {
      // Check if container is open
      if (kvContainer.getContainerData().isOpen()) {
        kvContainer.writeUnlock();
        throw new StorageContainerException(
            "Deletion of Open Container is not allowed.",
            DELETE_ON_OPEN_CONTAINER);
      } else {
        containerSet.removeContainer(
            kvContainer.getContainerData().getContainerId());
        // Release the lock first.
        // Avoid holding write locks for disk operations
        kvContainer.writeUnlock();

        kvContainer.delete(forceDelete);
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } finally {
      if (kvContainer.hasWriteLock()) {
        kvContainer.writeUnlock();
      }
    }
    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handles Close Container Request. An open container is closed.
   */
  ContainerCommandResponseProto handleCloseContainer(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasCloseContainer()) {
      LOG.debug("Malformed Update Container request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      kvContainer.close();
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    }

    return ContainerUtils.getSuccessResponse(request);
  }

  /**
   * Handle Put Key operation. Calls KeyManager to process the request.
   */
  ContainerCommandResponseProto handlePutKey(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasPutKey()) {
      LOG.debug("Malformed Put Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      KeyData keyData = KeyData.getFromProtoBuf(
          request.getPutKey().getKeyData());
      Preconditions.checkNotNull(keyData);

      keyManager.putKey(kvContainer, keyData);
      long numBytes = keyData.getProtoBufMessage().toByteArray().length;
      metrics.incContainerBytesStats(Type.PutKey, numBytes);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Put Key failed", ex, IO_EXCEPTION),
          request);
    }

    return KeyUtils.getKeyResponseSuccess(request);
  }

  /**
   * Handle Get Key operation. Calls KeyManager to process the request.
   */
  ContainerCommandResponseProto handleGetKey(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetKey()) {
      LOG.debug("Malformed Get Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    KeyData responseData;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getGetKey().getBlockID());
      responseData = keyManager.getKey(kvContainer, blockID);
      long numBytes = responseData.getProtoBufMessage().toByteArray().length;
      metrics.incContainerBytesStats(Type.GetKey, numBytes);

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Get Key failed", ex, IO_EXCEPTION),
          request);
    }

    return KeyUtils.getKeyDataResponse(request, responseData);
  }

  /**
   * Handle Delete Key operation. Calls KeyManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteKey(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteKey()) {
      LOG.debug("Malformed Delete Key request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteKey().getBlockID());

      keyManager.deleteKey(kvContainer, blockID);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Key failed", ex, IO_EXCEPTION),
          request);
    }

    return KeyUtils.getKeyResponseSuccess(request);
  }

  /**
   * Handle Read Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleReadChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasReadChunk()) {
      LOG.debug("Malformed Read Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    ChunkInfo chunkInfo;
    byte[] data;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getReadChunk().getBlockID());
      chunkInfo = ChunkInfo.getFromProtoBuf(request.getReadChunk()
          .getChunkData());
      Preconditions.checkNotNull(chunkInfo);

      data = chunkManager.readChunk(kvContainer, blockID, chunkInfo);
      metrics.incContainerBytesStats(Type.ReadChunk, data.length);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return ChunkUtils.getReadChunkResponse(request, data, chunkInfo);
  }

  /**
   * Handle Delete Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleDeleteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasDeleteChunk()) {
      LOG.debug("Malformed Delete Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getDeleteChunk().getBlockID());
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(request.getDeleteChunk()
          .getChunkData());
      Preconditions.checkNotNull(chunkInfo);

      chunkManager.deleteChunk(kvContainer, blockID, chunkInfo);
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Delete Chunk failed", ex,
              IO_EXCEPTION), request);
    }

    return ChunkUtils.getChunkResponseSuccess(request);
  }

  /**
   * Handle Write Chunk operation. Calls ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleWriteChunk(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasWriteChunk()) {
      LOG.debug("Malformed Write Chunk request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(
          request.getWriteChunk().getBlockID());
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(request.getWriteChunk()
          .getChunkData());
      Preconditions.checkNotNull(chunkInfo);

      byte[] data = null;
      if (request.getWriteChunk().getStage() == Stage.WRITE_DATA ||
          request.getWriteChunk().getStage() == Stage.COMBINED) {
        data = request.getWriteChunk().getData().toByteArray();
      }

      chunkManager.writeChunk(kvContainer, blockID, chunkInfo, data,
          request.getWriteChunk().getStage());

      // We should increment stats after writeChunk
      if (request.getWriteChunk().getStage() == Stage.WRITE_DATA ||
          request.getWriteChunk().getStage() == Stage.COMBINED) {
        metrics.incContainerBytesStats(Type.WriteChunk, request.getWriteChunk()
            .getChunkData().getLen());
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return ChunkUtils.getChunkResponseSuccess(request);
  }

  /**
   * Handle Put Small File operation. Writes the chunk and associated key
   * using a single RPC. Calls KeyManager and ChunkManager to process the
   * request.
   */
  ContainerCommandResponseProto handlePutSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasPutSmallFile()) {
      LOG.debug("Malformed Put Small File request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }
    PutSmallFileRequestProto putSmallFileReq =
        request.getPutSmallFile();

    try {
      checkContainerOpen(kvContainer);

      BlockID blockID = BlockID.getFromProtobuf(putSmallFileReq.getKey()
          .getKeyData().getBlockID());
      KeyData keyData = KeyData.getFromProtoBuf(
          putSmallFileReq.getKey().getKeyData());
      Preconditions.checkNotNull(keyData);

      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(
          putSmallFileReq.getChunkInfo());
      Preconditions.checkNotNull(chunkInfo);

      byte[] data = putSmallFileReq.getData().toByteArray();
      chunkManager.writeChunk(
          kvContainer, blockID, chunkInfo, data, Stage.COMBINED);

      List<ContainerProtos.ChunkInfo> chunks = new LinkedList<>();
      chunks.add(chunkInfo.getProtoBufMessage());
      keyData.setChunks(chunks);
      keyManager.putKey(kvContainer, keyData);
      metrics.incContainerBytesStats(Type.PutSmallFile, data.length);

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Read Chunk failed", ex,
              PUT_SMALL_FILE_ERROR), request);
    }

    return SmallFileUtils.getPutFileResponseSuccess(request);
  }

  /**
   * Handle Get Small File operation. Gets a data stream using a key. This
   * helps in reducing the RPC overhead for small files. Calls KeyManager and
   * ChunkManager to process the request.
   */
  ContainerCommandResponseProto handleGetSmallFile(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetSmallFile()) {
      LOG.debug("Malformed Get Small File request. trace ID: {}",
          request.getTraceID());
      return ContainerUtils.malformedRequest(request);
    }

    GetSmallFileRequestProto getSmallFileReq = request.getGetSmallFile();

    try {
      BlockID blockID = BlockID.getFromProtobuf(getSmallFileReq.getKey()
          .getBlockID());
      KeyData responseData = keyManager.getKey(kvContainer, blockID);

      ContainerProtos.ChunkInfo chunkInfo = null;
      ByteString dataBuf = ByteString.EMPTY;
      for (ContainerProtos.ChunkInfo chunk : responseData.getChunks()) {
        byte[] data = chunkManager.readChunk(kvContainer, blockID,
            ChunkInfo.getFromProtoBuf(chunk));
        ByteString current = ByteString.copyFrom(data);
        dataBuf = dataBuf.concat(current);
        chunkInfo = chunk;
      }
      metrics.incContainerBytesStats(Type.GetSmallFile, dataBuf.size());
      return SmallFileUtils.getGetSmallFileResponseSuccess(request, dataBuf
          .toByteArray(), ChunkInfo.getFromProtoBuf(chunkInfo));
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
    return ContainerUtils.unsupportedRequest(request);
  }

  /**
   * Check if container is open. Throw exception otherwise.
   * @param kvContainer
   * @throws StorageContainerException
   */
  private void checkContainerOpen(KeyValueContainer kvContainer)
      throws StorageContainerException {

    ContainerProtos.ContainerLifeCycleState containerState =
        kvContainer.getContainerState();

    if (containerState == ContainerProtos.ContainerLifeCycleState.OPEN) {
      return;
    } else {
      String msg = "Requested operation not allowed as ContainerState is " +
          containerState;
      ContainerProtos.Result result = null;
      switch (containerState) {
      case CLOSING:
      case CLOSED:
        result = CLOSED_CONTAINER_IO;
        break;
      case INVALID:
        result = INVALID_CONTAINER_STATE;
        break;
      default:
        result = CONTAINER_INTERNAL_ERROR;
      }

      throw new StorageContainerException(msg, result);
    }
  }
}