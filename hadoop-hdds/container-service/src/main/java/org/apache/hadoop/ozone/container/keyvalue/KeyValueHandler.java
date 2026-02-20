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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.HddsUtils.checksumToString;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CHUNK_FILE_INCONSISTENCY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DELETE_ON_NON_EMPTY_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DELETE_ON_OPEN_CONTAINER;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.GET_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_ARGUMENT;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.PUT_SMALL_FILE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNCLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockDataResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getBlockLengthResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getEchoResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getFinalizeBlockResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getGetContainerMerkleTreeResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getGetSmallFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getListBlockResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getPutFileResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadBlockResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadChunkResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getReadContainerResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getSuccessResponse;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getSuccessResponseBuilder;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getWriteChunkResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.malformedRequest;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.putBlockResponseSuccess;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.unsupportedRequest;
import static org.apache.hadoop.hdds.scm.utils.ClientCommandsUtils.getReadChunkVersion;
import static org.apache.hadoop.hdds.utils.IOUtils.roundUp;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient.createSingleNodePipeline;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.DEFAULT_LAYOUT;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.ratis.util.Preconditions.assertSame;
import static org.apache.ratis.util.Preconditions.assertTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.io.RandomAccessFileChannel;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.ChunkBufferToByteString;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerDiffReport;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
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
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for KeyValue Container type.
 */
public class KeyValueHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueHandler.class);
  private static final int STREAMING_BYTES_PER_CHUNK = 1024 * 64;

  private final BlockManager blockManager;
  private final ChunkManager chunkManager;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long maxContainerSize;
  private final long maxDeleteLockWaitMs;
  private final Function<ByteBuffer, ByteString> byteBufferToByteString;
  private final boolean validateChunkChecksumData;
  private final int chunkSize;
  // A striped lock that is held during container creation.
  private final Striped<Lock> containerCreationLocks;
  private final ContainerChecksumTreeManager checksumManager;
  private static FaultInjector injector;
  private final Clock clock;
  private final BlockInputStreamFactoryImpl blockInputStreamFactory;

  public KeyValueHandler(ConfigurationSource config,
                         String datanodeId,
                         ContainerSet contSet,
                         VolumeSet volSet,
                         ContainerMetrics metrics,
                         IncrementalReportSender<Container> icrSender,
                         ContainerChecksumTreeManager checksumManager) {
    this(config, datanodeId, contSet, volSet, null, metrics, icrSender, Clock.systemUTC(), checksumManager);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public KeyValueHandler(ConfigurationSource config,
                         String datanodeId,
                         ContainerSet contSet,
                         VolumeSet volSet,
                         VolumeChoosingPolicy volumeChoosingPolicy,
                         ContainerMetrics metrics,
                         IncrementalReportSender<Container> icrSender,
                         Clock clock,
                         ContainerChecksumTreeManager checksumManager) {
    super(config, datanodeId, contSet, volSet, metrics, icrSender);
    this.clock = clock;
    blockManager = new BlockManagerImpl(config);
    validateChunkChecksumData = conf.getObject(
        DatanodeConfiguration.class).isChunkDataValidationCheck();
    chunkManager = ChunkManagerFactory.createChunkManager(config, blockManager,
        volSet);
    this.checksumManager = checksumManager;
    this.volumeChoosingPolicy = volumeChoosingPolicy != null ? volumeChoosingPolicy
        : VolumeChoosingPolicyFactory.getPolicy(config);

    maxContainerSize = (long) config.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    maxDeleteLockWaitMs = dnConf.getDeleteContainerTimeoutMs();
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

    blockInputStreamFactory = new BlockInputStreamFactoryImpl();
    chunkSize = (int) conf.getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);

    if (ContainerLayoutVersion.getConfiguredVersion(conf) ==
        ContainerLayoutVersion.FILE_PER_CHUNK) {
      LOG.warn("FILE_PER_CHUNK layout is not supported. Falling back to default : {}.",
          DEFAULT_LAYOUT.name());
      OzoneConfiguration.of(conf).set(ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY,
          DEFAULT_LAYOUT.name());
    }
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
      ContainerCommandRequestProto request, KeyValueContainer kvContainer, DispatcherContext dispatcherContext) {
    Type cmdType = request.getCmdType();
    // Validate the request has been made to the correct datanode with the node id matching.
    if (kvContainer != null) {
      try {
        handler.validateRequestDatanodeId(kvContainer.getContainerData().getReplicaIndex(),
            request.getDatanodeUuid());
      } catch (StorageContainerException e) {
        return ContainerUtils.logAndReturnError(LOG, e, request);
      }
    }

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
    case FinalizeBlock:
      return handler.handleFinalizeBlock(request, kvContainer);
    case Echo:
      return handler.handleEcho(request, kvContainer);
    case GetContainerChecksumInfo:
      return handler.handleGetContainerChecksumInfo(request, kvContainer);
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

  @VisibleForTesting
  public ContainerChecksumTreeManager getChecksumManager() {
    return this.checksumManager;
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

    try {
      this.validateRequestDatanodeId(request.getCreateContainer().hasReplicaIndex() ?
          request.getCreateContainer().getReplicaIndex() : null, request.getDatanodeUuid());
    } catch (StorageContainerException e) {
      return ContainerUtils.logAndReturnError(LOG, e, request);
    }

    long containerID = request.getContainerID();
    State containerState = request.getCreateContainer().getState();

    if (containerState != RECOVERING) {
      try {
        containerSet.ensureContainerNotMissing(containerID, containerState);
      } catch (StorageContainerException ex) {
        return ContainerUtils.logAndReturnError(LOG, ex, request);
      }
    }

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
        if (RECOVERING == newContainer.getContainerState()) {
          created = containerSet.addContainerByOverwriteMissingContainer(newContainer);
        } else {
          created = containerSet.addContainer(newContainer);
        }
      } else {
        // The create container request for an already existing container can
        // arrive in case the ContainerStateMachine reapplies the transaction
        // on datanode restart. Just log a warning msg here.
        LOG.debug("Container already exists. container Id {}", containerID);
      }
    } catch (StorageContainerException ex) {
      newContainerData.releaseCommitSpace();
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
      ContainerProtos.ContainerDataProto.State previousState = kvContainer.getContainerState();
      markContainerForClose(kvContainer);
      closeContainer(kvContainer);
      if (previousState == RECOVERING) {
        // trigger container scan for recovered containers, i.e., after EC reconstruction
        containerSet.scanContainer(kvContainer.getContainerData().getContainerID(), "EC Reconstruction");
      }
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
      Objects.requireNonNull(blockData, "blockData == null");

      boolean endOfBlock = false;
      if (!request.getPutBlock().hasEof() || request.getPutBlock().getEof()) {
        // There are two cases where client sends empty put block with eof.
        // (1) An EC empty file. In this case, the block/chunk file does not exist,
        //     so no need to flush/close the file.
        // (2) Ratis output stream in incremental chunk list mode may send empty put block
        //     to close the block, in which case we need to flush/close the file.
        if (!request.getPutBlock().getBlockData().getChunksList().isEmpty() ||
            blockData.getMetadata().containsKey(INCREMENTAL_CHUNK_LIST)) {
          chunkManager.finishWriteChunks(kvContainer, blockData);
        }
        endOfBlock = true;
      }

      // Note: checksum held inside blockData. But no extra checksum validation here with handlePutBlock.

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

  ContainerCommandResponseProto handleFinalizeBlock(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    ContainerCommandResponseProto responseProto = checkFaultInjector(request);
    if (responseProto != null) {
      return responseProto;
    }

    if (!request.hasFinalizeBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Finalize block request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }
    ContainerProtos.BlockData responseData;

    try {
      if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.HBASE_SUPPORT)) {
        throw new StorageContainerException("DataNode has not finalized " +
            "upgrading to a version that supports block finalization.", UNSUPPORTED_REQUEST);
      }

      checkContainerOpen(kvContainer);
      BlockID blockID = BlockID.getFromProtobuf(
          request.getFinalizeBlock().getBlockID());
      Objects.requireNonNull(blockID, "blockID == null");

      LOG.info("Finalized Block request received {} ", blockID);

      responseData = blockManager.getBlock(kvContainer, blockID)
          .getProtoBufMessage();

      chunkManager.finalizeWriteChunk(kvContainer, blockID);
      blockManager.finalizeBlock(kvContainer, blockID);
      kvContainer.getContainerData()
          .addToFinalizedBlockSet(blockID.getLocalID());

      LOG.info("Block has been finalized {} ", blockID);

    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException(
              "Finalize Block failed", ex, IO_EXCEPTION), request);
    }
    return getFinalizeBlockResponse(request, responseData);
  }

  ContainerCommandResponseProto handleEcho(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {
    return getEchoResponse(request);
  }

  ContainerCommandResponseProto handleGetContainerChecksumInfo(
      ContainerCommandRequestProto request, KeyValueContainer kvContainer) {

    if (!request.hasGetContainerChecksumInfo()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Read Container Merkle tree request. trace ID: {}",
            request.getTraceID());
      }
      return malformedRequest(request);
    }

    // A container must have moved past the CLOSING
    KeyValueContainerData containerData = kvContainer.getContainerData();
    State state = containerData.getState();
    boolean stateSupportsChecksumInfo = (state == CLOSED || state == QUASI_CLOSED || state == UNHEALTHY);
    if (!stateSupportsChecksumInfo) {
      return ContainerCommandResponseProto.newBuilder()
          .setCmdType(request.getCmdType())
          .setTraceID(request.getTraceID())
          .setResult(UNCLOSED_CONTAINER_IO)
          .setMessage("Checksum information is not available for containers in state " + state)
          .build();
    }

    ByteString checksumTree = null;
    try {
      checksumTree = checksumManager.getContainerChecksumInfo(containerData);
    } catch (IOException ex) {
      // Only build from metadata if the file doesn't exist
      if (ex instanceof FileNotFoundException) {
        try {
          LOG.info("Checksum tree file not found for container {}. Building merkle tree from container metadata.",
              containerData.getContainerID());
          ContainerProtos.ContainerChecksumInfo checksumInfo = updateAndGetContainerChecksumFromMetadata(kvContainer);
          checksumTree = checksumInfo.toByteString();
        } catch (IOException metadataEx) {
          LOG.error("Failed to build merkle tree from metadata for container {}",
              containerData.getContainerID(), metadataEx);
          return ContainerCommandResponseProto.newBuilder()
              .setCmdType(request.getCmdType())
              .setTraceID(request.getTraceID())
              .setResult(IO_EXCEPTION)
              .setMessage("Failed to get or build merkle tree: " + metadataEx.getMessage())
              .build();
        }
      } else {
        // For other inability to read the file, return an error to the client.
        LOG.error("Error occurred when reading checksum file for container {}", containerData.getContainerID(), ex);
        return ContainerCommandResponseProto.newBuilder()
            .setCmdType(request.getCmdType())
            .setTraceID(request.getTraceID())
            .setResult(IO_EXCEPTION)
            .setMessage(ex.getMessage())
            .build();
      }
    }

    return getGetContainerMerkleTreeResponse(request, checksumTree);
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
      BlockUtils.verifyReplicaIdx(kvContainer, blockID);
      responseData = blockManager.getBlock(kvContainer, blockID).getProtoBufMessage();
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

    ContainerCommandResponseProto responseProto = checkFaultInjector(request);
    if (responseProto != null) {
      return responseProto;
    }

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

    final ChunkBufferToByteString data;
    try {
      BlockID blockID = BlockID.getFromProtobuf(
          request.getReadChunk().getBlockID());
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(request.getReadChunk()
          .getChunkData());
      Objects.requireNonNull(chunkInfo, "chunkInfo == null");
      BlockUtils.verifyReplicaIdx(kvContainer, blockID);
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
      LOG.debug("read chunk from block {} chunk {}", blockID, chunkInfo);
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

    Objects.requireNonNull(data, "data == null");
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

  private void validateChunkChecksumData(ChunkBufferToByteString data, ChunkInfo info)
      throws StorageContainerException {
    if (validateChunkChecksumData) {
      try {
        if (data instanceof ChunkBuffer) {
          final ChunkBuffer b = (ChunkBuffer)data;
          Checksum.verifyChecksum(b.duplicate(b.position(), b.limit()), info.getChecksumData(), 0);
        } else {
          Checksum.verifyChecksum(data.toByteString(byteBufferToByteString).asReadOnlyByteBuffer(),
              info.getChecksumData(), 0);
        }
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

    ContainerProtos.BlockData blockDataProto = null;
    HddsVolume volume = kvContainer.getContainerData().getVolume();
    long bytesToWrite = 0;
    boolean spaceReserved = false;
    boolean writeChunkSucceeded = false;
    
    try {
      checkContainerOpen(kvContainer);

      WriteChunkRequestProto writeChunk = request.getWriteChunk();
      BlockID blockID = BlockID.getFromProtobuf(writeChunk.getBlockID());
      ContainerProtos.ChunkInfo chunkInfoProto = writeChunk.getChunkData();

      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Objects.requireNonNull(chunkInfo,  "chunkInfo == null");

      ChunkBuffer data = null;
      if (dispatcherContext == null) {
        dispatcherContext = DispatcherContext.getHandleWriteChunk();
      }
      final boolean isWrite = dispatcherContext.getStage().isWrite();
      if (isWrite) {
        data =
            ChunkBuffer.wrap(writeChunk.getData().asReadOnlyByteBufferList());
        // TODO: Can improve checksum validation here. Make this one-shot after protocol change.
        validateChunkChecksumData(data, chunkInfo);
        bytesToWrite = chunkInfo.getLen();
        
        // Reserve space before writing
        if (volume != null && bytesToWrite > 0) {
          volume.reserveSpaceForWrite(bytesToWrite);
          spaceReserved = true;
        }
      }
      chunkManager
          .writeChunk(kvContainer, blockID, chunkInfo, data, dispatcherContext);
      writeChunkSucceeded = true;

      final boolean isCommit = dispatcherContext.getStage().isCommit();
      if (isCommit && writeChunk.hasBlock()) {
        long startTime = Time.monotonicNowNanos();
        metrics.incContainerOpsMetrics(Type.PutBlock);
        BlockData blockData = BlockData.getFromProtoBuf(
            writeChunk.getBlock().getBlockData());
        // optimization for hsync when WriteChunk is in commit phase:
        //
        // block metadata is piggybacked in the same message.
        // there will not be an additional PutBlock request.
        //
        // do not do this in WRITE_DATA phase otherwise PutBlock will be out
        // of order.
        blockData.setBlockCommitSequenceId(dispatcherContext.getLogIndex());
        boolean eob = writeChunk.getBlock().getEof();
        if (eob) {
          chunkManager.finishWriteChunks(kvContainer, blockData);
        }
        blockManager.putBlock(kvContainer, blockData, eob);
        blockDataProto = blockData.getProtoBufMessage();
        final long numBytes = blockDataProto.getSerializedSize();
        metrics.incContainerBytesStats(Type.PutBlock, numBytes);
        metrics.incContainerOpsLatencies(Type.PutBlock, Time.monotonicNowNanos() - startTime);
      }

      if (spaceReserved) {
        commitSpaceReservedForWrite(volume, bytesToWrite);
      }
      // We should increment stats after writeChunk
      if (isWrite) {
        metrics.incContainerBytesStats(Type.WriteChunk, writeChunk
            .getChunkData().getLen());
      }
    } catch (StorageContainerException ex) {
      releaseSpaceReservedForWrite(volume, spaceReserved, writeChunkSucceeded, bytesToWrite, kvContainer);
      return ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ex) {
      releaseSpaceReservedForWrite(volume, spaceReserved, writeChunkSucceeded, bytesToWrite, kvContainer);
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Write Chunk failed", ex, IO_EXCEPTION),
          request);
    }

    return getWriteChunkResponseSuccess(request, blockDataProto);
  }

  /**
   * Commit space reserved for write to usedSpace when write operation succeeds.
   */
  private void commitSpaceReservedForWrite(HddsVolume volume, long bytes) {
    volume.incrementUsedSpace(bytes);
    volume.releaseReservedSpaceForWrite(bytes);
  }

  /**
   * Release space reserved for write when write operation fails.
   * Also restores committedBytes if it was decremented during write.
   */
  private void releaseSpaceReservedForWrite(HddsVolume volume, boolean spaceReserved,
      boolean writeChunkSucceeded, long bytes, KeyValueContainer kvContainer) {
    if (spaceReserved) {
      volume.releaseReservedSpaceForWrite(bytes);
      // Only restore committedBytes if write chunk succeeded
      if (writeChunkSucceeded) {
        kvContainer.getContainerData().restoreCommittedBytesOnWriteFailure(bytes);
      }
    }
  }

  /**
   * Handle Write Chunk operation for closed container. Calls ChunkManager to process the request.
   */
  public void writeChunkForClosedContainer(ChunkInfo chunkInfo, BlockID blockID,
                                           ChunkBuffer data, KeyValueContainer kvContainer)
      throws IOException {
    Objects.requireNonNull(kvContainer, "kvContainer == null");
    Objects.requireNonNull(chunkInfo, "chunkInfo == null");
    Objects.requireNonNull(data, "data == null");
    long writeChunkStartTime = Time.monotonicNowNanos();
    if (!checkContainerClose(kvContainer)) {
      throw new IOException("Container #" + kvContainer.getContainerData().getContainerID() +
          " is not in closed state, Container state is " + kvContainer.getContainerState());
    }

    DispatcherContext dispatcherContext = DispatcherContext.getHandleWriteChunk();
    chunkManager.writeChunk(kvContainer, blockID, chunkInfo, data,
        dispatcherContext);

    // Increment write stats for WriteChunk after write.
    metrics.incClosedContainerBytesStats(Type.WriteChunk, chunkInfo.getLen());
    metrics.incContainerOpsLatencies(Type.WriteChunk, Time.monotonicNowNanos() - writeChunkStartTime);
  }

  /**
   * Handle Put Block operation for closed container. Calls BlockManager to process the request.
   * This is primarily used by container reconciliation process to persist the block data for closed container.
   * @param kvContainer           - Container for which block data need to be persisted.
   * @param blockData             - Block Data to be persisted (BlockData should have the chunks).
   * @param blockCommitSequenceId - Block Commit Sequence ID for the block.
   * @param overwriteBscId        - To overwrite bcsId in the block data and container. In case of chunk failure
   *                              during reconciliation, we do not want to overwrite the bcsId as this block/container
   *                              is incomplete in its current state.
   */
  public void putBlockForClosedContainer(KeyValueContainer kvContainer, BlockData blockData,
                                         long blockCommitSequenceId, boolean overwriteBscId)
      throws IOException {
    Objects.requireNonNull(kvContainer, "kvContainer == null");
    Objects.requireNonNull(blockData, "blockData == null");
    long startTime = Time.monotonicNowNanos();

    if (!checkContainerClose(kvContainer)) {
      throw new IOException("Container #" + kvContainer.getContainerData().getContainerID() +
          " is not in closed state, Container state is " + kvContainer.getContainerState());
    }
    // To be set from the Replica's BCSId
    if (overwriteBscId) {
      blockData.setBlockCommitSequenceId(blockCommitSequenceId);
    }

    blockManager.putBlockForClosedContainer(kvContainer, blockData, overwriteBscId);
    ContainerProtos.BlockData blockDataProto = blockData.getProtoBufMessage();
    final long numBytes = blockDataProto.getSerializedSize();
    // Increment write stats for PutBlock after write.
    metrics.incClosedContainerBytesStats(Type.PutBlock, numBytes);
    metrics.incContainerOpsLatencies(Type.PutBlock, Time.monotonicNowNanos() - startTime);
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
      Objects.requireNonNull(blockData, "blockData == null");

      ContainerProtos.ChunkInfo chunkInfoProto = putSmallFileReq.getChunkInfo();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
      Objects.requireNonNull(chunkInfo, "chunkInfo == null");

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
        final ChunkBufferToByteString data = chunkManager.readChunk(
            kvContainer, blockID, chunkInfo, dispatcherContext);
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
        || containerState == RECOVERING) {
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

  /**
   * Check if container is Closed.
   * @param kvContainer
   */
  private boolean checkContainerClose(KeyValueContainer kvContainer) {

    final State containerState = kvContainer.getContainerState();
    if (containerState == State.QUASI_CLOSED || containerState == State.CLOSED || containerState == State.UNHEALTHY) {
      return true;
    }
    return false;
  }

  @Override
  public Container importContainer(ContainerData originalContainerData,
      final InputStream rawContainerStream,
      final TarContainerPacker packer) throws IOException {
    KeyValueContainer container = createNewContainer(originalContainerData);

    HddsVolume targetVolume = originalContainerData.getVolume();
    populateContainerPathFields(container, targetVolume);
    container.importContainerData(rawContainerStream, packer);
    ContainerLogger.logImported(container.getContainerData());
    sendICR(container);
    return container;
  }

  @Override
  public Container importContainer(ContainerData targetTempContainerData) throws IOException {
    KeyValueContainer container = createNewContainer(targetTempContainerData);
    HddsVolume targetVolume = targetTempContainerData.getVolume();
    populateContainerPathFields(container, targetVolume);
    container.importContainerData((KeyValueContainerData) targetTempContainerData);
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
    boolean stateChanged = false;
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
        stateChanged = true;
      }
    } finally {
      container.writeUnlock();
    }
    ContainerLogger.logClosing(container.getContainerData());
    if (stateChanged) {
      sendICR(container);
    } else {
      sendDeferredICR(container);
    }
  }

  @Override
  public void updateContainerChecksum(Container container, ContainerMerkleTreeWriter treeWriter)
      throws IOException {
    updateAndGetContainerChecksum(container, treeWriter, true);
  }

  /**
   * Write the merkle tree for this container using the existing checksum metadata only. The data is not read or
   * validated by this method, so it is expected to run quickly.
   * <p>
   * If a data checksum for the container already exists, this method does nothing. The existing value would have either
   * been made from the metadata or data itself so there is no need to recreate it from the metadata. This method
   * does not send an ICR with the updated checksum info.
   * <p>
   *
   * @param container The container which will have a tree generated.
   */
  private void updateContainerChecksumFromMetadataIfNeeded(Container container) {
    if (!container.getContainerData().needsDataChecksum()) {
      return;
    }

    try {
      KeyValueContainer keyValueContainer = (KeyValueContainer) container;
      updateAndGetContainerChecksumFromMetadata(keyValueContainer);
    } catch (IOException ex) {
      LOG.error("Cannot create container checksum for container {} , Exception: ",
          container.getContainerData().getContainerID(), ex);
    }
  }

  /**
   * Updates the container merkle tree based on the RocksDb's block metadata and returns the updated checksum info.
   * This method does not send an ICR with the updated checksum info.
   * @param container - Container for which the container merkle tree needs to be updated.
   */
  @VisibleForTesting
  public ContainerProtos.ContainerChecksumInfo updateAndGetContainerChecksumFromMetadata(
      KeyValueContainer container) throws IOException {
    ContainerMerkleTreeWriter merkleTree = new ContainerMerkleTreeWriter();
    try (DBHandle dbHandle = BlockUtils.getDB(container.getContainerData(), conf);
         BlockIterator<BlockData> blockIterator = dbHandle.getStore().
             getBlockIterator(container.getContainerData().getContainerID())) {
      while (blockIterator.hasNext()) {
        BlockData blockData = blockIterator.nextBlock();
        merkleTree.addBlock(blockData.getLocalID());
        // Assume all chunks are healthy when building the tree from metadata. Scanner will identify corruption when
        // it runs after.
        List<ContainerProtos.ChunkInfo> chunkInfos = blockData.getChunks();
        merkleTree.addChunks(blockData.getLocalID(), true, chunkInfos);
      }
    }
    return updateAndGetContainerChecksum(container, merkleTree, false);
  }

  private ContainerProtos.ContainerChecksumInfo updateAndGetContainerChecksum(Container container,
      ContainerMerkleTreeWriter treeWriter, boolean sendICR) throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();

    // Attempt to write the new data checksum to disk. If persisting this fails, keep using the original data
    // checksum to prevent divergence from what SCM sees in the ICR vs what datanode peers will see when pulling the
    // merkle tree.
    long originalDataChecksum = containerData.getDataChecksum();
    boolean hadDataChecksum = !containerData.needsDataChecksum();
    ContainerProtos.ContainerChecksumInfo updateChecksumInfo = checksumManager.updateTree(containerData, treeWriter);
    long updatedDataChecksum = updateChecksumInfo.getContainerMerkleTree().getDataChecksum();

    if (updatedDataChecksum != originalDataChecksum) {
      containerData.setDataChecksum(updatedDataChecksum);
      try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
        // This value is only used during the datanode startup. If the update fails, then it's okay as the merkle tree
        // and in-memory checksum will still be the same. This will be updated the next time we update the tree.
        // Either scanner or reconciliation will update the checksum.
        dbHandle.getStore().getMetadataTable().put(containerData.getContainerDataChecksumKey(), updatedDataChecksum);
      } catch (IOException e) {
        LOG.error("Failed to update container data checksum in RocksDB for container {}. " +
                "Leaving the original checksum in RocksDB: {}", containerData.getContainerID(),
            checksumToString(originalDataChecksum), e);
      }

      if (sendICR) {
        sendICR(container);
      }

      String message = "Container " + containerData.getContainerID() +  " data checksum updated from " +
          checksumToString(originalDataChecksum) + " to " + checksumToString(updatedDataChecksum);
      if (hadDataChecksum) {
        LOG.warn(message);
        ContainerLogger.logChecksumUpdated(containerData, originalDataChecksum);
      } else {
        // If this is the first time the checksum is being generated, don't log a warning about updating the checksum.
        LOG.debug(message);
      }
    }
    return updateChecksumInfo;
  }

  @Override
  public void markContainerUnhealthy(Container container, ScanResult reason)
      throws IOException {
    container.writeLock();
    long containerID = container.getContainerData().getContainerID();
    try {
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
      container.markContainerUnhealthy();
    } catch (StorageContainerException ex) {
      LOG.warn("Unexpected error while marking container {} unhealthy",
          containerID, ex);
    } finally {
      container.writeUnlock();
    }
    updateContainerChecksumFromMetadataIfNeeded(container);
    // Even if the container file is corrupted/missing and the unhealthy
    // update fails, the unhealthy state is kept in memory and sent to
    // SCM. Write a corresponding entry to the container log as well.
    ContainerLogger.logUnhealthy(container.getContainerData(), reason);
    sendICR(container);
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
    } finally {
      container.writeUnlock();
    }
    updateContainerChecksumFromMetadataIfNeeded(container);
    ContainerLogger.logQuasiClosed(container.getContainerData(), reason);
    sendICR(container);
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
    } finally {
      container.writeUnlock();
    }
    updateContainerChecksumFromMetadataIfNeeded(container);
    ContainerLogger.logClosed(container.getContainerData());
    sendICR(container);
  }

  @Override
  public void copyContainer(final Container container, Path destinationPath)
      throws IOException {
    final KeyValueContainer kvc = (KeyValueContainer) container;
    kvc.copyContainerDirectory(destinationPath);
  }

  private KeyValueContainer createNewContainer(
      ContainerData originalContainerData) {
    Preconditions.checkState(originalContainerData instanceof
        KeyValueContainerData, "Should be KeyValueContainerData instance");

    KeyValueContainerData containerData = new KeyValueContainerData(
        (KeyValueContainerData) originalContainerData);

    return new KeyValueContainer(containerData, conf);
  }

  @Override
  public void deleteContainer(Container container, boolean force)
      throws IOException {
    deleteInternal(container, force);
  }

  @Override
  public void reconcileContainer(DNContainerOperationClient dnClient, Container<?> container,
      Collection<DatanodeDetails> peers) throws IOException {
    long containerID = container.getContainerData().getContainerID();
    try {
      reconcileContainerInternal(dnClient, container, peers);
    } finally {
      // Trigger on demand scanner after reconciliation
      containerSet.scanContainerWithoutGap(containerID,
          "Container reconciliation");
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  private void reconcileContainerInternal(DNContainerOperationClient dnClient, Container<?> container,
      Collection<DatanodeDetails> peers) throws IOException {
    KeyValueContainer kvContainer = (KeyValueContainer) container;
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    long containerID = containerData.getContainerID();

    // Obtain the original checksum info before reconciling with any peers.
    ContainerProtos.ContainerChecksumInfo originalChecksumInfo = checksumManager.read(containerData);
    if (!ContainerChecksumTreeManager.hasDataChecksum(originalChecksumInfo)) {
      // Try creating the merkle tree from RocksDB metadata if it is not present.
      originalChecksumInfo = updateAndGetContainerChecksumFromMetadata(kvContainer);
    }
    // This holds our current most up-to-date checksum info that we are using for the container.
    ContainerProtos.ContainerChecksumInfo latestChecksumInfo = originalChecksumInfo;

    int successfulPeerCount = 0;
    Set<Long> allBlocksUpdated = new HashSet<>();
    ByteBuffer chunkByteBuffer = ByteBuffer.allocate(chunkSize);

    for (DatanodeDetails peer : peers) {
      try {
        long numMissingBlocksRepaired = 0;
        long numCorruptChunksRepaired = 0;
        long numMissingChunksRepaired = 0;
        long numDivergedDeletedBlocksUpdated = 0;

        LOG.info("Beginning reconciliation for container {} with peer {}. Current data checksum is {}",
            containerID, peer, checksumToString(ContainerChecksumTreeManager.getDataChecksum(latestChecksumInfo)));
        // Data checksum updated after each peer reconciles.
        long start = Instant.now().toEpochMilli();
        ContainerProtos.ContainerChecksumInfo peerChecksumInfo;


        // Data checksum updated after each peer reconciles.
        peerChecksumInfo = dnClient.getContainerChecksumInfo(containerID, peer);
        if (peerChecksumInfo == null) {
          LOG.warn("Cannot reconcile container {} with peer {} which has not yet generated a checksum",
              containerID, peer);
          continue;
        }

        // This tree writer is initialized with our current persisted tree, then updated with the modifications done
        // while reconciling with the peer. Once we finish reconciling with this peer, we will write the updated version
        // back to the disk and pick it up as the starting point for reconciling with the next peer.
        ContainerMerkleTreeWriter updatedTreeWriter =
            new ContainerMerkleTreeWriter(latestChecksumInfo.getContainerMerkleTree());
        ContainerDiffReport diffReport = checksumManager.diff(latestChecksumInfo, peerChecksumInfo);
        Pipeline pipeline = createSingleNodePipeline(peer);

        // Handle missing blocks
        for (ContainerProtos.BlockMerkleTree missingBlock : diffReport.getMissingBlocks()) {
          try {
            long localID = missingBlock.getBlockID();
            BlockID blockID = new BlockID(containerID, localID);
            if (getBlockManager().blockExists(container, blockID)) {
              LOG.warn("Cannot reconcile block {} in container {} which was previously reported missing but is now " +
                  "present. Our container merkle tree is stale.", localID, containerID);
            } else {
              long chunksInBlockRetrieved = reconcileChunksPerBlock(kvContainer, pipeline, dnClient, localID,
                  missingBlock.getChunkMerkleTreeList(), updatedTreeWriter, chunkByteBuffer);
              if (chunksInBlockRetrieved != 0) {
                allBlocksUpdated.add(localID);
                numMissingBlocksRepaired++;
              }
            }
          } catch (IOException e) {
            LOG.error("Error while reconciling missing block for block {} in container {}", missingBlock.getBlockID(),
                  containerID, e);
          }
        }

        // Handle missing chunks
        for (Map.Entry<Long, List<ContainerProtos.ChunkMerkleTree>> entry : diffReport.getMissingChunks().entrySet()) {
          long localID = entry.getKey();
          try {
            long missingChunksRepaired = reconcileChunksPerBlock(kvContainer, pipeline, dnClient, entry.getKey(),
                entry.getValue(), updatedTreeWriter, chunkByteBuffer);
            if (missingChunksRepaired != 0) {
              allBlocksUpdated.add(localID);
              numMissingChunksRepaired += missingChunksRepaired;
            }
          } catch (IOException e) {
            LOG.error("Error while reconciling missing chunk for block {} in container {}", entry.getKey(),
                containerID, e);
          }
        }

        // Handle corrupt chunks
        for (Map.Entry<Long, List<ContainerProtos.ChunkMerkleTree>> entry : diffReport.getCorruptChunks().entrySet()) {
          long localID = entry.getKey();
          try {
            long corruptChunksRepaired = reconcileChunksPerBlock(kvContainer, pipeline, dnClient, entry.getKey(),
                entry.getValue(), updatedTreeWriter, chunkByteBuffer);
            if (corruptChunksRepaired != 0) {
              allBlocksUpdated.add(localID);
              numCorruptChunksRepaired += corruptChunksRepaired;
            }
          } catch (IOException e) {
            LOG.error("Error while reconciling corrupt chunk for block {} in container {}", entry.getKey(),
                containerID, e);
          }
        }

        // Merge block deletes from the peer that do not match our list of deleted blocks.
        for (ContainerDiffReport.DeletedBlock deletedBlock : diffReport.getDivergedDeletedBlocks()) {
          updatedTreeWriter.setDeletedBlock(deletedBlock.getBlockID(), deletedBlock.getDataChecksum());
          numDivergedDeletedBlocksUpdated++;
        }

        // Based on repaired done with this peer, write the updated merkle tree to the container.
        // This updated tree will be used when we reconcile with the next peer.
        ContainerProtos.ContainerChecksumInfo previousChecksumInfo = latestChecksumInfo;
        latestChecksumInfo = updateAndGetContainerChecksum(container, updatedTreeWriter, false);

        // Log the results of reconciliation with this peer.
        long duration = Instant.now().toEpochMilli() - start;
        long previousDataChecksum = ContainerChecksumTreeManager.getDataChecksum(previousChecksumInfo);
        long latestDataChecksum = ContainerChecksumTreeManager.getDataChecksum(latestChecksumInfo);
        if (previousDataChecksum == latestDataChecksum) {
          if (numCorruptChunksRepaired != 0 ||
              numMissingBlocksRepaired != 0 ||
              numMissingChunksRepaired != 0 ||
              numDivergedDeletedBlocksUpdated != 0) {
            // This condition should never happen.
            LOG.error("Checksum of container was not updated but blocks were repaired.");
          }
          LOG.info("Container {} reconciled with peer {}. Data checksum {} was not updated. Time taken: {} ms",
              containerID, peer, checksumToString(previousDataChecksum), duration);
        } else {
          LOG.warn("Container {} reconciled with peer {}. Data checksum updated from {} to {}" +
                  ".\nMissing blocks repaired: {}/{}\n" +
                  "Missing chunks repaired: {}/{}\n" +
                  "Corrupt chunks repaired:  {}/{}\n" +
                  "Diverged deleted blocks updated:  {}/{}\n" +
                  "Time taken: {} ms",
              containerID, peer, checksumToString(previousDataChecksum), checksumToString(latestDataChecksum),
              numMissingBlocksRepaired, diffReport.getNumMissingBlocks(),
              numMissingChunksRepaired, diffReport.getNumMissingChunks(),
              numCorruptChunksRepaired, diffReport.getNumCorruptChunks(),
              numDivergedDeletedBlocksUpdated, diffReport.getNumdivergedDeletedBlocks(),
              duration);
        }

        ContainerLogger.logReconciled(container.getContainerData(), previousDataChecksum, peer);
        successfulPeerCount++;
      } catch (IOException ex) {
        LOG.error("Failed to reconcile with peer {} for container #{}. Skipping to next peer.",
            peer, containerID, ex);
      }
    }

    // Log a summary after reconciling with all peers.
    long originalDataChecksum = ContainerChecksumTreeManager.getDataChecksum(originalChecksumInfo);
    long latestDataChecksum = ContainerChecksumTreeManager.getDataChecksum(latestChecksumInfo);
    if (originalDataChecksum == latestDataChecksum) {
      LOG.info("Completed reconciliation for container {} with {}/{} peers. Original data checksum {} was not updated",
          containerID, successfulPeerCount, peers.size(), checksumToString(latestDataChecksum));
    } else {
      LOG.warn("Completed reconciliation for container {} with {}/{} peers. {} blocks were updated. Data checksum " +
              "updated from {} to {}", containerID, successfulPeerCount, peers.size(), allBlocksUpdated.size(),
          checksumToString(originalDataChecksum), checksumToString(latestDataChecksum));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Blocks updated in container {} after reconciling with {} peers: {}", containerID,
            successfulPeerCount, allBlocksUpdated);
      }
    }

    // Trigger on demand scanner, which will build the merkle tree based on the newly ingested data.
    containerSet.scanContainerWithoutGap(containerID, "Container reconciliation");
    sendICR(container);
  }

  /**
   * Read chunks from a peer datanode and use them to repair our container.
   *
   * We will keep pulling chunks from the peer unless the requested chunk's offset would leave a hole if written past
   * the end of our current block file. Since we currently don't support leaving holes in block files, reconciliation
   * for this block will be stopped at this point and whatever data we have pulled will be committed.
   * Block commit sequence ID of the block and container are only updated based on the peer's value if the entire block
   * is read and written successfully.
   *
   * To avoid verbose logging during reconciliation, this method should not log successful operations above the debug
   * level.
   *
   * @return The number of chunks that were reconciled in our container.
   */
  private long reconcileChunksPerBlock(KeyValueContainer container, Pipeline pipeline,
      DNContainerOperationClient dnClient, long localID, List<ContainerProtos.ChunkMerkleTree> peerChunkList,
      ContainerMerkleTreeWriter treeWriter, ByteBuffer chunkByteBuffer) throws IOException {
    long containerID = container.getContainerData().getContainerID();
    DatanodeDetails peer = pipeline.getFirstNode();

    BlockID blockID = new BlockID(containerID, localID);
    // The length of the block is not known, so instead of passing the default block length we pass 0. As the length
    // is not used to validate the token for getBlock call.
    Token<OzoneBlockTokenIdentifier> blockToken = dnClient.getTokenHelper().getBlockToken(blockID, 0L);

    // Contains all the chunks we currently have for this block.
    // This should be empty if we do not have the block.
    // As reconciliation progresses, we will add any updated chunks here and commit the resulting list back to the
    // block.
    NavigableMap<Long, ContainerProtos.ChunkInfo> localOffset2Chunk;
    long localBcsid = 0;
    BlockData localBlockData;
    if (blockManager.blockExists(container, blockID)) {
      localBlockData = blockManager.getBlock(container, blockID);
      localOffset2Chunk = localBlockData.getChunks().stream()
          .collect(Collectors.toMap(ContainerProtos.ChunkInfo::getOffset,
              Function.identity(), (chunk1, chunk2) -> chunk1, TreeMap::new));
      localBcsid = localBlockData.getBlockCommitSequenceId();
    } else {
      localOffset2Chunk = new TreeMap<>();
      // If we are creating the block from scratch because we don't have it, use 0 BCSID. This will get incremented
      // if we pull chunks from the peer to fill this block.
      localBlockData = new BlockData(blockID);
    }

    boolean allChunksSuccessful = true;
    int numSuccessfulChunks = 0;

    BlockLocationInfo blkInfo = new BlockLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .setToken(blockToken)
        .build();
    // Under construction is set here, during BlockInputStream#initialize() it is used to update the block length.
    blkInfo.setUnderConstruction(true);
    try (BlockInputStream blockInputStream = blockInputStreamFactory.createBlockInputStream(
        blkInfo, pipeline, blockToken, dnClient.getXceiverClientManager(),
        null, conf.getObject(OzoneClientConfig.class))) {
      // Initialize the BlockInputStream. Gets the blockData from the peer, sets the block length and
      // initializes ChunkInputStream for each chunk.
      blockInputStream.initialize();
      ContainerProtos.BlockData peerBlockData = blockInputStream.getStreamBlockData();
      long maxBcsId = Math.max(localBcsid, peerBlockData.getBlockID().getBlockCommitSequenceId());

      for (ContainerProtos.ChunkMerkleTree chunkMerkleTree : peerChunkList) {
        long chunkOffset = chunkMerkleTree.getOffset();
        if (!previousChunkPresent(blockID, chunkOffset, localOffset2Chunk)) {
          break;
        }

        if (!chunkMerkleTree.getChecksumMatches()) {
          LOG.warn("Skipping chunk at offset {} in block {} of container {} from peer {} since peer reported it as " +
                  "unhealthy.", chunkOffset, localID, containerID, peer);
          continue;
        }
        try {
          // Seek to the offset of the chunk. Seek updates the chunkIndex in the BlockInputStream.
          blockInputStream.seek(chunkOffset);
          ChunkInputStream currentChunkStream = blockInputStream.getChunkStreams().get(
              blockInputStream.getChunkIndex());
          ContainerProtos.ChunkInfo chunkInfoProto = currentChunkStream.getChunkInfo();

          // If we are overwriting a chunk, make sure is the same size as the current chunk we are replacing.
          if (localOffset2Chunk.containsKey(chunkOffset)) {
            verifyChunksLength(chunkInfoProto, localOffset2Chunk.get(chunkOffset));
          }

          // Read the chunk data from the BlockInputStream and write it to the container.
          int chunkLength = (int) chunkInfoProto.getLen();
          if (chunkByteBuffer.capacity() < chunkLength) {
            chunkByteBuffer = ByteBuffer.allocate(chunkLength);
          }

          chunkByteBuffer.clear();
          chunkByteBuffer.limit(chunkLength);
          int bytesRead = blockInputStream.read(chunkByteBuffer);
          // Make sure we read exactly the same amount of data we expected so it fits in the block.
          if (bytesRead != chunkLength) {
            throw new IOException("Error while reading chunk data from peer " + peer + ". Expected length: " +
                chunkLength + ", Actual length: " + bytesRead);
          }

          chunkByteBuffer.flip();
          ChunkBuffer chunkBuffer = ChunkBuffer.wrap(chunkByteBuffer);
          ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(chunkInfoProto);
          chunkInfo.addMetadata(OzoneConsts.CHUNK_OVERWRITE, "true");
          writeChunkForClosedContainer(chunkInfo, blockID, chunkBuffer, container);
          localOffset2Chunk.put(chunkOffset, chunkInfoProto);
          // Update the treeWriter to reflect the current state of blocks and chunks on disk after successful
          // chunk writes. If putBlockForClosedContainer fails, the container scanner will later update the
          // Merkle tree to resolve any discrepancies.
          treeWriter.addChunks(localID, true, chunkInfoProto);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully ingested chunk at offset {} into block {} of container {} from peer {}",
                chunkOffset, localID, containerID, peer);
          }
          numSuccessfulChunks++;
        } catch (IOException ex) {
          // The peer's chunk was expected to be healthy. Log a stack trace for more info as to why this failed.
          LOG.error("Failed to ingest chunk at offset {} for block {} in container {} from peer {}",
              chunkOffset, localID, containerID, peer, ex);
          allChunksSuccessful = false;
        }
        // Stop block repair once we fail to pull a chunk from the peer.
        // Our write chunk API currently does not have a good way to handle writing around holes in a block.
        if (!allChunksSuccessful) {
          break;
        }
      }

      // Do not update block metadata in this container if we did not ingest any chunks for the block.
      if (!localOffset2Chunk.isEmpty()) {
        List<ContainerProtos.ChunkInfo> allChunks = new ArrayList<>(localOffset2Chunk.values());
        localBlockData.setChunks(allChunks);
        putBlockForClosedContainer(container, localBlockData, maxBcsId, allChunksSuccessful);
        // Invalidate the file handle cache, so new read requests get the new file if one was created.
        chunkManager.finishWriteChunks(container, localBlockData);
      }
    }

    if (!allChunksSuccessful) {
      LOG.warn("Partially reconciled block {} in container {} with peer {}. {}/{} chunks were " +
          "obtained successfully", localID, containerID, peer, numSuccessfulChunks, peerChunkList.size());
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Reconciled all {} chunks in block {} in container {} from peer {}",
          peerChunkList.size(), localID, containerID, peer);
    }

    return numSuccessfulChunks;
  }

  private void verifyChunksLength(ContainerProtos.ChunkInfo peerChunkInfo, ContainerProtos.ChunkInfo localChunkInfo)
      throws StorageContainerException {
    if (localChunkInfo == null || peerChunkInfo == null) {
      return;
    }

    if (peerChunkInfo.getOffset() != localChunkInfo.getOffset()) {
      throw new StorageContainerException("Offset mismatch for chunk. Expected: " + localChunkInfo.getOffset() +
          ", Actual: " + peerChunkInfo.getOffset(), CHUNK_FILE_INCONSISTENCY);
    }

    if (peerChunkInfo.getLen() != localChunkInfo.getLen()) {
      throw new StorageContainerException("Length mismatch for chunk at offset " + localChunkInfo.getOffset() +
          ". Expected: " + localChunkInfo.getLen() + ", Actual: " + peerChunkInfo.getLen(), CHUNK_FILE_INCONSISTENCY);
    }
  }

  /**
   * If we do not have the previous chunk for the current entry, abort the reconciliation here. Currently we do
   * not support repairing around holes in a block, the missing chunk must be obtained first.
   */
  private boolean previousChunkPresent(BlockID blockID, long chunkOffset,
                                       NavigableMap<Long, ContainerProtos.ChunkInfo> localOffset2Chunk) {
    if (chunkOffset == 0) {
      return true;
    }
    long localID = blockID.getLocalID();
    long containerID = blockID.getContainerID();
    Map.Entry<Long, ContainerProtos.ChunkInfo> prevEntry = localOffset2Chunk.lowerEntry(chunkOffset);
    if (prevEntry == null) {
      // We are trying to write a chunk that is not the first, but we currently have no chunks in the block.
      LOG.warn("Exiting reconciliation for block {} in container {} at length {}. The previous chunk required for " +
          "offset {} is not present locally.", localID, containerID, 0, chunkOffset);
      return false;
    } else {
      long prevOffset = prevEntry.getKey();
      long prevLength = prevEntry.getValue().getLen();
      if (prevOffset + prevLength != chunkOffset) {
        LOG.warn("Exiting reconciliation for block {} in container {} at length {}. The previous chunk required for " +
            "offset {} is not present locally.", localID, containerID, prevOffset + prevLength, chunkOffset);
        return false;
      }
      return true;
    }
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

  @Override
  public ContainerCommandResponseProto readBlock(
      ContainerCommandRequestProto request, Container kvContainer,
      RandomAccessFileChannel blockFile,
      StreamObserver<ContainerCommandResponseProto> streamObserver) {

    if (kvContainer.getContainerData().getLayoutVersion() != FILE_PER_BLOCK) {
      return ContainerUtils.logAndReturnError(LOG,
          new StorageContainerException("Only File Per Block is supported", IO_EXCEPTION), request);
    }

    ContainerCommandResponseProto responseProto = null;
    if (!request.hasReadBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Read Block request. trace ID: {}", request.getTraceID());
      }
      return malformedRequest(request);
    }
    try {
      final long startTime = Time.monotonicNow();
      final long bytesRead = readBlockImpl(request, blockFile, kvContainer, streamObserver, false);
      KeyValueContainerData containerData = (KeyValueContainerData) kvContainer
          .getContainerData();
      HddsVolume volume = containerData.getVolume();
      if (volume != null) {
        volume.getVolumeIOStats().recordReadOperation(startTime, bytesRead);
      }
      metrics.incContainerBytesStats(Type.ReadBlock, bytesRead);
    } catch (StorageContainerException ex) {
      responseProto = ContainerUtils.logAndReturnError(LOG, ex, request);
    } catch (IOException ioe) {
      final StorageContainerException sce = new StorageContainerException(
          "Failed to readBlock " + request.getReadBlock(), ioe, IO_EXCEPTION);
      responseProto = ContainerUtils.logAndReturnError(LOG, sce, request);
    } catch (Exception e) {
      final StorageContainerException sce = new StorageContainerException(
          "Failed to readBlock " + request.getReadBlock(), e, CONTAINER_INTERNAL_ERROR);
      LOG.error("", sce);
      responseProto = ContainerUtils.logAndReturnError(LOG, sce, request);
    }
    return responseProto;
  }

  private long readBlockImpl(ContainerCommandRequestProto request, RandomAccessFileChannel blockFile,
      Container kvContainer, StreamObserver<ContainerCommandResponseProto> streamObserver, boolean verifyChecksum)
      throws IOException {
    final ReadBlockRequestProto readBlock = request.getReadBlock();
    int responseDataSize = readBlock.getResponseDataSize();
    if (responseDataSize == 0) {
      responseDataSize = 1 << 20;
    }

    final BlockID blockID = BlockID.getFromProtobuf(readBlock.getBlockID());
    if (!blockFile.isOpen()) {
      final File file = FILE_PER_BLOCK.getChunkFile(kvContainer.getContainerData(), blockID, "unused");
      blockFile.open(file);
    }

    // This is a new api the block should always be checked.
    BlockUtils.verifyReplicaIdx(kvContainer, blockID);
    BlockUtils.verifyBCSId(kvContainer, blockID);

    final BlockData blockData = getBlockManager().getBlock(kvContainer, blockID);
    final List<ContainerProtos.ChunkInfo> chunkInfos = blockData.getChunks();
    final int bytesPerChunk = Math.toIntExact(chunkInfos.get(0).getLen());
    final ChecksumType checksumType = chunkInfos.get(0).getChecksumData().getType();
    ChecksumData checksumData = null;
    int bytesPerChecksum = STREAMING_BYTES_PER_CHUNK;
    if (checksumType == ContainerProtos.ChecksumType.NONE) {
      checksumData = new ChecksumData(checksumType, 0);
    } else {
      bytesPerChecksum = chunkInfos.get(0).getChecksumData().getBytesPerChecksum();
    }
    // We have to align the read to checksum boundaries, so whatever offset is requested, we have to move back to the
    // previous checksum boundary.
    // eg if bytesPerChecksum is 512, and the requested offset is 600, we have to move back to 512.
    // If the checksum type is NONE, we don't have to do this, but using no checksums should be rare in practice and
    // it simplifies the code to always do this.
    final long offsetAlignment = readBlock.getOffset() % bytesPerChecksum;
    long adjustedOffset = readBlock.getOffset() - offsetAlignment;

    final ByteBuffer buffer = ByteBuffer.allocate(responseDataSize);
    blockFile.position(adjustedOffset);
    long totalDataLength = 0;
    int numResponses = 0;
    final long rounded = roundUp(readBlock.getLength() + offsetAlignment, bytesPerChecksum);
    final long requiredLength = Math.min(rounded, blockData.getSize() - adjustedOffset);
    LOG.debug("adjustedOffset {}, requiredLength {}, blockSize {}",
        adjustedOffset, requiredLength, blockData.getSize());
    for (boolean shouldRead = true; totalDataLength < requiredLength && shouldRead;) {
      shouldRead = blockFile.read(buffer);
      buffer.flip();
      final int readLength = buffer.remaining();
      if (readLength == 0) {
        assertTrue(!shouldRead);
        break;
      }
      assertTrue(readLength > 0, () -> "readLength = " + readLength + " <= 0");

      if (checksumType != ContainerProtos.ChecksumType.NONE) {
        final List<ByteString> checksums = getChecksums(adjustedOffset, readLength,
            bytesPerChunk, bytesPerChecksum, chunkInfos);
        LOG.debug("Read {} at adjustedOffset {}, readLength {}, bytesPerChunk {}, bytesPerChecksum {}",
            readBlock, adjustedOffset, readLength, bytesPerChunk, bytesPerChecksum);
        checksumData = new ChecksumData(checksumType, bytesPerChecksum, checksums);
        if (verifyChecksum) {
          Checksum.verifyChecksum(buffer.duplicate(), checksumData, 0);
        }
      }
      final ContainerCommandResponseProto response = getReadBlockResponse(
          request, checksumData, buffer, adjustedOffset);
      final int dataLength = response.getReadBlock().getData().size();
      LOG.debug("server onNext response {}: dataLength={}, numChecksums={}",
          numResponses, dataLength, response.getReadBlock().getChecksumData().getChecksumsList().size());
      streamObserver.onNext(response);
      buffer.clear();

      adjustedOffset += readLength;
      totalDataLength += dataLength;
      numResponses++;
    }
    return totalDataLength;
  }

  static List<ByteString> getChecksums(long blockOffset, int readLength, int bytesPerChunk, int bytesPerChecksum,
      final List<ContainerProtos.ChunkInfo> chunks) {
    assertSame(0, blockOffset % bytesPerChecksum, "blockOffset % bytesPerChecksum");
    final int numChecksums = 1 + (readLength - 1) / bytesPerChecksum;
    final List<ByteString> checksums = new ArrayList<>(numChecksums);
    for (int i = 0; i < numChecksums; i++) {
      // As the checksums are stored "chunk by chunk", we need to figure out which chunk we start reading from,
      // and its offset to pull out the correct checksum bytes for each read.
      final int n = i * bytesPerChecksum;
      final long offset = blockOffset + n;
      final int c = Math.toIntExact(offset / bytesPerChunk);
      final int chunkOffset = Math.toIntExact(offset % bytesPerChunk);
      final int csi = chunkOffset / bytesPerChecksum;

      assertTrue(c < chunks.size(),
          () -> "chunkIndex = " + c + " >= chunk.size()" + chunks.size());
      final ContainerProtos.ChunkInfo chunk = chunks.get(c);
      if (c < chunks.size() - 1) {
        assertSame(bytesPerChunk, chunk.getLen(), "bytesPerChunk");
      }
      final ContainerProtos.ChecksumData checksumDataProto = chunks.get(c).getChecksumData();
      assertSame(bytesPerChecksum, checksumDataProto.getBytesPerChecksum(), "bytesPerChecksum");
      final List<ByteString> checksumsList = checksumDataProto.getChecksumsList();
      assertTrue(csi < checksumsList.size(),
          () -> "checksumIndex = " + csi + " >= checksumsList.size()" + checksumsList.size());
      checksums.add(checksumsList.get(csi));
    }
    return checksums;
  }

  @Override
  public void addFinalizedBlock(Container container, long localID) {
    KeyValueContainer keyValueContainer = (KeyValueContainer)container;
    keyValueContainer.getContainerData().addToFinalizedBlockSet(localID);
  }

  @Override
  public boolean isFinalizedBlockExist(Container container, long localID) {
    KeyValueContainer keyValueContainer = (KeyValueContainer)container;
    return keyValueContainer.getContainerData().isFinalizedBlockExist(localID);
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
            stringBuilder);
      }
    }
    return nonZero;
  }

  private boolean logBlocksFoundOnDisk(Container container) throws IOException {
    // List files left over
    File chunksPath = new
        File(container.getContainerData().getChunksPath());
    assertTrue(chunksPath.isDirectory(), () -> chunksPath + " is not a directory");
    boolean notEmpty = false;
    try (DirectoryStream<Path> dir
             = Files.newDirectoryStream(chunksPath.toPath())) {
      StringBuilder stringBuilder = new StringBuilder();
      for (Path block : dir) {
        if (notEmpty) {
          stringBuilder.append(',');
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
    long startTime = clock.millis();
    container.writeLock();
    try {
      final ContainerData data = container.getContainerData();
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
          LOG.error("Received container deletion command for non-empty {}: {}", data, data.getStatistics());
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
          long waitTime = clock.millis() - startTime;
          if (waitTime > maxDeleteLockWaitMs) {
            LOG.warn("An attempt to delete container {} took {} ms acquiring locks and pre-checks. " +
                    "The delete has been skipped and should be retried automatically by SCM.",
                container.getContainerData().getContainerID(), waitTime);
            return;
          }
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
    sendICR(container);
    long bytesUsed = container.getContainerData().getBytesUsed();
    HddsVolume volume = container.getContainerData().getVolume();
    container.delete();
    volume.decrementUsedSpace(bytesUsed);
  }

  private void triggerVolumeScanAndThrowException(Container container,
      String msg, ContainerProtos.Result result)
      throws StorageContainerException {
    // Trigger a volume scan as exception occurred.
    StorageVolumeUtil.onFailure(container.getContainerData().getVolume());
    throw new StorageContainerException(msg, result);
  }

  private ContainerCommandResponseProto checkFaultInjector(ContainerCommandRequestProto request) {
    if (injector != null) {
      synchronized (injector) {
        ContainerProtos.Type type = injector.getType();
        if (request.getCmdType().equals(type) || type == null) {
          Throwable ex = injector.getException();
          if (ex != null) {
            if (type == null) {
              injector = null;
            }
            return ContainerUtils.logAndReturnError(LOG, (StorageContainerException) ex, request);
          }
          try {
            injector.pause();
          } catch (IOException e) {
            // do nothing
          }
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  public static FaultInjector getInjector() {
    return injector;
  }

  @VisibleForTesting
  public static void setInjector(FaultInjector instance) {
    injector = instance;
  }

  /**
   * Verify if request's replicaIndex matches with containerData. This validates only for EC containers i.e.
   * containerReplicaIdx should be > 0.
   *
   * @param containerReplicaIdx  replicaIndex for the container command.
   * @param requestDatanodeUUID requested block info
   * @throws StorageContainerException if replicaIndex mismatches.
   */
  private boolean validateRequestDatanodeId(Integer containerReplicaIdx, String requestDatanodeUUID)
      throws StorageContainerException {
    if (containerReplicaIdx != null && containerReplicaIdx > 0 && !requestDatanodeUUID.equals(this.getDatanodeId())) {
      throw new StorageContainerException(
          String.format("Request is trying to write to node with uuid : %s but the current nodeId is: %s .",
              requestDatanodeUUID, this.getDatanodeId()), INVALID_ARGUMENT);
    }
    return true;
  }
}
