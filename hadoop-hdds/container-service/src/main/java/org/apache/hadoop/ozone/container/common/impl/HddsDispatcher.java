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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerType;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Ozone Container dispatcher takes a call from the netty server and routes it
 * to the right handler function.
 */
public class HddsDispatcher implements ContainerDispatcher {

  static final Logger LOG = LoggerFactory.getLogger(HddsDispatcher.class);

  private final Map<ContainerType, Handler> handlers;
  private final Configuration conf;
  private final ContainerSet containerSet;
  private final VolumeSet volumeSet;
  private String scmID;

  /**
   * Constructs an OzoneContainer that receives calls from
   * XceiverServerHandler.
   */
  public HddsDispatcher(Configuration config, ContainerSet contSet,
      VolumeSet volumes) {
    //TODO: initialize metrics
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumes;
    this.handlers = Maps.newHashMap();
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(
              containerType, conf, containerSet, volumeSet));
    }
  }

  @Override
  public void init() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ContainerCommandResponseProto dispatch(
      ContainerCommandRequestProto msg) {
    LOG.trace("Command {}, trace ID: {} ", msg.getCmdType().toString(),
        msg.getTraceID());
    Preconditions.checkNotNull(msg);

    Container container = null;
    ContainerType containerType = null;
    try {
      long containerID = getContainerID(msg);

      if (msg.getCmdType() != ContainerProtos.Type.CreateContainer) {
        container = getContainer(containerID);
        containerType = getContainerType(container);
      } else {
        containerType = msg.getCreateContainer().getContainerType();
      }
    } catch (StorageContainerException ex) {
      return ContainerUtils.logAndReturnError(LOG, ex, msg);
    }

    Handler handler = getHandler(containerType);
    if (handler == null) {
      StorageContainerException ex = new StorageContainerException("Invalid " +
          "ContainerType " + containerType,
          ContainerProtos.Result.CONTAINER_INTERNAL_ERROR);
      return ContainerUtils.logAndReturnError(LOG, ex, msg);
    }
    return handler.handle(msg, container);
  }

  @Override
  public Handler getHandler(ContainerProtos.ContainerType containerType) {
    return handlers.get(containerType);
  }

  @Override
  public void setScmId(String scmId) {
    Preconditions.checkNotNull(scmId, "scmId Cannot be null");
    if (this.scmID == null) {
      this.scmID = scmId;
      for (Map.Entry<ContainerType, Handler> handlerMap : handlers.entrySet()) {
        handlerMap.getValue().setScmID(scmID);
      }
    }
  }

  private long getContainerID(ContainerCommandRequestProto request)
      throws StorageContainerException {
    ContainerProtos.Type cmdType = request.getCmdType();

    switch(cmdType) {
    case CreateContainer:
      return request.getCreateContainer().getContainerID();
    case ReadContainer:
      return request.getReadContainer().getContainerID();
    case UpdateContainer:
      return request.getUpdateContainer().getContainerID();
    case DeleteContainer:
      return request.getDeleteContainer().getContainerID();
    case ListContainer:
      return request.getListContainer().getStartContainerID();
    case CloseContainer:
      return request.getCloseContainer().getContainerID();
    case PutKey:
      return request.getPutKey().getKeyData().getBlockID().getContainerID();
    case GetKey:
      return request.getGetKey().getBlockID().getContainerID();
    case DeleteKey:
      return request.getDeleteKey().getBlockID().getContainerID();
    case ListKey:
      return request.getListKey().getContainerID();
    case ReadChunk:
      return request.getReadChunk().getBlockID().getContainerID();
    case DeleteChunk:
      return request.getDeleteChunk().getBlockID().getContainerID();
    case WriteChunk:
      return request.getWriteChunk().getBlockID().getContainerID();
    case ListChunk:
      return request.getListChunk().getBlockID().getContainerID();
    case PutSmallFile:
      return request.getPutSmallFile().getKey().getKeyData().getBlockID()
          .getContainerID();
    case GetSmallFile:
      return request.getGetSmallFile().getKey().getBlockID().getContainerID();
    }

    throw new StorageContainerException(
        ContainerProtos.Result.UNSUPPORTED_REQUEST);
  }

  @VisibleForTesting
  public Container getContainer(long containerID)
      throws StorageContainerException {
    Container container = containerSet.getContainer(containerID);
    if (container == null) {
      throw new StorageContainerException(
          "ContainerID " + containerID + " does not exist",
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
    }
    return container;
  }

  private ContainerType getContainerType(Container container) {
    return container.getContainerType();
  }
}
