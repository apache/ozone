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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/** Utility methods for testing HDDS containers. */
public class ContainerTestUtils {
  private static final ContainerDispatcher NOOP_CONTAINER_DISPATCHER
      = new ContainerDispatcher() {
    @Override
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg, DispatcherContext context) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void validateContainerCommand(ContainerCommandRequestProto msg) {
    }

    @Override
    public void init() {
    }

    @Override
    public void buildMissingContainerSetAndValidate(
        Map<Long, Long> container2BCSIDMap) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Handler getHandler(ContainerType containerType) {
      return null;
    }

    @Override
    public void setClusterId(String clusterId) {
    }
  };

  public static ContainerDispatcher getNoopContainerDispatcher() {
    return NOOP_CONTAINER_DISPATCHER;
  }

  private static ContainerController EMPTY_CONTAINER_CONTROLLER
      = new ContainerController(new ContainerSet(1000), Collections.emptyMap());

  public static ContainerController getEmptyContainerController() {
    return EMPTY_CONTAINER_CONTROLLER;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        dn.getPort(DatanodeDetails.Port.Name.RATIS).getValue());

    return XceiverServerRatis.newXceiverServerRatis(dn, conf,
        getNoopContainerDispatcher(), getEmptyContainerController(),
        null, null);
  }
}
