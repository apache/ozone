/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdds.protocol.ReconfigProtocol;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.GetConfigurationChangeProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.GetReconfigStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.GetReconfigStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.ListReconfigPropertiesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.ListReconfigPropertiesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.StartReconfigRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigProtocolProtos.StartReconfigResponseProto;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link ReconfigProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN/DN as specified in the generic
 * ReconfigProtocol.
 */
public class ReconfigProtocolServerSideTranslatorPB implements
    ReconfigProtocolPB {

  private final ReconfigProtocol impl;

  private static final StartReconfigResponseProto START_RECONFIG_RESP =
      StartReconfigResponseProto.newBuilder().build();

  public ReconfigProtocolServerSideTranslatorPB(
      ReconfigProtocol impl) {
    this.impl = impl;
  }

  @Override
  public StartReconfigResponseProto startReconfig(
      RpcController controller, StartReconfigRequestProto request)
      throws ServiceException {
    try {
      impl.startReconfig();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  @Override
  public ListReconfigPropertiesResponseProto listReconfigProperties(
      RpcController controller, ListReconfigPropertiesRequestProto request)
      throws ServiceException {
    try {
      return listReconfigProperties(impl.listReconfigProperties());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private ListReconfigPropertiesResponseProto listReconfigProperties(
      List<String> reconfigurableProperties) {
    ListReconfigPropertiesResponseProto.Builder builder =
        ListReconfigPropertiesResponseProto.newBuilder();
    builder.addAllName(reconfigurableProperties);
    return builder.build();
  }

  @Override
  public GetReconfigStatusResponseProto getReconfigStatus(
      RpcController unused, GetReconfigStatusRequestProto request)
      throws ServiceException {
    try {
      return getReconfigStatus(impl.getReconfigStatus());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private GetReconfigStatusResponseProto getReconfigStatus(
      ReconfigurationTaskStatus status) {
    GetReconfigStatusResponseProto.Builder builder =
        GetReconfigStatusResponseProto.newBuilder();

    builder.setStartTime(status.getStartTime());
    if (status.stopped()) {
      builder.setEndTime(status.getEndTime());
      assert status.getStatus() != null;
      for (Map.Entry<PropertyChange, Optional<String>> result : status
          .getStatus().entrySet()) {
        GetConfigurationChangeProto.Builder changeBuilder = 
            GetConfigurationChangeProto.newBuilder();
        PropertyChange change = result.getKey();
        changeBuilder.setName(change.prop);
        changeBuilder.setOldValue(change.oldVal != null ? change.oldVal : "");
        if (change.newVal != null) {
          changeBuilder.setNewValue(change.newVal);
        }
        if (result.getValue().isPresent()) {
          // Get full stack trace.
          changeBuilder.setErrorMessage(result.getValue().get());
        }
        builder.addChanges(changeBuilder);
      }
    }
    return builder.build();
  }
}
