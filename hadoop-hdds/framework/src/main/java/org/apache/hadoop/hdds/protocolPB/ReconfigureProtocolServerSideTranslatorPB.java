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

package org.apache.hadoop.hdds.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetConfigurationChangeProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetReconfigureStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetReconfigureStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetServerNameRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetServerNameResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ListReconfigurePropertiesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ListReconfigurePropertiesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.StartReconfigureRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.StartReconfigureResponseProto;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link ReconfigureProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN/DN as specified in the generic
 * ReconfigureProtocol.
 */
public class ReconfigureProtocolServerSideTranslatorPB implements
    ReconfigureProtocolPB, ReconfigureProtocolOmPB, ReconfigureProtocolDatanodePB {

  private final ReconfigureProtocol impl;

  private static final StartReconfigureResponseProto START_RECONFIG_RESP =
      StartReconfigureResponseProto.newBuilder().build();

  public ReconfigureProtocolServerSideTranslatorPB(
      ReconfigureProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetServerNameResponseProto getServerName(RpcController controller,
      GetServerNameRequestProto request) throws ServiceException {
    try {
      return GetServerNameResponseProto.newBuilder()
          .setName(impl.getServerName()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StartReconfigureResponseProto startReconfigure(
      RpcController controller, StartReconfigureRequestProto request)
      throws ServiceException {
    try {
      impl.startReconfigure();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  @Override
  public ListReconfigurePropertiesResponseProto listReconfigureProperties(
      RpcController controller, ListReconfigurePropertiesRequestProto request)
      throws ServiceException {
    try {
      return listReconfigureProperties(impl.listReconfigureProperties());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private ListReconfigurePropertiesResponseProto listReconfigureProperties(
      List<String> reconfigurableProperties) {
    ListReconfigurePropertiesResponseProto.Builder builder =
        ListReconfigurePropertiesResponseProto.newBuilder();
    builder.addAllName(reconfigurableProperties);
    return builder.build();
  }

  @Override
  public GetReconfigureStatusResponseProto getReconfigureStatus(
      RpcController unused, GetReconfigureStatusRequestProto request)
      throws ServiceException {
    try {
      return getReconfigureStatus(impl.getReconfigureStatus());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private GetReconfigureStatusResponseProto getReconfigureStatus(
      ReconfigurationTaskStatus status) {
    GetReconfigureStatusResponseProto.Builder builder =
        GetReconfigureStatusResponseProto.newBuilder();

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
