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
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StartDiskBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StartDiskBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StopDiskBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StopDiskBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.UpdateDiskBalancerConfigurationRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.UpdateDiskBalancerConfigurationResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;

/**
 * Server-side translator for {@link DiskBalancerProtocolPB}.
 */
public class DiskBalancerProtocolServerSideTranslatorPB
    implements DiskBalancerProtocolPB {

  private final DiskBalancerProtocol impl;

  public DiskBalancerProtocolServerSideTranslatorPB(DiskBalancerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetDiskBalancerInfoResponseProto getDiskBalancerInfo(RpcController controller,
      GetDiskBalancerInfoRequestProto request) throws ServiceException {
    try {
      return GetDiskBalancerInfoResponseProto.newBuilder()
          .setInfo(impl.getDiskBalancerInfo(request))
          .build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public StartDiskBalancerResponseProto startDiskBalancer(RpcController controller,
      StartDiskBalancerRequestProto request) throws ServiceException {
    try {
      DiskBalancerConfigurationProto config =
          request.hasConfig() ? request.getConfig() : null;
      impl.startDiskBalancer(config);
      return StartDiskBalancerResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public StopDiskBalancerResponseProto stopDiskBalancer(RpcController controller,
      StopDiskBalancerRequestProto request) throws ServiceException {
    try {
      impl.stopDiskBalancer();
      return StopDiskBalancerResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public UpdateDiskBalancerConfigurationResponseProto updateDiskBalancerConfiguration(RpcController controller,
      UpdateDiskBalancerConfigurationRequestProto request) throws ServiceException {
    try {
      impl.updateDiskBalancerConfiguration(request.getConfig());
      return UpdateDiskBalancerConfigurationResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }
}


