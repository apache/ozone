/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.protocolPB;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.ProtocolProxy;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestOMAdminProtocolClientSideImpl {

  @Test
  void doesNotRetryAccessControlException() throws Exception {
    OMAdminProtocolPB rpcProxy = mock(OMAdminProtocolPB.class);
    when(rpcProxy.getPeerUpgradeStatus(any(), any()))
        .thenThrow(new ServiceException(new AccessControlException("Permission denied.")));

    OMNodeDetails omNodeDetails = mock(OMNodeDetails.class);
    when(omNodeDetails.getRpcAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
    when(omNodeDetails.getOMPrintInfo()).thenReturn("om1");

    try (MockedStatic<RPC> rpc = mockStatic(RPC.class)) {
      rpc.when(() -> RPC.getProtocolProxy(
          eq(OMAdminProtocolPB.class), anyLong(), any(), any(), any(), any(), anyInt(), any()))
          .thenReturn(new ProtocolProxy<>(OMAdminProtocolPB.class, rpcProxy));

      OMAdminProtocolClientSideImpl client = OMAdminProtocolClientSideImpl.createProxyForSingleOM(
          new OzoneConfiguration(), UserGroupInformation.getCurrentUser(), omNodeDetails);

      assertThrows(IOException.class, client::getPeerUpgradeStatus);
    }

    verify(rpcProxy, times(1)).getPeerUpgradeStatus(any(), any());
  }
}
