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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import org.apache.hadoop.ipc_.Server;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Test ozone metadata reader.
 */
public class TestOMMetadataReader {

  @Test
  public void testGetClientAddress() {
    try (
        MockedStatic<Server> ipcServerStaticMock = mockStatic(Server.class);
        MockedStatic<Context> grpcRequestContextStaticMock = mockStatic(Context.class);
    ) {
      // given
      String expectedClientAddressInCaseOfHadoopRpcCall =
          "hadoop.ipc.client.com";
      ipcServerStaticMock.when(Server::getRemoteAddress)
          .thenReturn(null, null, expectedClientAddressInCaseOfHadoopRpcCall);

      String expectedClientAddressInCaseOfGrpcCall = "172.45.23.4";
      Context.Key<String> clientIpAddressKey = mock(Context.Key.class);
      when(clientIpAddressKey.get())
          .thenReturn(expectedClientAddressInCaseOfGrpcCall, null);

      grpcRequestContextStaticMock.when(() -> Context.key("CLIENT_IP_ADDRESS"))
          .thenReturn(clientIpAddressKey);

      // when (GRPC call with defined client address)
      String clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfGrpcCall, clientAddress);

      // and when (GRPC call without client address)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals("", clientAddress);

      // and when (Hadoop RPC client call)
      clientAddress = OmMetadataReader.getClientAddress();
      // then
      assertEquals(expectedClientAddressInCaseOfHadoopRpcCall, clientAddress);
    }
  }

}
