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

package org.apache.hadoop.hdds.scm.protocolPB;

import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status.INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationResponse;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ScmBlockLocationProtocolClientSideTranslatorPB.
 */
public class TestScmBlockLocationProtocolClientSideTranslatorPB {

  private ScmBlockLocationProtocolPB mockRpcProxy;
  private ScmBlockLocationProtocolClientSideTranslatorPB translator;

  @BeforeEach
  public void setUp() {
    SCMBlockLocationFailoverProxyProvider mockProxyProvider = mock(SCMBlockLocationFailoverProxyProvider.class);
    mockRpcProxy = mock(ScmBlockLocationProtocolPB.class);
    when(mockProxyProvider.getInterface()).thenReturn(ScmBlockLocationProtocolPB.class);
    when(mockProxyProvider.getProxy()).thenReturn(new FailoverProxyProvider.ProxyInfo<>(mockRpcProxy, null));
    translator = new ScmBlockLocationProtocolClientSideTranslatorPB(mockProxyProvider, new OzoneConfiguration());
  }

  @Test
  public void testGetScmInfo() throws IOException, ServiceException {
    // Setup
    HddsProtos.GetScmInfoResponseProto mockResponse = HddsProtos.GetScmInfoResponseProto.newBuilder()
        .setClusterId("test-cluster-id")
        .setScmId("test-scm-id")
        .setMetaDataLayoutVersion(1)
        .build();

    SCMBlockLocationResponse responseWrapper = SCMBlockLocationResponse.newBuilder()
        .setStatus(OK)
        .setGetScmInfoResponse(mockResponse)
        .setCmdType(ScmBlockLocationProtocolProtos.Type.GetScmInfo)
        .build();

    when(mockRpcProxy.send(any(), any(SCMBlockLocationRequest.class)))
        .thenReturn(responseWrapper);

    // Execute
    ScmInfo result = translator.getScmInfo();

    // Verify
    assertEquals("test-cluster-id", result.getClusterId());
    assertEquals("test-scm-id", result.getScmId());
    verify(mockRpcProxy).send(any(), any(SCMBlockLocationRequest.class));
  }

  @Test
  public void testGetScmInfoWithoutMetaDataLayoutVersion() throws IOException, ServiceException {
    // Setup
    HddsProtos.GetScmInfoResponseProto mockResponse = HddsProtos.GetScmInfoResponseProto.newBuilder()
        .setClusterId("test-cluster-id")
        .setScmId("test-scm-id")
        .build();

    SCMBlockLocationResponse responseWrapper = SCMBlockLocationResponse.newBuilder()
        .setStatus(OK)
        .setGetScmInfoResponse(mockResponse)
        .setCmdType(ScmBlockLocationProtocolProtos.Type.GetScmInfo)
        .build();

    when(mockRpcProxy.send(any(), any(SCMBlockLocationRequest.class)))
        .thenReturn(responseWrapper);

    // Execute
    ScmInfo result = translator.getScmInfo();

    // Verify
    assertEquals("test-cluster-id", result.getClusterId());
    assertEquals("test-scm-id", result.getScmId());
    verify(mockRpcProxy).send(any(), any(SCMBlockLocationRequest.class));
  }

  @Test
  public void testGetScmInfoWithError() throws ServiceException {
    // Setup
    SCMBlockLocationResponse errorResponse = SCMBlockLocationResponse.newBuilder()
        .setStatus(INTERNAL_ERROR)
        .setMessage("Internal error occurred")
        .setCmdType(ScmBlockLocationProtocolProtos.Type.GetScmInfo)
        .build();

    when(mockRpcProxy.send(any(), any(SCMBlockLocationRequest.class)))
        .thenReturn(errorResponse);

    // Execute & Verify
    SCMException exception = assertThrows(SCMException.class, () -> {
      translator.getScmInfo();
    });
    assertEquals("Internal error occurred", exception.getMessage());
    verify(mockRpcProxy).send(any(), any(SCMBlockLocationRequest.class));
  }
}
