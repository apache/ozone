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

package org.apache.hadoop.ozone.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RpcClient#getOzoneFileStatus} propagates the headOp flag
 * all the way into the wire {@code KeyArgs}, so the OM can skip the pipeline
 * refresh for OFS type checks (HDDS-15678).
 */
public class TestRpcClientGetFileStatusHeadOp {

  private final AtomicReference<KeyArgs> captured = new AtomicReference<>();

  private RpcClient newClient() throws IOException {
    InMemoryConfigurationForTesting conf = new InMemoryConfigurationForTesting();
    conf.setFromObject(conf.getObject(OzoneClientConfig.class));
    return new RpcClient(conf, null) {
      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return new MockOmTransport() {
          @Override
          public OMResponse submitRequest(OMRequest payload) throws IOException {
            if (payload.getCmdType() == Type.GetFileStatus) {
              captured.set(payload.getGetFileStatusRequest().getKeyArgs());
              // Request captured; short-circuit before building a response.
              throw new IOException("captured");
            }
            return super.submitRequest(payload);
          }
        };
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };
  }

  @Test
  public void headOpFlagReachesWireKeyArgs() throws IOException {
    RpcClient client = newClient();
    try {
      assertThrows(IOException.class,
          () -> client.getOzoneFileStatus("vol", "bucket", "key", true));
      assertTrue(captured.get().getHeadOp(),
          "headOp=true must be sent in the GetFileStatus KeyArgs");

      assertThrows(IOException.class,
          () -> client.getOzoneFileStatus("vol", "bucket", "key"));
      assertFalse(captured.get().getHeadOp(),
          "default getOzoneFileStatus must not set headOp");
    } finally {
      client.close();
    }
  }
}
