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

package org.apache.hadoop.ozone.om.protocolPB.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GrpcOmExceptionUtils}.
 */
public class TestGrpcOmExceptionUtils {

  @Test
  void testOmExceptionRoundTrip() {
    final OMLeaderNotReadyException omException = new OMLeaderNotReadyException("om1 is not ready: replaying log");

    final StatusRuntimeException e = GrpcOmExceptionUtils.toStatusRuntimeException(omException);
    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
    assertEquals(omException.toString(), e.getStatus().getDescription());

    final RemoteException remote = GrpcOmExceptionUtils.toRemoteException(e);
    assertEquals(OMLeaderNotReadyException.class.getName(), remote.getClassName());
    final IOException unwrapped = remote.unwrapRemoteException();
    assertThat(unwrapped).isInstanceOf(OMLeaderNotReadyException.class)
        .hasMessage("om1 is not ready: replaying log");
  }

  @Test
  void testExceptionWithoutMessage() {
    final RemoteException remote = GrpcOmExceptionUtils.toRemoteException(
        GrpcOmExceptionUtils.toStatusRuntimeException(new IllegalStateException()));

    assertEquals(IllegalStateException.class.getName(), remote.getClassName());
    assertNull(remote.getMessage());
  }

  @Test
  void testErrorWithoutOmException() {
    assertNull(GrpcOmExceptionUtils.toRemoteException(Status.INTERNAL.withDescription("a: b").asRuntimeException()));
    assertNull(GrpcOmExceptionUtils.toRemoteException(Status.UNAVAILABLE.asRuntimeException()));
  }
}
