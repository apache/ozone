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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMGrpcErrorUtils}.
 */
public class TestOMGrpcErrorUtils {

  @Test
  void testNotLeaderExceptionKeepsSuggestedLeader() {
    final OMNotLeaderException omException =
        new OMNotLeaderException(RaftPeerId.valueOf("om1"), RaftPeerId.valueOf("om2"), "host2/10.0.0.2");

    final IOException e = roundTrip(omException);

    assertThat(e).isInstanceOf(OMNotLeaderException.class).hasMessage(omException.getMessage());
    assertEquals("om2", ((OMNotLeaderException) e).getSuggestedLeaderNodeId());
  }

  @Test
  void testOmExceptionRoundTrip() {
    assertRoundTrip(new OMNotLeaderException(RaftPeerId.valueOf("om1")));
    assertRoundTrip(new OMLeaderNotReadyException("om1 is Leader but not ready to process request yet."));
    assertRoundTrip(new ReadIndexException("read index failed"));
    assertRoundTrip(new ReadException("read failed"));
  }

  @Test
  void testOtherExceptionIsInternalError() {
    final IOException e = roundTrip(new IllegalStateException("unexpected"));

    assertThat(e).isInstanceOf(OMException.class).hasMessage("unexpected");
    assertEquals(OMException.ResultCodes.INTERNAL_ERROR, ((OMException) e).getResult());
  }

  @Test
  void testDescriptionKeepsExceptionClassAndMessage() {
    final OMLeaderNotReadyException omException = new OMLeaderNotReadyException("om1 is not ready");

    final StatusRuntimeException e = OMGrpcErrorUtils.toStatusRuntimeException(omException);

    assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
    assertEquals(omException.toString(), e.getStatus().getDescription());
  }

  @Test
  void testErrorWithoutOmError() {
    assertNull(OMGrpcErrorUtils.toOMException(Status.INTERNAL.withDescription("a: b").asRuntimeException()));
    assertNull(OMGrpcErrorUtils.toOMException(Status.UNAVAILABLE.asRuntimeException()));
  }

  private static void assertRoundTrip(IOException omException) {
    final IOException e = roundTrip(omException);
    assertEquals(omException.getClass(), e.getClass());
    assertEquals(omException.getMessage(), e.getMessage());
  }

  private static IOException roundTrip(Throwable omException) {
    return OMGrpcErrorUtils.toOMException(OMGrpcErrorUtils.toStatusRuntimeException(omException));
  }
}
