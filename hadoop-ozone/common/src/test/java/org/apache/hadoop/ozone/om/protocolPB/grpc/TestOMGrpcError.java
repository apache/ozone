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

import java.io.IOException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.grpc.OMGrpcError.Kind;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMGrpcErrorProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMGrpcError}.
 */
public class TestOMGrpcError {
  private static final OMNotLeaderException NOT_LEADER =
      new OMNotLeaderException(RaftPeerId.valueOf("om1"), RaftPeerId.valueOf("om2"), "host2/10.0.0.2");

  @Test
  void testFromException() {
    assertError(OMGrpcError.fromException(NOT_LEADER), Kind.NOT_LEADER, NOT_LEADER.getMessage(), "om2");
    assertError(OMGrpcError.fromException(new OMLeaderNotReadyException("om1 is not ready")),
        Kind.LEADER_NOT_READY, "om1 is not ready", null);
    assertError(OMGrpcError.fromException(new ReadIndexException("read index failed")),
        Kind.READ_INDEX_FAILED, "read index failed", null);
    assertError(OMGrpcError.fromException(new ReadException("read failed")), Kind.READ_FAILED, "read failed", null);

    final IllegalStateException other = new IllegalStateException("unexpected");
    assertError(OMGrpcError.fromException(other), Kind.INTERNAL, other.toString(), null);
  }

  @Test
  void testProtobuf() {
    final OMGrpcError notLeader = OMGrpcError.fromException(NOT_LEADER);
    assertError(OMGrpcError.getFromProtobuf(notLeader.getProtobuf()), Kind.NOT_LEADER, NOT_LEADER.getMessage(), "om2");

    for (IOException omException : new IOException[] {new OMLeaderNotReadyException("om1 is not ready"),
        new ReadIndexException("read index failed"), new ReadException("read failed")}) {
      final OMGrpcError error = OMGrpcError.fromException(omException);
      assertError(OMGrpcError.getFromProtobuf(error.getProtobuf()), error.getKind(), omException.getMessage(), null);
    }

    // The message of any other error is only in the gRPC status description.
    assertEquals(OMGrpcErrorProto.getDefaultInstance(),
        OMGrpcError.fromException(new IllegalStateException("unexpected")).getProtobuf());
  }

  @Test
  void testStatusRuntimeException() {
    final StatusRuntimeException notLeader = OMGrpcError.toStatusRuntimeException(NOT_LEADER);
    assertEquals(Status.Code.INTERNAL, notLeader.getStatus().getCode());
    assertEquals(NOT_LEADER.toString(), notLeader.getStatus().getDescription());
    assertError(OMGrpcError.fromStatusRuntimeException(notLeader), Kind.NOT_LEADER, NOT_LEADER.getMessage(), "om2");

    final IllegalStateException other = new IllegalStateException("unexpected");
    assertError(OMGrpcError.fromStatusRuntimeException(OMGrpcError.toStatusRuntimeException(other)),
        Kind.INTERNAL, other.toString(), null);
  }

  @Test
  void testErrorWithoutOmError() {
    assertNull(OMGrpcError.fromStatusRuntimeException(Status.INTERNAL.withDescription("a: b").asRuntimeException()));
    assertNull(OMGrpcError.fromStatusRuntimeException(Status.UNAVAILABLE.asRuntimeException()));
  }

  @Test
  void testToException() {
    final IOException notLeader = OMGrpcError.fromException(NOT_LEADER).toException();
    assertThat(notLeader).isInstanceOf(OMNotLeaderException.class).hasMessage(NOT_LEADER.getMessage());
    assertEquals("om2", ((OMNotLeaderException) notLeader).getSuggestedLeaderNodeId());

    assertToException(new OMLeaderNotReadyException("om1 is not ready"));
    assertToException(new ReadIndexException("read index failed"));
    assertToException(new ReadException("read failed"));

    final IllegalStateException other = new IllegalStateException("unexpected");
    final IOException internal = OMGrpcError.fromException(other).toException();
    assertThat(internal).isInstanceOf(OMException.class).hasMessage(other.toString());
    assertEquals(OMException.ResultCodes.INTERNAL_ERROR, ((OMException) internal).getResult());
  }

  private static void assertError(OMGrpcError error, Kind kind, String message, String suggestedLeaderNodeId) {
    assertEquals(kind, error.getKind());
    assertEquals(message, error.getMessage());
    assertEquals(suggestedLeaderNodeId, error.getSuggestedLeaderNodeId());
  }

  private static void assertToException(IOException omException) {
    final IOException e = OMGrpcError.fromException(omException).toException();
    assertEquals(omException.getClass(), e.getClass());
    assertEquals(omException.getMessage(), e.getMessage());
  }
}
