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

import java.io.IOException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMGrpcErrorProto;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;

/**
 * The Java form of {@link OMGrpcErrorProto}, the error that the OM gRPC server sends in the gRPC trailers.
 */
public final class OMGrpcError {
  private static final Metadata.Key<OMGrpcErrorProto> ERROR_KEY = Metadata.Key.of("om-grpc-error-bin",
      OzoneManagerServiceGrpcBinding.metadataMarshaller(OMGrpcErrorProto.parser()));

  private final Kind kind;
  private final String message;
  /** The OM node ID of the suggested leader, for {@link Kind#NOT_LEADER}. */
  private final String suggestedLeaderNodeId;

  /** A kind for each error in {@link OMGrpcErrorProto}, and {@link #INTERNAL} for any other error. */
  public enum Kind {
    NOT_LEADER,
    LEADER_NOT_READY,
    READ_INDEX_FAILED,
    READ_FAILED,
    INTERNAL
  }

  private OMGrpcError(Kind kind, String message, String suggestedLeaderNodeId) {
    this.kind = kind;
    this.message = message;
    this.suggestedLeaderNodeId = suggestedLeaderNodeId;
  }

  /** @return the error for the given exception thrown by OM. */
  public static OMGrpcError fromException(Throwable omException) {
    if (omException instanceof OMNotLeaderException) {
      return new OMGrpcError(Kind.NOT_LEADER, omException.getMessage(),
          ((OMNotLeaderException) omException).getSuggestedLeaderNodeId());
    } else if (omException instanceof OMLeaderNotReadyException) {
      return new OMGrpcError(Kind.LEADER_NOT_READY, omException.getMessage(), null);
    } else if (omException instanceof ReadIndexException) {
      return new OMGrpcError(Kind.READ_INDEX_FAILED, omException.getMessage(), null);
    } else if (omException instanceof ReadException) {
      return new OMGrpcError(Kind.READ_FAILED, omException.getMessage(), null);
    }
    return new OMGrpcError(Kind.INTERNAL, omException.toString(), null);
  }

  public static OMGrpcError getFromProtobuf(OMGrpcErrorProto proto) {
    switch (proto.getErrorCase()) {
    case NOTLEADER:
      final OMGrpcErrorProto.NotLeader notLeader = proto.getNotLeader();
      return new OMGrpcError(Kind.NOT_LEADER, notLeader.hasMessage() ? notLeader.getMessage() : null,
          notLeader.hasSuggestedLeaderNodeId() ? notLeader.getSuggestedLeaderNodeId() : null);
    case LEADERNOTREADY:
      final OMGrpcErrorProto.LeaderNotReady leaderNotReady = proto.getLeaderNotReady();
      return new OMGrpcError(Kind.LEADER_NOT_READY,
          leaderNotReady.hasMessage() ? leaderNotReady.getMessage() : null, null);
    case READINDEXFAILED:
      final OMGrpcErrorProto.ReadIndexFailed readIndexFailed = proto.getReadIndexFailed();
      return new OMGrpcError(Kind.READ_INDEX_FAILED,
          readIndexFailed.hasMessage() ? readIndexFailed.getMessage() : null, null);
    case READFAILED:
      final OMGrpcErrorProto.ReadFailed readFailed = proto.getReadFailed();
      return new OMGrpcError(Kind.READ_FAILED, readFailed.hasMessage() ? readFailed.getMessage() : null, null);
    default:
      return new OMGrpcError(Kind.INTERNAL, null, null);
    }
  }

  /**
   * @return the error that the gRPC error carries, or null if the gRPC error does not come from OM.
   */
  public static OMGrpcError fromStatusRuntimeException(StatusRuntimeException e) {
    final Metadata trailers = e.getTrailers();
    final OMGrpcErrorProto proto = trailers == null ? null : trailers.get(ERROR_KEY);
    if (proto == null) {
      return null;
    }
    final OMGrpcError error = getFromProtobuf(proto);
    return error.kind == Kind.INTERNAL ? new OMGrpcError(Kind.INTERNAL, e.getStatus().getDescription(), null) : error;
  }

  /** @return the gRPC error for the given exception thrown by OM. */
  public static StatusRuntimeException toStatusRuntimeException(Throwable omException) {
    final Metadata trailers = new Metadata();
    trailers.put(ERROR_KEY, fromException(omException).getProtobuf());
    // NOTE: gRPC clients of old versions read the exception class and message from the description.
    return Status.INTERNAL.withDescription(omException.toString()).asRuntimeException(trailers);
  }

  /**
   * @return the protobuf of this error. An {@link Kind#INTERNAL} error is empty,
   *         since the gRPC status description already has its message.
   */
  public OMGrpcErrorProto getProtobuf() {
    final OMGrpcErrorProto.Builder proto = OMGrpcErrorProto.newBuilder();
    switch (kind) {
    case NOT_LEADER:
      final OMGrpcErrorProto.NotLeader.Builder notLeader = OMGrpcErrorProto.NotLeader.newBuilder();
      if (message != null) {
        notLeader.setMessage(message);
      }
      if (suggestedLeaderNodeId != null) {
        notLeader.setSuggestedLeaderNodeId(suggestedLeaderNodeId);
      }
      proto.setNotLeader(notLeader);
      break;
    case LEADER_NOT_READY:
      final OMGrpcErrorProto.LeaderNotReady.Builder leaderNotReady = OMGrpcErrorProto.LeaderNotReady.newBuilder();
      if (message != null) {
        leaderNotReady.setMessage(message);
      }
      proto.setLeaderNotReady(leaderNotReady);
      break;
    case READ_INDEX_FAILED:
      final OMGrpcErrorProto.ReadIndexFailed.Builder readIndexFailed = OMGrpcErrorProto.ReadIndexFailed.newBuilder();
      if (message != null) {
        readIndexFailed.setMessage(message);
      }
      proto.setReadIndexFailed(readIndexFailed);
      break;
    case READ_FAILED:
      final OMGrpcErrorProto.ReadFailed.Builder readFailed = OMGrpcErrorProto.ReadFailed.newBuilder();
      if (message != null) {
        readFailed.setMessage(message);
      }
      proto.setReadFailed(readFailed);
      break;
    default:
      break;
    }
    return proto.build();
  }

  /** @return the exception thrown by OM. */
  public IOException toException() {
    switch (kind) {
    case NOT_LEADER:
      return new OMNotLeaderException(message, suggestedLeaderNodeId);
    case LEADER_NOT_READY:
      return new OMLeaderNotReadyException(message);
    case READ_INDEX_FAILED:
      return new ReadIndexException(message);
    case READ_FAILED:
      return new ReadException(message);
    default:
      return new OMException(message, OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  public Kind getKind() {
    return kind;
  }

  public String getMessage() {
    return message;
  }

  public String getSuggestedLeaderNodeId() {
    return suggestedLeaderNodeId;
  }
}
