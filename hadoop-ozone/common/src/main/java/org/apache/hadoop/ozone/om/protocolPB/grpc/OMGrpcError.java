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

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMGrpcErrorProto;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;

/**
 * The Java form of {@link OMGrpcErrorProto}, the error that the OM gRPC server sends in the gRPC trailers.
 * It converts between this error and the exception thrown by OM.
 */
public final class OMGrpcError {
  private static final Metadata.Key<OMGrpcErrorProto> ERROR_KEY = Metadata.Key.of(
      "om-grpc-error" + Metadata.BINARY_HEADER_SUFFIX, new Metadata.BinaryMarshaller<OMGrpcErrorProto>() {
        @Override
        public byte[] toBytes(OMGrpcErrorProto error) {
          return error.toByteArray();
        }

        @Override
        public OMGrpcErrorProto parseBytes(byte[] serialized) {
          try {
            return OMGrpcErrorProto.parseFrom(serialized);
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to parse OMGrpcErrorProto", e);
          }
        }
      });

  private OMGrpcError() {
  }

  /** @return the gRPC error for the given exception thrown by OM. */
  public static StatusRuntimeException toStatusRuntimeException(Throwable omException) {
    final Metadata trailers = new Metadata();
    trailers.put(ERROR_KEY, toProto(omException));
    // NOTE: gRPC clients of old versions read the exception class and message from the description.
    return Status.INTERNAL.withDescription(omException.toString()).asRuntimeException(trailers);
  }

  /**
   * @return the exception thrown by OM that the gRPC error carries,
   *         or null if the error does not come from OM.
   */
  public static IOException toOMException(StatusRuntimeException e) {
    final Metadata trailers = e.getTrailers();
    final OMGrpcErrorProto error = trailers == null ? null : trailers.get(ERROR_KEY);
    if (error == null) {
      return null;
    }
    final String message = error.hasMessage() ? error.getMessage() : null;
    switch (error.getCode()) {
    case NOT_LEADER:
      return new OMNotLeaderException(message,
          error.hasSuggestedLeaderNodeId() ? error.getSuggestedLeaderNodeId() : null);
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

  private static OMGrpcErrorProto toProto(Throwable omException) {
    final OMGrpcErrorProto.Builder error = OMGrpcErrorProto.newBuilder().setCode(toCode(omException));
    if (omException.getMessage() != null) {
      error.setMessage(omException.getMessage());
    }
    if (omException instanceof OMNotLeaderException) {
      final String suggestedLeader = ((OMNotLeaderException) omException).getSuggestedLeaderNodeId();
      if (suggestedLeader != null) {
        error.setSuggestedLeaderNodeId(suggestedLeader);
      }
    }
    return error.build();
  }

  private static OMGrpcErrorProto.Code toCode(Throwable omException) {
    if (omException instanceof OMNotLeaderException) {
      return OMGrpcErrorProto.Code.NOT_LEADER;
    } else if (omException instanceof OMLeaderNotReadyException) {
      return OMGrpcErrorProto.Code.LEADER_NOT_READY;
    } else if (omException instanceof ReadIndexException) {
      return OMGrpcErrorProto.Code.READ_INDEX_FAILED;
    } else if (omException instanceof ReadException) {
      return OMGrpcErrorProto.Code.READ_FAILED;
    }
    return OMGrpcErrorProto.Code.INTERNAL;
  }
}
