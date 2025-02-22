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

package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.utils.io.ByteBufferInputStream;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for converting between proto 2 (OM) and proto 3 (Ratis) messages.
 */
public final class OMRatisHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      OMRatisHelper.class);

  private OMRatisHelper() {
  }

  /** Convert the given proto 2 request to a proto 3 {@link ByteString}. */
  public static ByteString convertRequestToByteString(OMRequest request) {
    return UnsafeByteOperations.unsafeWrap(request.toByteString().asReadOnlyByteBuffer());
  }

  /** Convert the given proto 3 {@link ByteString} to a proto 2 request. */
  public static OMRequest convertByteStringToOMRequest(ByteString bytes) throws IOException {
    final ByteBuffer buffer = bytes.asReadOnlyByteBuffer();
    return OMRequest.parseFrom(new ByteBufferInputStream(buffer));
  }

  /** Convert the given proto 2 response to a proto 3 {@link ByteString}. */
  public static Message convertResponseToMessage(OMResponse response) {
    return () -> UnsafeByteOperations.unsafeWrap(response.toByteString().asReadOnlyByteBuffer());
  }

  /** Convert the given proto 3 {@link ByteString} to a proto 2 response. */
  public static OMResponse convertByteStringToOMResponse(ByteString bytes) throws IOException {
    final ByteBuffer buffer = bytes.asReadOnlyByteBuffer();
    return OMResponse.parseFrom(new ByteBufferInputStream(buffer));
  }

  /** Convert the given reply with proto 3 {@link ByteString} to a proto 2 response. */
  public static OMResponse getOMResponseFromRaftClientReply(RaftClientReply reply) throws IOException {
    final OMResponse response = convertByteStringToOMResponse(reply.getMessage().getContent());
    if (reply.getReplierId().equals(response.getLeaderOMNodeId())) {
      return response;
    }
    return OMResponse.newBuilder(response)
        .setLeaderOMNodeId(reply.getReplierId())
        .build();
  }

  /** Convert the given {@link StateMachineLogEntryProto} to a short {@link String}. */
  public static String smProtoToString(StateMachineLogEntryProto proto) {
    try {
      final OMRequest request = convertByteStringToOMRequest(proto.getLogData());
      return TextFormat.shortDebugString(request);
    } catch (Throwable ex) {
      LOG.info("smProtoToString failed", ex);
      return "Failed to smProtoToString: " + ex;
    }
  }
}
