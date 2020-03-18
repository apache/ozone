/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods for OM Ratis server and client.
 */
public final class OMRatisHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      OMRatisHelper.class);

  private OMRatisHelper() {
  }

  static RaftPeerId getRaftPeerId(String omId) {
    return RaftPeerId.valueOf(omId);
  }

  public static ByteString convertRequestToByteString(OMRequest request) {
    byte[] requestBytes = request.toByteArray();
    return ByteString.copyFrom(requestBytes);
  }

  public static OMRequest convertByteStringToOMRequest(ByteString byteString)
      throws InvalidProtocolBufferException {
    byte[] bytes = byteString.toByteArray();
    return OMRequest.parseFrom(bytes);
  }

  public static Message convertResponseToMessage(OMResponse response) {
    byte[] requestBytes = response.toByteArray();
    return Message.valueOf(ByteString.copyFrom(requestBytes));
  }

  public static OMResponse getOMResponseFromRaftClientReply(
      RaftClientReply reply) throws InvalidProtocolBufferException {
    byte[] bytes = reply.getMessage().getContent().toByteArray();
    return OMResponse.newBuilder(OMResponse.parseFrom(bytes))
        .setLeaderOMNodeId(reply.getReplierId())
        .build();
  }

  /**
   * Convert StateMachineLogEntryProto to String.
   * @param proto - {@link StateMachineLogEntryProto}
   * @return String
   */
  public static String smProtoToString(StateMachineLogEntryProto proto) {
    StringBuilder builder = new StringBuilder();
    try {
      builder.append(TextFormat.shortDebugString(
          OMRatisHelper.convertByteStringToOMRequest(proto.getLogData())));

    } catch (Throwable ex) {
      LOG.info("smProtoToString failed", ex);
      builder.append("smProtoToString failed with");
      builder.append(ex.getMessage());
    }
    return builder.toString();
  }
}