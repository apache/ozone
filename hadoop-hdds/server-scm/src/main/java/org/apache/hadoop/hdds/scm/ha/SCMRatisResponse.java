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

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.SCMRatisResponseProto;
import org.apache.hadoop.hdds.scm.ha.io.ScmCodecFactory;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Represents the response from RatisServer.
 */
public final class SCMRatisResponse {

  private final boolean success;
  private final Object result;
  private final Exception exception;

  private SCMRatisResponse() {
    this(true, null, null);
  }

  private SCMRatisResponse(final Object result) {
    this(true, result, null);
  }

  private SCMRatisResponse(final Exception exception) {
    this(false, null, exception);
  }

  private SCMRatisResponse(final boolean success, final Object result,
                           final Exception exception) {
    this.success = success;
    this.result = result;
    this.exception = exception;
  }

  public boolean isSuccess() {
    return success;
  }

  public Object getResult() {
    return result;
  }

  public Exception getException() {
    return exception;
  }

  public static Message encode(final Object result)
      throws InvalidProtocolBufferException {

    if (result == null) {
      return Message.EMPTY;
    }

    final Class<?> type = result.getClass();
    final SCMRatisResponseProto response = SCMRatisResponseProto.newBuilder()
        .setType(type.getName())
        .setValue(ScmCodecFactory.getCodec(type).serialize(result))
        .build();
    return Message.valueOf(UnsafeByteOperations.unsafeWrap(response.toByteString().asReadOnlyByteBuffer()));
  }

  public static SCMRatisResponse decode(RaftClientReply reply)
      throws InvalidProtocolBufferException {
    if (!reply.isSuccess()) {
      return new SCMRatisResponse(reply.getException());
    }

    final ByteString response = reply.getMessage().getContent();

    if (response.isEmpty()) {
      return new SCMRatisResponse();
    }

    final SCMRatisResponseProto responseProto = SCMRatisResponseProto.parseFrom(response.asReadOnlyByteBuffer());

    // proto2 required-equivalent checks
    if (!responseProto.hasType()) {
      throw new InvalidProtocolBufferException("Missing response type");
    }
    if (!responseProto.hasValue()) {
      throw new InvalidProtocolBufferException("Missing response value");
    }

    try {
      final Class<?> type = ReflectionUtil.getClass(responseProto.getType());
      return new SCMRatisResponse(ScmCodecFactory.getCodec(type)
          .deserialize(type, responseProto.getValue()));
    } catch (ClassNotFoundException e) {
      throw new InvalidProtocolBufferException(responseProto.getType() +
          " cannot be decoded!" + e.getMessage());
    }
  }

}
