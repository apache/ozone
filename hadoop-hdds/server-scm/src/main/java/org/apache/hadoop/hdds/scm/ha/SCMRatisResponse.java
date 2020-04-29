/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.SCMRatisResponseProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

public class SCMRatisResponse {

  private final boolean success;
  private final Object result;
  private final Exception exception;

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

    final ByteString value;
    if (result instanceof GeneratedMessage) {
      value = ((GeneratedMessage) result).toByteString();
    } else if (result instanceof ProtocolMessageEnum) {
      value = ByteString.copyFrom(BigInteger.valueOf(
          ((ProtocolMessageEnum) result).getNumber()).toByteArray());
    } else {
      throw new InvalidProtocolBufferException(result.getClass() +
          " is not a protobuf object!");
    }

    final SCMRatisResponseProto response =
        SCMRatisResponseProto.newBuilder()
            .setType(result.getClass().getCanonicalName())
            .setValue(value)
        .build();
    return Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
            response.toByteArray()));
  }

  public static SCMRatisResponse decode(RaftClientReply reply)
      throws InvalidProtocolBufferException {
    return reply.isSuccess() ?
        new SCMRatisResponse(
            deserializeResult(reply.getMessage().getContent().toByteArray())) :
        new SCMRatisResponse(reply.getException());
  }

  private static Object deserializeResult(byte[] response)
      throws InvalidProtocolBufferException {
    final SCMRatisResponseProto responseProto =
        SCMRatisResponseProto.parseFrom(response);
    try {
      final Class<?> clazz = HAUtil.getClass(responseProto.getType());
      if (GeneratedMessage.class.isAssignableFrom(clazz)) {
        return HAUtil.getMethod(clazz, "parseFrom", byte[].class)
            .invoke(null, (Object) responseProto.getValue().toByteArray());
      }

      if (Enum.class.isAssignableFrom(clazz)) {
        return HAUtil.getMethod(clazz, "valueOf", int.class)
            .invoke(null, new BigInteger(
                responseProto.getValue().toByteArray()).intValue());
      }

      throw new InvalidProtocolBufferException(responseProto.getType() +
            " is not a protobuf object!");

    } catch (ClassNotFoundException | NoSuchMethodException |
        IllegalAccessException | InvocationTargetException ex) {
      throw new InvalidProtocolBufferException(responseProto.getType() +
          " cannot be decoded!" + ex.getMessage());
    }

  }

}
