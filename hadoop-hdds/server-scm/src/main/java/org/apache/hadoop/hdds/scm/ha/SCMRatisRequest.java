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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Ints;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;

import org.apache.ratis.protocol.Message;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.ListArgument;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.Method;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.MethodArgument;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.SCMRatisRequestProto;


/**
 * Represents the request that is sent to RatisServer.
 */
public final class SCMRatisRequest {

  private final RequestType type;
  private final String operation;
  private final Object[] arguments;

  private SCMRatisRequest(final RequestType type, final String operation,
                         final Object... arguments) {
    this.type = type;
    this.operation = operation;
    this.arguments = arguments;
  }

  public static SCMRatisRequest of(final RequestType type,
                                   final String operation,
                                   final Object... arguments) {
    return new SCMRatisRequest(type, operation, arguments);
  }

  /**
   * Returns the type of request.
   */
  public RequestType getType() {
    return type;
  }

  /**
   * Returns the operation that this request represents.
   */
  public String getOperation() {
    return operation;
  }

  /**
   * Returns the arguments encoded in the request.
   */
  public Object[] getArguments() {
    return arguments.clone();
  }

  private static MethodArgument toMethodArgument(
      String type, ByteString value) {
    final MethodArgument.Builder argBuilder = MethodArgument.newBuilder();
    argBuilder.setType(type);
    argBuilder.setValue(value);
    return argBuilder.build();
  }

  private List<MethodArgument> encodeArgs()
      throws InvalidProtocolBufferException {
    final List<MethodArgument> args = new ArrayList<>();
    for (Object argument : arguments) {
      args.add(
          toMethodArgument(argument.getClass().getName(), encodeArg(argument)));
    }
    return args;
  }

  private ByteString encodeArg(Object argument)
      throws InvalidProtocolBufferException {
    if (argument instanceof GeneratedMessage) {
      return ((GeneratedMessage) argument).toByteString();
    } else if (argument instanceof ProtocolMessageEnum) {
      return ByteString.copyFrom(Ints.toByteArray(
          ((ProtocolMessageEnum) argument).getNumber()));
    } else if (argument instanceof Long) {
      return ByteString.copyFrom(
          ((ByteBuffer) ByteBuffer.allocate(8)
              .putLong((Long)argument).flip()));
    } else if (argument instanceof List<?>) {
      final ListArgument.Builder listBuilder = ListArgument.newBuilder();
      for (Object o : (List<?>) argument) {
        listBuilder.setType(o.getClass().getName());
        listBuilder.addValue(encodeArg(o));
      }
      return listBuilder.build().toByteString();
    } else {
      throw new InvalidProtocolBufferException(argument.getClass() +
          " is not a protobuf object!");
    }
  }

  /**
   * Encodes the request into Ratis Message.
   */
  public Message encode() throws InvalidProtocolBufferException {
    final SCMRatisRequestProto.Builder requestProtoBuilder =
        SCMRatisRequestProto.newBuilder();
    requestProtoBuilder.setType(type);

    final Method.Builder methodBuilder = Method.newBuilder();
    methodBuilder.setName(operation);
    methodBuilder.addAllArgs(encodeArgs());
    requestProtoBuilder.setMethod(methodBuilder.build());
    return Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
            requestProtoBuilder.build().toByteArray()));
  }

  private static Object[] decodeArgs(List<MethodArgument> arguments)
      throws InvalidProtocolBufferException {
    List<Object> args = new ArrayList<>();
    for (MethodArgument argument : arguments) {
      args.add(decodeArg(argument));
    }
    return args.toArray();
  }

  private static Object decodeArg(MethodArgument argument)
      throws InvalidProtocolBufferException {
    try {
      final Class<?> clazz = ReflectionUtil.getClass(argument.getType());
      if (GeneratedMessage.class.isAssignableFrom(clazz)) {
        return ReflectionUtil.getMethod(clazz, "parseFrom", byte[].class)
            .invoke(null, (Object) argument.getValue().toByteArray());
      } else if (Enum.class.isAssignableFrom(clazz)) {
        return ReflectionUtil.getMethod(clazz, "valueOf", int.class)
            .invoke(null, Ints.fromByteArray(
                argument.getValue().toByteArray()));
      } else if (Long.class.isAssignableFrom(clazz)) {
        return ((ByteBuffer) ByteBuffer.allocate(8)
            .put(argument.getValue().toByteArray())
            .flip()).getLong();
      } else if (List.class.isAssignableFrom(clazz)) {
        ListArgument listArgument = (ListArgument) ReflectionUtil
            .getMethod(ListArgument.class, "parseFrom", byte[].class)
            .invoke(null, (Object) argument.getValue().toByteArray());

        List<Object> list = new ArrayList<>();
        for (ByteString v : listArgument.getValueList()) {
          list.add(decodeArg(toMethodArgument(listArgument.getType(), v)));
        }

        return list;
      } else {
        throw new InvalidProtocolBufferException(argument.getType() +
            " is not a protobuf object!");
      }
    } catch (ClassNotFoundException | NoSuchMethodException |
        IllegalAccessException | InvocationTargetException ex) {
      throw new InvalidProtocolBufferException(argument.getType() +
          " cannot be decoded!" + ex.getMessage());
    }
  }
  /**
   * Decodes the request from Ratis Message.
   */
  public static SCMRatisRequest decode(Message message)
      throws InvalidProtocolBufferException {
    final SCMRatisRequestProto requestProto =
        SCMRatisRequestProto.parseFrom(message.getContent().toByteArray());
    final Method method = requestProto.getMethod();
    return new SCMRatisRequest(requestProto.getType(),
        method.getName(), decodeArgs(method.getArgsList()));
  }

}
