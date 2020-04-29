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

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolMessageEnum;

import org.apache.ratis.protocol.Message;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.Method;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.MethodArgument;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.RequestType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocolProtos.SCMRatisRequestProto;


import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class SCMRatisRequest {

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

  public RequestType getType() {
    return type;
  }

  public String getOperation() {
    return operation;
  }

  public Object[] getArguments() {
    return arguments;
  }

  public Message encode() throws InvalidProtocolBufferException {
    final SCMRatisRequestProto.Builder requestProtoBuilder =
        SCMRatisRequestProto.newBuilder();
    requestProtoBuilder.setType(type);

    final Method.Builder methodBuilder = Method.newBuilder();
    methodBuilder.setName(operation);

    final List<MethodArgument> args = new ArrayList<>();
    for (Object argument : arguments) {
      final MethodArgument.Builder argBuilder = MethodArgument.newBuilder();
      argBuilder.setType(argument.getClass().getCanonicalName());
      if (argument instanceof GeneratedMessage) {
        argBuilder.setValue(((GeneratedMessage) argument).toByteString());
      } else if (argument instanceof ProtocolMessageEnum) {
        argBuilder.setValue(ByteString.copyFrom(BigInteger.valueOf(
            ((ProtocolMessageEnum) argument).getNumber()).toByteArray()));
      } else {
        throw new InvalidProtocolBufferException(argument.getClass() +
            " is not a protobuf object!");
      }
      args.add(argBuilder.build());
    }
    methodBuilder.addAllArgs(args);
    return Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
            requestProtoBuilder.build().toByteArray()));
  }

  public static SCMRatisRequest decode(Message message)
      throws InvalidProtocolBufferException {
    final SCMRatisRequestProto requestProto =
        SCMRatisRequestProto.parseFrom(message.getContent().toByteArray());
    final Method method = requestProto.getMethod();
    List<Object> args = new ArrayList<>();
    for (MethodArgument argument : method.getArgsList()) {
      try {
        final Class<?> clazz = HAUtil.getClass(argument.getType());
        if (GeneratedMessage.class.isAssignableFrom(clazz)) {
          args.add(HAUtil.getMethod(clazz, "parseFrom", byte[].class)
              .invoke(null, (Object) argument.getValue().toByteArray()));
        } else if (Enum.class.isAssignableFrom(clazz)) {
          args.add(HAUtil.getMethod(clazz, "valueOf", int.class)
              .invoke(null, new BigInteger(
                  argument.getValue().toByteArray()).intValue()));
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
    return new SCMRatisRequest(requestProto.getType(),
        method.getName(), args.toArray());
  }

}
