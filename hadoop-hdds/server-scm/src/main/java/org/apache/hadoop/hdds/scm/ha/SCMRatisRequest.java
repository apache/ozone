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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hdds.scm.ha.io.CodecFactory;
import org.apache.ratis.protocol.Message;

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
  private final Class<?>[] parameterTypes;

  private SCMRatisRequest(final RequestType type, final String operation,
      final Class<?>[] parameterTypes, final Object... arguments) {
    this.type = type;
    this.operation = operation;
    this.parameterTypes = parameterTypes;
    this.arguments = arguments;
  }

  public static SCMRatisRequest of(final RequestType type,
      final String operation,
      final Class<?>[] parameterTypes,
      final Object... arguments) {
    Preconditions.checkState(parameterTypes.length == arguments.length);
    return new SCMRatisRequest(type, operation, parameterTypes, arguments);
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

  public Class<?>[] getParameterTypes() {
    return parameterTypes.clone();
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

    final List<MethodArgument> args = new ArrayList<>();

    int paramCounter = 0;
    for (Object argument : arguments) {
      final MethodArgument.Builder argBuilder = MethodArgument.newBuilder();
      // Set actual method parameter type, not actual argument type.
      // This is done to avoid MethodNotFoundException in case if argument is
      // subclass type, where as method is defined with super class type.
      argBuilder.setType(parameterTypes[paramCounter++].getName());
      argBuilder.setValue(CodecFactory.getCodec(argument.getClass())
          .serialize(argument));
      args.add(argBuilder.build());
    }
    methodBuilder.addAllArgs(args);
    requestProtoBuilder.setMethod(methodBuilder.build());
    return Message.valueOf(
        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
            requestProtoBuilder.build().toByteArray()));
  }

  /**
   * Decodes the request from Ratis Message.
   */
  public static SCMRatisRequest decode(Message message)
      throws InvalidProtocolBufferException {
    final SCMRatisRequestProto requestProto =
        SCMRatisRequestProto.parseFrom(message.getContent().toByteArray());
    final Method method = requestProto.getMethod();
    List<Object> args = new ArrayList<>();
    Class<?>[] parameterTypes = new Class[method.getArgsCount()];
    int paramCounter = 0;
    for (MethodArgument argument : method.getArgsList()) {
      try {
        final Class<?> clazz = ReflectionUtil.getClass(argument.getType());
        parameterTypes[paramCounter++] = clazz;
        args.add(CodecFactory.getCodec(clazz)
            .deserialize(clazz, argument.getValue()));
      } catch (ClassNotFoundException ex) {
        throw new InvalidProtocolBufferException(argument.getType() +
            " cannot be decoded!" + ex.getMessage());
      }
    }
    return new SCMRatisRequest(requestProto.getType(),
        method.getName(), parameterTypes, args.toArray());
  }

}
