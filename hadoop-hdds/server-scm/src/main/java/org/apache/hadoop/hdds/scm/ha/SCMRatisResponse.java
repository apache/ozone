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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
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

  public static Message encode(final Class<?> returnType,
      final Type genericReturnType,
      final Object result) throws InvalidProtocolBufferException {

    if (result == null) {
      return Message.EMPTY;
    }

    final SCMRatisResponseProto.Builder responseBuilder =
        SCMRatisResponseProto.newBuilder();

    responseBuilder.setType(returnType.getName());

    if (genericReturnType instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) genericReturnType;
      Type rawType = pt.getRawType();
      Type[] actualTypes = pt.getActualTypeArguments();

      if (rawType instanceof Class<?>
          && List.class.isAssignableFrom((Class<?>) rawType)
          && actualTypes.length == 1
          && actualTypes[0] instanceof Class<?>) {
        responseBuilder.setGenericType(((Class<?>) actualTypes[0]).getName());
      }
    }

    responseBuilder.setValue(
        ScmCodecFactory.getCodec(genericReturnType).serialize(result));

    final SCMRatisResponseProto response = responseBuilder.build();
    return Message.valueOf(UnsafeByteOperations.unsafeWrap(
        response.toByteString().asReadOnlyByteBuffer()));
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
      final Class<?> clazz = ReflectionUtil.getClass(responseProto.getType());

      Type genericType = clazz;
      if (responseProto.hasGenericType()) {
        Class<?> genericClazz =
            ReflectionUtil.getClass(responseProto.getGenericType());
        genericType = new SimpleParameterizedType(clazz, genericClazz);
      }

      final Object decoded;
      if (genericType instanceof Class<?>) {
        decoded = ScmCodecFactory.getCodec((Class<?>) genericType)
            .deserialize((Class<?>) genericType, responseProto.getValue());
      } else if (genericType instanceof ParameterizedType) {
        ParameterizedType pt = (ParameterizedType) genericType;
        Type rawType = pt.getRawType();
        if (!(rawType instanceof Class<?>)) {
          throw new InvalidProtocolBufferException(
              "Unsupported raw type: " + rawType);
        }
        decoded = ScmCodecFactory.getCodec(genericType)
            .deserialize((Class<?>) rawType, responseProto.getValue());
      } else {
        throw new InvalidProtocolBufferException(
            "Unsupported generic type: " + genericType);
      }

      return new SCMRatisResponse(decoded);
    } catch (ClassNotFoundException e) {
      throw new InvalidProtocolBufferException(responseProto.getType() +
          " cannot be decoded!" + e.getMessage());
    }
  }

  /**
   * Simple implementation of ParameterizedType used to reconstruct
   * generic types such as List<T> during decode.
   */
  private static final class SimpleParameterizedType
      implements ParameterizedType {

    private final Type rawType;
    private final Type[] actualTypeArguments;

    private SimpleParameterizedType(Type rawType, Type... actualTypeArguments) {
      this.rawType = rawType;
      this.actualTypeArguments = actualTypeArguments.clone();
    }

    @Override
    public Type[] getActualTypeArguments() {
      return actualTypeArguments.clone();
    }

    @Override
    public Type getRawType() {
      return rawType;
    }

    @Override
    public Type getOwnerType() {
      return null;
    }

    @Override
    public String getTypeName() {
      StringBuilder sb = new StringBuilder();
      sb.append(((Class<?>) rawType).getName());
      if (actualTypeArguments.length > 0) {
        sb.append('<');
        for (int i = 0; i < actualTypeArguments.length; i++) {
          if (i > 0) {
            sb.append(',');
          }
          sb.append(actualTypeArguments[i].getTypeName());
        }
        sb.append('>');
      }
      return sb.toString();
    }
  }
}
