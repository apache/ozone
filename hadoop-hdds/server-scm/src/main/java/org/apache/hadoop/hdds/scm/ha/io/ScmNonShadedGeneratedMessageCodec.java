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

package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * {@link ScmCodec} implementation for non-shaded
 * {@link com.google.protobuf.Message} objects.
 */
public class ScmNonShadedGeneratedMessageCodec<T extends Message> implements ScmCodec<T> {

  private final Parser<T> parser;

  public ScmNonShadedGeneratedMessageCodec(Parser<T> parser) {
    this.parser = parser;
  }

  @Override
  public ByteString serialize(T object) throws InvalidProtocolBufferException {
    return UnsafeByteOperations.unsafeWrap(object.toByteString().asReadOnlyByteBuffer());
  }

  @Override
  public T deserialize(Class<?> type, ByteString value) throws InvalidProtocolBufferException {
    try {
      return parser.parseFrom(value.toByteArray());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new InvalidProtocolBufferException("Message cannot be decoded", e);
    }
  }
}
