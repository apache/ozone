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

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A dummy codec that serializes a ByteString object to ByteString.
 */
public class ByteStringCodec implements Codec {

  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    if (object instanceof ByteString) {
      return (ByteString) object;
    }
    if (object instanceof com.google.protobuf.ByteString) {
      com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) object;
      return org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
          .unsafeWrap(bs.asReadOnlyByteBuffer());
    }
    return (ByteString) object;
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    if (type == ByteString.class || ByteString.class.isAssignableFrom(type)) {
      return value;
    }
    if (type == com.google.protobuf.ByteString.class
        || com.google.protobuf.ByteString.class.isAssignableFrom(type)) {
      return com.google.protobuf.UnsafeByteOperations
          .unsafeWrap(value.asReadOnlyByteBuffer());
    }
    return value;
  }
}
