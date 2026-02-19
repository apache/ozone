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

import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * A codec for {@link ManagedSecretKey} objects.
 */
public class ScmManagedSecretKeyCodec implements ScmCodec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    ManagedSecretKey secretKey = (ManagedSecretKey) object;
    return UnsafeByteOperations.unsafeWrap(
        secretKey.toProtobuf().toByteString().asReadOnlyByteBuffer());
  }

  @Override
  public Object deserialize(Class<?> type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      SCMSecretKeyProtocolProtos.ManagedSecretKey message =
          SCMSecretKeyProtocolProtos.ManagedSecretKey.parseFrom(
              value.asReadOnlyByteBuffer());
      return ManagedSecretKey.fromProtobuf(message);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new InvalidProtocolBufferException("Failed to deserialize value for " + type, e);
    }
  }
}
