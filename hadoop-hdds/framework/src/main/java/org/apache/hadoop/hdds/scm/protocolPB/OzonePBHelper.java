/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.protocolPB;

import com.google.protobuf.ByteString;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class for converting protobuf objects.
 */
public final class OzonePBHelper {

  private OzonePBHelper() {
    // no instances
  }

  // Borrowed from ProtobufHelper.java in hadoop-common involving protobuf
  // messages to avoid breakage due to shading of protobuf in Hadoop-3.3+.
  /**
   * Map used to cache fixed strings to ByteStrings. Since there is no
   * automatic expiration policy, only use this for strings from a fixed, small
   * set.
   * <p/>
   * This map should not be accessed directly. Used the getFixedByteString
   * methods instead.
   */
  private static final ConcurrentHashMap<Object, ByteString>
      FIXED_BYTESTRING_CACHE = new ConcurrentHashMap<>();

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   *
   * @param key string
   */
  public static ByteString getFixedByteString(Text key) {
    return FIXED_BYTESTRING_CACHE.computeIfAbsent(key,
        k -> ByteString.copyFromUtf8(k.toString()));
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static Token<? extends TokenIdentifier> tokenFromProto(
      TokenProto tokenProto) {
    return new Token<>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(),
        new Text(tokenProto.getKind()),
        new Text(tokenProto.getService()));
  }

  public static TokenProto protoFromToken(Token<?> token) {
    TokenProto.Builder builder = TokenProto.newBuilder().
        setIdentifier(getByteString(token.getIdentifier())).
        setPassword(getByteString(token.getPassword())).
        setKindBytes(getFixedByteString(token.getKind())).
        setServiceBytes(getFixedByteString(token.getService()));
    return builder.build();
  }
}
