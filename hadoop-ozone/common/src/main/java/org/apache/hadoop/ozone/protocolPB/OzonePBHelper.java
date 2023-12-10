/**
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
package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import static org.apache.hadoop.hdds.scm.protocolPB.OzonePBHelper.getByteString;
import static org.apache.hadoop.hdds.scm.protocolPB.OzonePBHelper.getFixedByteString;

/**
 * Helper class for converting protobuf objects.
 */
public final class OzonePBHelper {
  private OzonePBHelper() {
    /** Hidden constructor */
  }

  public static Token<? extends TokenIdentifier> tokenFromProto(
      TokenProto tokenProto) {
    return new Token<>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(),
        new Text(tokenProto.getKind()),
        new Text(tokenProto.getService()));
  }

  public static TokenProto protoFromToken(Token<?> tok) {
    return TokenProto.newBuilder()
        .setIdentifier(getByteString(tok.getIdentifier()))
        .setPassword(getByteString(tok.getPassword()))
        .setKindBytes(getFixedByteString(tok.getKind()))
        .setServiceBytes(getByteString(tok.getService().getBytes()))
        .build();
  }
}
