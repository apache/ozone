/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.token;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.DeleteBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.DeleteChunk;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.DELETE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;


/**
 * Verify token and return a UGI with token if authenticated.
 */
public class BlockTokenVerifier extends
    ShortLivedTokenVerifier<OzoneBlockTokenIdentifier> {

  public static String getTokenService(BlockID blockID) {
    return getTokenService(blockID.getContainerBlockID());
  }

  public static String getTokenService(ContainerBlockID blockID) {
    return String.valueOf(blockID);
  }

  public BlockTokenVerifier(SecurityConfig conf, CertificateClient caClient) {
    super(conf, caClient);
  }

  @Override
  protected boolean isTokenRequired(ContainerProtos.Type cmdType) {
    return getConf().isBlockTokenEnabled() &&
        HddsUtils.requireBlockToken(cmdType);
  }

  @Override
  protected OzoneBlockTokenIdentifier createTokenIdentifier() {
    return new OzoneBlockTokenIdentifier();
  }

  @Override
  protected Object getService(ContainerCommandRequestProtoOrBuilder cmd) {
    BlockID blockID = HddsUtils.getBlockID(cmd);
    Preconditions.checkNotNull(blockID,
        "no blockID in %s command", cmd.getCmdType());
    return getTokenService(blockID);
  }

  @Override
  protected void verify(OzoneBlockTokenIdentifier tokenId,
      ContainerCommandRequestProtoOrBuilder cmd) throws SCMSecurityException {

    HddsProtos.BlockTokenSecretProto.AccessModeProto accessMode;
    if (HddsUtils.isReadOnly(cmd)) {
      accessMode = READ;
    } else if (cmd.getCmdType() == DeleteBlock ||
        cmd.getCmdType() == DeleteChunk) {
      accessMode = DELETE;
    } else {
      accessMode = WRITE;
    }
    if (!tokenId.getAccessModes().contains(accessMode)) {
      throw new BlockTokenException("Block token with " + tokenId.getService()
          + " doesn't have " + accessMode + " permission");
    }
  }
}
