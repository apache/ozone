/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.codec;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Codec to encode OmDBTenantState as byte array.
 */
public class OmDBTenantStateCodec implements Codec<OmDBTenantState> {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmDBTenantStateCodec.class);

  @Override
  public byte[] toPersistedFormat(OmDBTenantState object) throws IOException {
    checkNotNull(object, "Null object can't be converted to byte array.");
    return object.getProtobuf().toByteArray();
  }

  @Override
  public OmDBTenantState fromPersistedFormat(byte[] rawData)
      throws IOException {
    checkNotNull(rawData, "Null byte array can't be converted to " +
        "real object.");
    return OmDBTenantState.getFromProtobuf(
        OzoneManagerProtocolProtos.TenantState.parseFrom(rawData));
  }

  @Override
  public OmDBTenantState copyObject(OmDBTenantState object) {
    // Note: Not really a "copy". from OMTransactionInfoCodec
    return object;
  }
}
