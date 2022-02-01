/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.ozone.ClientVersions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMManagerClientVersionsValidations is a collection of all the validations
 * that need to be run to maintain compatibility with varying versions of
 * client.
 */
public final class OMManagerClientVersionValidations {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMManagerClientVersionValidations.class);
  private OMManagerClientVersionValidations() {

  }

  public static void validateClientVersionPostProcess(
      OzoneManagerProtocolProtos.Type lookupKey,
      OzoneManagerProtocolProtos.OMRequest request,
      OzoneManagerProtocolProtos.InfoBucketResponse infoBucketResponse) {
    // ToDo: Add EC specific rejection of request based on key info.
    if (request.hasVersion()
        &&
        ClientVersions.isClientCompatible(
            ClientVersions.CLIENT_EC_CAPABLE,
            request.getVersion())){
      return;
    }
    // TODO: Update the response and log message
    LOG.debug("Request rejected as client version is not EC Compatible: {}",
        request);
  }
  public static void validateClientVersionPostProcess(
      OzoneManagerProtocolProtos.Type lookupKey,
      OzoneManagerProtocolProtos.OMRequest request,
      OzoneManagerProtocolProtos.LookupKeyResponse lookupKeyResponse) {
    // ToDo: Add FSO specific rejection of request based on bucket info.
    if (request.hasVersion()
        &&
        ClientVersions.isClientCompatible(
            ClientVersions.CLIENT_FSO_CAPABLE,
            request.getVersion())){
      return;
    }
    // TODO: Update the response and log message
    LOG.debug("Request rejected as client version is not FSO Compatible: {}",
        request);
  }

}
