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

package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Defines conditions for which validators can be assigned to.
 *
 * These conditions describe a situation where special request handling might
 * be necessary. In these cases we do not override the actual request handling
 * code, but based on certain request properties we might reject a request
 * early, or we might modify the request, or the response received/sent from/to
 * the client.
 */
public enum ValidationCondition {
  /**
   * Classifies validations that has to run after an upgrade until the cluster
   * is in a pre-finalized state.
   */
  CLUSTER_NEEDS_FINALIZATION {
    @Override
    public boolean shouldApply(OMRequest req, ValidationContext ctx) {
      return ctx.versionManager().needsFinalization();
    }
  },

  /**
   * Classifies validations that has to run, when the client uses an older
   * protocol version than the server.
   */
  OLDER_CLIENT_REQUESTS {
    @Override
    public boolean shouldApply(OMRequest req, ValidationContext ctx) {
      return req.getVersion() < ClientVersion.CURRENT.serialize();
    }
  };

  public abstract boolean shouldApply(OMRequest req, ValidationContext ctx);
}
