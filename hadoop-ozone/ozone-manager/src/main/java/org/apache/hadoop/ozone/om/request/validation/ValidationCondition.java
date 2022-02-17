/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.ClientVersions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

import java.util.function.Predicate;

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
  CLUSTER_NEEDS_FINALIZATION(r -> true,
      ctx -> ctx.versionManager().needsFinalization()),
  /**
   * Classifies validations that has to run, when the client uses an older
   * protocol version than the server.
   */
  OLDER_CLIENT_REQUESTS(r -> r.getVersion() < ClientVersions.CURRENT_VERSION,
      ctx -> true),
  /**
   * Classifies validations that has to run, when the client uses a newer
   * protocol version than the server.
   */
  NEWER_CLIENT_REQUESTS(r -> r.getVersion() > ClientVersions.CURRENT_VERSION,
      ctx -> true),
  /**
   * Classifies validations that has to run for every request.
   * If you plan to use this, please justify why the validation code should not
   * be part of the actual request handling code.
   */
  UNCONDITIONAL(r -> true, ctx -> true);

  private final Predicate<OMRequest> shouldApplyTo;
  private final Predicate<ValidationContext> shouldApplyIn;

  ValidationCondition(
      Predicate<OMRequest> shouldApplyTo,
      Predicate<ValidationContext> shouldApplyIn) {
    this.shouldApplyTo = shouldApplyTo;
    this.shouldApplyIn = shouldApplyIn;
  }

  boolean shouldApply(OMRequest req, ValidationContext ctx) {
    return shouldApplyTo.test(req) && shouldApplyIn.test(ctx);
  }
}
