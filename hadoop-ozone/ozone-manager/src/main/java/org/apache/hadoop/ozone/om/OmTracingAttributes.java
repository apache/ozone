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

package org.apache.hadoop.ozone.om;

/**
 * Span attribute keys used by OM.
 * Fields that are also in the audit log (volume, bucket, key, ...) use the audit key unchanged instead.
 */
public final class OmTracingAttributes {

  /** Status or result code of a failed request, for example KEY_NOT_FOUND. */
  public static final String STATUS = "ozone.status";

  /** {@code OMRequest.Type} of an OM request, for example {@code CreateKey}. */
  public static final String OM_CMD_TYPE = "om.cmd_type";

  /** Client ID sent with an OM request. */
  public static final String OM_CLIENT_ID = "om.client_id";

  /** Hadoop RPC call ID of an OM request. */
  public static final String OM_CALL_ID = "om.call_id";

  /** {@code false} when OM returned status OK without success, for example a no-op ACL or owner change. */
  public static final String OM_SUCCESS = "om.success";

  /** Raft role of the OM applying a transaction: leader or follower. */
  public static final String OM_ROLE = "om.role";

  private OmTracingAttributes() {
  }
}
