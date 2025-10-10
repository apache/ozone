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
package org.apache.hadoop.ipc_;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol which is used to refresh arbitrary things at runtime.
 */
@KerberosInfo(
    serverPrincipal=CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
public interface GenericRefreshProtocol {
  /**
   * Version 1: Initial version.
   */
  public static final long versionID = 1L;

  /**
   * Refresh the resource based on identity passed in.
   *
   * @param identifier input identifier.
   * @param args input args.
   * @throws IOException raised on errors performing I/O.
   * @return Collection RefreshResponse.
   */
  @Idempotent
  Collection<RefreshResponse> refresh(String identifier, String[] args)
      throws IOException;
}
