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
package org.apache.hadoop.ozone.om.request.s3.tenant;

import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.slf4j.Logger;

/**
 * Common helper methods for Tenant Requests.
 */
public final class TenantRequestHelper {

  private TenantRequestHelper() {
  }

  /**
   * Wrapper method that attempts to release the lock when actually held.
   */
  static void unlockWriteAfterRequest(
      OMMultiTenantManager multiTenantManager, Logger logger, long lockStamp) {

    if (lockStamp != 0L) {
      // Release write lock to authorizer (Ranger)
      multiTenantManager.getAuthorizerLock().unlockWriteInOMRequest(lockStamp);
    } else {
      // Should never reach this statement. Even when OM crashes the lock stamp
      // passed from preExecute is still recoverable from Raft log.
      logger.warn("Authorizer lock is not held during the operation!");
    }
  }
}
