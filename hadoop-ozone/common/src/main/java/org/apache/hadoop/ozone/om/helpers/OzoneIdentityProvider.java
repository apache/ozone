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
package org.apache.hadoop.ozone.om.helpers;

import io.netty.util.internal.StringUtil;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.IdentityProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Ozone implementation of IdentityProvider used by
 * Hadoop DecayRpcScheduler.
 */
public class OzoneIdentityProvider implements IdentityProvider {

  public OzoneIdentityProvider() {
  }

  /**
   * If schedulable isn't instance of {@link Server.Call},
   * then trying to access getCallerContext() method, will
   * result in an exception.
   *
   * @param schedulable Schedulable object
   * @return string with the identity of the user
   */
  @Override
  public String makeIdentity(Schedulable schedulable) {
    UserGroupInformation ugi = schedulable.getUserGroupInformation();
    if (schedulable instanceof Server.Call) {
      CallerContext callerContext = schedulable.getCallerContext();
      if (!StringUtil.isNullOrEmpty(callerContext.getContext())) {
        return callerContext.getContext();
      }
    }
    return ugi.getShortUserName() == null ? null : ugi.getShortUserName();
  }
}
