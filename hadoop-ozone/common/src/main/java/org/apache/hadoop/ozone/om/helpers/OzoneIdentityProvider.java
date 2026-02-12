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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_CALLER_CONTEXT_PREFIX;

import java.util.Objects;
import org.apache.hadoop.ipc_.CallerContext;
import org.apache.hadoop.ipc_.IdentityProvider;
import org.apache.hadoop.ipc_.Schedulable;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone implementation of IdentityProvider used by
 * Hadoop DecayRpcScheduler.
 */
public class OzoneIdentityProvider implements IdentityProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneIdentityProvider.class);

  public OzoneIdentityProvider() {
  }

  /**
   * If schedulable isn't instance of {@link org.apache.hadoop.ipc_.Server.Call},
   * then trying to access getCallerContext() method, will
   * result in an exception.
   *
   * @param schedulable Schedulable object
   * @return string with the identity of the user
   */
  @Override
  public String makeIdentity(Schedulable schedulable) {
    UserGroupInformation ugi = schedulable.getUserGroupInformation();
    try {
      CallerContext callerContext = schedulable.getCallerContext();
      // If the CallerContext is set by the OM then its value
      // should have the prefix "S3Auth:S3G|"
      // and it should be in format "S3Auth:S3G|username".
      // If the prefix is not present then its set by a user
      // and the value should be ignored.
      if (Objects.nonNull(callerContext) &&
          callerContext.getContext() != null &&
          callerContext.getContext()
              .startsWith(OM_S3_CALLER_CONTEXT_PREFIX)) {
        return callerContext.getContext()
            .substring(OM_S3_CALLER_CONTEXT_PREFIX.length());
      }
    } catch (UnsupportedOperationException ex) {
      LOG.error("Trying to access CallerContext from a Schedulable " +
          "implementation that's not instance of Server.Call");
    }
    return ugi.getShortUserName() == null ? null : ugi.getShortUserName();
  }
}
