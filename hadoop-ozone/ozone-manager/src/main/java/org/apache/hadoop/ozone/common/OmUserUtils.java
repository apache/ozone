/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.common;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;

import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Utility class to store User related utilities.
 */
public final class OmUserUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmUserUtils.class);

  private OmUserUtils() { }

  /**
   * Get the users and groups that are S3 admin.
   * @param conf  Stores the Ozone configuration being used
   * @return an instance of {@link OzoneAdmins} containing the S3 admin users and groups
   */
  public static OzoneAdmins getS3Admins(OzoneConfiguration conf) {
    Collection<String> s3Admins;
    try {
      s3Admins = OzoneConfigUtil.getS3AdminsFromConfig(conf);
    } catch (IOException ie) {
      s3Admins = null;
    }
    Collection<String> s3AdminGroups =
        OzoneConfigUtil.getS3AdminsGroupsFromConfig(conf);
    if (LOG.isDebugEnabled()) {
      if (null == s3Admins) {
        LOG.debug("S3 Admins are not set in configuration");
      }
      if (null == s3AdminGroups) {
        LOG.debug("S3 Admin Groups are not set in configuration");
      }
    }
    return new OzoneAdmins(s3Admins, s3AdminGroups);
  }

  /**
   * Check if the provided user is a part of the S3 admins.
   * @param user Stores the user to verify
   * @param conf Stores the Ozone configuration being used
   * @return true if the provided user is an S3 admin else false
   */
  public static boolean isS3Admin(UserGroupInformation user,
                                  OzoneConfiguration conf) {
    OzoneAdmins s3Admins = getS3Admins(conf);
    return null != user && s3Admins.isAdmin(user);
  }

  /**
   * Check if the provided user is a part of the S3 admins.
   * @param user Stores the user to verify
   * @param s3Admins Stores the users and groups that are admins
   * @return true if the provided user is an S3 admin else false
   */
  public static boolean isS3Admin(UserGroupInformation user,
                                  OzoneAdmins s3Admins) {
    return null != user && s3Admins.isAdmin(user);
  }
}
