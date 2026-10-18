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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.ratis.util.Preconditions;

/**
 * Utility methods for Ozone file system tests.
 */
public final class OzoneFileSystemTestUtils {

  private OzoneFileSystemTestUtils() {
    // no instances
  }

  /**
   * Set file system listing page size.  Also disable the file system cache to
   * ensure new {@link FileSystem} instance reflects the updated page size.
   */
  public static void setPageSize(ConfigurationTarget conf, int pageSize) {
    Preconditions.assertTrue(pageSize > 0, () -> "pageSize=" + pageSize + " <= 0");
    conf.setInt(OZONE_FS_LISTING_PAGE_SIZE, pageSize);
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", OZONE_URI_SCHEME), true);
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", OZONE_OFS_URI_SCHEME), true);
  }
}
