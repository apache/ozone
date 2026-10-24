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

package org.apache.hadoop.ozone.upgrade;

import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.common.StorageInfo;

/**
 * Upgrade related test utility methods.
 */
public final class UpgradeTestUtils {
  private UpgradeTestUtils() { }

  /**
   * Creates a VERSION file for the specified node type under the directory
   * {@code parentDir}.
   */
  public static File createVersionFile(File parentDir,
      HddsProtos.NodeType nodeType, int mlv) throws IOException {
    return createVersionFile(parentDir, nodeType, mlv, null);
  }

  public static File createVersionFile(File parentDir,
      HddsProtos.NodeType nodeType, int mlv,
      @Nullable Properties properties) throws IOException {

    final String versionFileName = "VERSION";

    StorageInfo info = new StorageInfo(
        nodeType,
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        mlv);

    if (properties != null) {
      properties.forEach((key, value) ->
          info.setProperty((String) key, (String) value));
    }

    File versionFile = new File(parentDir, versionFileName);
    info.writeTo(versionFile);

    return versionFile;
  }
}
