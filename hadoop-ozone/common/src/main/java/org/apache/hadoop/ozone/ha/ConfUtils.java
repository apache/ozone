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
package org.apache.hadoop.ozone.ha;

import com.google.common.base.Joiner;
import org.apache.hadoop.net.NetUtils;

import java.net.InetSocketAddress;

public final class ConfUtils {

  private ConfUtils() {

  }

  /**
   * Add non empty and non null suffix to a key.
   */
  public static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN.
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  /**
   * Concatenate list of suffix strings '.' separated.
   */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Match input address to local address.
   * Return true if it matches, false otherwsie.
   */
  public static boolean isAddressLocal(InetSocketAddress addr) {
    return NetUtils.isLocalAddress(addr.getAddress());
  }
}
