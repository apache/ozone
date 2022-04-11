/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.util;

import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilities.
 */
public final class S3Utils {

  public static String urlDecode(String str)
      throws UnsupportedEncodingException {
    return URLDecoder.decode(str, UTF_8.name());
  }

  public static String urlEncode(String str)
      throws UnsupportedEncodingException {
    return URLEncoder.encode(str, UTF_8.name());
  }

  public static Map<String, String> genAuditParam(String... strs) {
    if (strs.length % 2 == 1) {
      throw new IllegalArgumentException("Unexpected number of parameters: "
          + strs.length);
    }
    Map<String, String> auditParams = new TreeMap<>();
    for (int i = 0; i < strs.length; i++) {
      if (StringUtils.isEmpty(strs[i]) || StringUtils.isEmpty(strs[i + 1])) {
        ++i;
        continue;
      }
      auditParams.put(strs[i], strs[++i]);
    }
    return auditParams;
  }

  private S3Utils() {
    // no instances
  }
}
