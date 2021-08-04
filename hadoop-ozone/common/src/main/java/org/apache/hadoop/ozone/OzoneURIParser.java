/*
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
package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Written to parse Ozone URIs because java.net.URI rejects parsing String
 * when the path includes unescaped space, while encoding the String before
 * passing it to URL constructor can causes other issues, plus it slows things
 * down.
 */
public class OzoneURIParser {

  private final String scheme;
  private final String authority;
  private final String path;

  private static final String SCHEME_DELIM = "://";

  // Parser workflow:
  // 1. Locate "://", if found, we expect a scheme (o3fs / ofs);
  // 2. locate authority before the next '/', authority could be empty string;
  // 3. the rest, includes the leading delimiter, are treated as path.
  public OzoneURIParser(String path) {
    String p = path.trim();
    // Find the delimiter between scheme and authority
    int schemeEnd = p.indexOf(SCHEME_DELIM);
    if (schemeEnd < 0) {
      this.scheme = null;
      this.authority = "";
    } else {
      this.scheme = p.substring(0, schemeEnd).toLowerCase();
      // Trim scheme
      p = p.substring(schemeEnd + SCHEME_DELIM.length());
      // Find the delimiter before authority ends
      int authEnd = p.indexOf(OZONE_URI_DELIMITER);
      if (authEnd < 0) {
        this.authority = p;
        p = "";
      } else {
        this.authority = p.substring(0, authEnd);
        // Trim authority.
        p = p.substring(authEnd);
      }
    }
    if (!p.startsWith(OZONE_URI_DELIMITER)) {
      p = OZONE_URI_DELIMITER + p;
    }
    this.path = p;
  }

  public String getScheme() {
    return scheme;
  }

  public String getAuthority() {
    return authority;
  }

  public String getPath() {
    return path;
  }
}
