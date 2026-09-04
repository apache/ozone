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

package org.apache.hadoop.hdds.server.http.servletbridge;

/**
 * Value-type conversions between the {@code javax.servlet} and
 * {@code jakarta.servlet} namespaces used by the servlet bridge.
 */
final class ServletBridgeUtils {

  private ServletBridgeUtils() {
  }

  /** Convert a jakarta cookie to its javax equivalent. */
  static javax.servlet.http.Cookie toJavax(jakarta.servlet.http.Cookie source) {
    javax.servlet.http.Cookie result =
        new javax.servlet.http.Cookie(source.getName(), source.getValue());
    if (source.getDomain() != null) {
      result.setDomain(source.getDomain());
    }
    if (source.getPath() != null) {
      result.setPath(source.getPath());
    }
    result.setMaxAge(source.getMaxAge());
    result.setSecure(source.getSecure());
    result.setHttpOnly(source.isHttpOnly());
    return result;
  }

  /** Convert a javax cookie to its jakarta equivalent. */
  static jakarta.servlet.http.Cookie toJakarta(javax.servlet.http.Cookie source) {
    jakarta.servlet.http.Cookie result =
        new jakarta.servlet.http.Cookie(source.getName(), source.getValue());
    if (source.getDomain() != null) {
      result.setDomain(source.getDomain());
    }
    if (source.getPath() != null) {
      result.setPath(source.getPath());
    }
    result.setMaxAge(source.getMaxAge());
    result.setSecure(source.getSecure());
    result.setHttpOnly(source.isHttpOnly());
    return result;
  }
}
