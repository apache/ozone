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

package org.apache.hadoop.ozone.s3.commontypes;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/** Allow looking up query parameters as primitive types. */
public interface RequestParameters {

  String get(String key);

  static MultivaluedMapImpl of(MultivaluedMap<String, String> params) {
    return new MultivaluedMapImpl(params);
  }

  default String get(String key, String defaultValue) {
    final String value = get(key);
    return value != null ? value : defaultValue;
  }

  default int getInt(String key, int defaultValue) {
    final String value = get(key);
    if (value == null) {
      return defaultValue;
    }

    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw translateException(e);
    }
  }

  default WebApplicationException translateException(RuntimeException e) {
    return new WebApplicationException(e.getMessage(), S3ErrorTable.INVALID_ARGUMENT.getHttpCode());
  }

  /** Additional methods for tests. */
  interface Mutable extends RequestParameters {

    void set(String key, String value);

    void unset(String key);

    default void setInt(String key, int value) {
      set(key, String.valueOf(value));
    }
  }

  /** Mutable implementation based on {@link MultivaluedMap}. */
  final class MultivaluedMapImpl implements Mutable {
    private final MultivaluedMap<String, String> params;

    private MultivaluedMapImpl(MultivaluedMap<String, String> params) {
      this.params = params;
    }

    @Override
    public String get(String key) {
      return params.getFirst(key);
    }

    @Override
    public void set(String key, String value) {
      params.putSingle(key, value);
    }

    @Override
    public void unset(String key) {
      params.remove(key);
    }
  }
}
