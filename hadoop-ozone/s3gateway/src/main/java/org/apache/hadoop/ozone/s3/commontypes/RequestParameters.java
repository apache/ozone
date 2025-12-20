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

import java.util.Collection;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;

/** Allow looking up query parameters as primitive types. */
public final class RequestParameters implements MutableConfigurationSource {

  private final MultivaluedMap<String, String> params;

  public static RequestParameters of(MultivaluedMap<String, String> params) {
    return new RequestParameters(params);
  }

  private RequestParameters(MultivaluedMap<String, String> params) {
    this.params = params;
  }

  @Override
  public String get(String key) {
    return params.getFirst(key);
  }

  @Override
  public Collection<String> getConfigKeys() {
    return params.keySet();
  }

  @Override
  public char[] getPassword(String key) {
    throw new UnsupportedOperationException();
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
