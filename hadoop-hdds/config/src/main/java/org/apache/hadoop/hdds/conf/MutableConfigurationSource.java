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

package org.apache.hadoop.hdds.conf;

import java.io.IOException;
import java.util.Collection;

/**
 * Configuration that can be both read and written.
 */
public interface MutableConfigurationSource
    extends ConfigurationSource, ConfigurationTarget {

  /**
   * Sets {@code value} for {@code key} only if the key is not already set.
   * Default implementation treats any non-null {@link #get(String)} result as set.
   * {@code OzoneConfiguration} (in hdds-common) overrides this to allow
   * overriding values that come only from default resources.
   */
  default void setIfUnset(String key, String value) {
    if (get(key) == null) {
      set(key, value);
    }
  }

  /**
   * Creates a wrapper config that changes {@link #set(String, String)} to
   * {@link #setIfUnset(String, String)}. In other words, value is stored only if
   * no existing value is explicitly set.
   */
  static MutableConfigurationSource ifUnsetWrapper(MutableConfigurationSource wrapped) {
    return new IfUnsetWrapper(wrapped);
  }

  /**
   * Delegates all calls to another configuration object, but changes semantics of
   * {@link #set(String, String)} to {@link #setIfUnset(String, String)}.
   */
  class IfUnsetWrapper implements MutableConfigurationSource {

    private final MutableConfigurationSource wrapped;

    private IfUnsetWrapper(MutableConfigurationSource wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public String get(String key) {
      return wrapped.get(key);
    }

    @Override
    public Collection<String> getConfigKeys() {
      return wrapped.getConfigKeys();
    }

    @Override
    public char[] getPassword(String key) throws IOException {
      return wrapped.getPassword(key);
    }

    @Override
    public void set(String key, String value) {
      wrapped.setIfUnset(key, value);
    }

    @Override
    public void setIfUnset(String key, String value) {
      wrapped.setIfUnset(key, value);
    }
  }
}
