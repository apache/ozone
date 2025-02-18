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

/**
 * "Key" element to the Version specific instance factory. Currently it has 2
 * dimensions -&gt; a 'key' string and a version. This is to support a factory
 * which returns an instance for a given "key" and "version".
 */
public class VersionFactoryKey {
  private String key;
  private Integer version;

  public VersionFactoryKey(String key, Integer version) {
    this.key = key;
    this.version = version;
  }

  public String getKey() {
    return key;
  }

  public Integer getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " : [" + key + ", "
        + version  + "]";
  }

  /**
   * Builder for above key.
   */
  public static class Builder {
    private String key;
    private Integer version;

    public Builder key(String k) {
      this.key = k;
      return this;
    }

    public Builder version(Integer v) {
      this.version = v;
      return this;
    }

    public VersionFactoryKey build() {
      return new VersionFactoryKey(key, version);
    }
  }
}
