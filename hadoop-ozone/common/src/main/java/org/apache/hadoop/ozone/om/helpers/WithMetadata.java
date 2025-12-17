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

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import net.jcip.annotations.Immutable;

/**
 * Mixin class to handle custom metadata.
 */
@Immutable
public abstract class WithMetadata {

  private final ImmutableMap<String, String> metadata;

  protected WithMetadata() {
    metadata = ImmutableMap.of();
  }

  protected WithMetadata(Builder b) {
    metadata = b.metadata.build();
  }

  protected WithMetadata(WithMetadata other) {
    metadata = other.getMetadata();
  }

  /**
   * Custom key value metadata.
   */
  public final ImmutableMap<String, String> getMetadata() {
    return metadata;
  }

  /** Builder for {@link WithMetadata}. */
  public static class Builder {
    private final MapBuilder<String, String> metadata;

    protected Builder() {
      metadata = MapBuilder.empty();
    }

    protected Builder(WithMetadata obj) {
      metadata = MapBuilder.of(obj.getMetadata());
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      if (additionalMetadata != null) {
        metadata.putAll(additionalMetadata);
      }
      return this;
    }

    public Builder setMetadata(Map<String, String> map) {
      metadata.set(map);
      return this;
    }

    public MapBuilder<String, String> metadata() {
      return metadata;
    }
  }

}
