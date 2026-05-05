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

import jakarta.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * A converter to encode string if needed.
 */
public final class EncodingTypeObject {
  private final String encodingType;
  private final String name;

  private EncodingTypeObject(String name, @Nullable String encodingType) {
    this.encodingType = encodingType;
    this.name = name;
  }

  @Nullable public String getEncodingType() {
    return encodingType;
  }

  public String getName() {
    return name;
  }

  /**
   * Create a EncodingTypeObject Object, if the parameter name is null.
   * @return If name is null return null else return a EncodingTypeObject object
   */
  @Nullable public static EncodingTypeObject createNullable(
      @Nullable String name, @Nullable String encodingType) {
    if (StringUtils.isEmpty(name)) {
      return null;
    }
    return new EncodingTypeObject(name, encodingType);
  }
}
