/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.HashMap;
import java.util.Map;

/**
 * Static StorageClassRegistry for POC purpose.
 */
public class StaticStorageClassRegistry implements StorageClassRegistry {

  // TODO(baoloongmao): rename this to REDUCED_REDUNDANCY to
  //  keep consistent with s3
  public static final StorageClass REDUCED_REDUNDANCY = new StorageClass() {

    @Override
    public OpenStateConfiguration getOpenStateConfiguration() {
      return new OpenStateConfiguration(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE);
    }

    @Override
    public ClosedStateConfiguration getClosedStateConfiguration() {
      return new ClosedStateConfiguration(1);
    }

    @Override
    public String getName() {
      return "REDUCED_REDUNDANCY";
    }
  };

  public static final StorageClass STANDARD = new StorageClass() {

    @Override
    public OpenStateConfiguration getOpenStateConfiguration() {
      return new OpenStateConfiguration(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
    }

    @Override
    public ClosedStateConfiguration getClosedStateConfiguration() {
      return new ClosedStateConfiguration(3);
    }

    @Override
    public String getName() {
      return "STANDARD";
    }
  };

  public static final StorageClass LEGACY = new StorageClass() {

    @Override
    public OpenStateConfiguration getOpenStateConfiguration() {
      return new OpenStateConfiguration(
          HddsProtos.ReplicationType.STAND_ALONE,
          HddsProtos.ReplicationFactor.ONE);
    }

    @Override
    public ClosedStateConfiguration getClosedStateConfiguration() {
      return new ClosedStateConfiguration(1);
    }

    @Override
    public String getName() {
      return "LEGACY";
    }
  };

  private static final Map<String, StorageClass> STRING_STORAGE_CLASS_MAP =
      new HashMap<>();
  static {
    STRING_STORAGE_CLASS_MAP.put(STANDARD.getName(), STANDARD);
    STRING_STORAGE_CLASS_MAP.put(REDUCED_REDUNDANCY.getName(),
        REDUCED_REDUNDANCY);
    STRING_STORAGE_CLASS_MAP.put(LEGACY.getName(), LEGACY);
  }

  @Override
  public StorageClass getStorageClass(String name) {
    StorageClass storageClass = STRING_STORAGE_CLASS_MAP.get(name);
    if (storageClass == null) {
      final StringBuilder systemStorageClassesStr = new StringBuilder();
      STRING_STORAGE_CLASS_MAP.values().forEach(
          v -> systemStorageClassesStr.append(" " + v.getName()));
      throw new UnsupportedOperationException("Storage class " + name
          + " is not supported. Use" + systemStorageClassesStr);
    }
    return storageClass;
  }
}
