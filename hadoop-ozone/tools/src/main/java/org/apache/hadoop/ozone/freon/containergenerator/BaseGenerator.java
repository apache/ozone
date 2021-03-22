/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.freon.containergenerator;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.freon.BaseFreonGenerator;

import picocli.CommandLine.Option;

public abstract class BaseGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  @Option(names = {"-u", "--user"},
      description = "Owner of the files",
      defaultValue = "ozone")
  private static String userId;

  @Option(names = {"--key-size"},
      description = "Size of the generated keys (in bytes) in each of the "
          + "containers",
      defaultValue = "16000000")
  private int keySize;

  @Option(names = {"--size"},
      description = "Size of generated containers (default is defined by "
          + "ozone.scm.container.size)")
  private long containerSize;

  @Option(names = {"--from"},
      description = "First container index to use",
      defaultValue = "1")
  private long containerIdOffset;

  public static String getUserId() {
    return userId;
  }

  public int getKeysPerContainer(ConfigurationSource conf) {
    return (int) (getContainerSize(conf) / getKeySize());
  }

  public long getContainerIdOffset() {
    return containerIdOffset;
  }

  public long getContainerSize(ConfigurationSource conf) {
    if (containerSize == 0) {
      final Double defaultContainerSize =
          conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
              ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
              StorageUnit.BYTES);
      return defaultContainerSize.longValue();
    } else {
      return containerSize;
    }
  }

  public int getKeySize() {
    return keySize;
  }
}
