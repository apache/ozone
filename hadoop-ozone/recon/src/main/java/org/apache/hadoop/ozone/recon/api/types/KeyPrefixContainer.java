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

package org.apache.hadoop.ozone.recon.api.types;

/**
 * Class to encapsulate the Key information needed for the Recon container DB.
 * Currently, it is the whole key + key version and the containerId.
 * <p>
 * The implementations of this interface MUST be immutable.
 */
public interface KeyPrefixContainer {

  static KeyPrefixContainer get(String keyPrefix, long keyVersion,
      long containerId) {
    return ContainerKeyPrefixImpl.get(containerId, keyPrefix, keyVersion);
  }

  static KeyPrefixContainer get(String keyPrefix, long keyVersion) {
    return get(keyPrefix, keyVersion, -1);
  }

  static KeyPrefixContainer get(String keyPrefix) {
    return get(keyPrefix, -1, -1);
  }

  long getContainerId();

  String getKeyPrefix();

  long getKeyVersion();

  ContainerKeyPrefix toContainerKeyPrefix();
}
