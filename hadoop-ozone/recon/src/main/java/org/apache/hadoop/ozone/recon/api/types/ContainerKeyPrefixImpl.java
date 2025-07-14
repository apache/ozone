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

import java.util.Objects;

/**
 * An implementation of both {@link ContainerKeyPrefix}
 * and {@link KeyPrefixContainer}.
 * <p>
 * This class is immutable.
 */
final class ContainerKeyPrefixImpl
    implements ContainerKeyPrefix, KeyPrefixContainer {

  private final long containerId;
  private final String keyPrefix;
  private final long keyVersion;

  private ContainerKeyPrefixImpl(long containerId, String keyPrefix,
      long keyVersion) {
    this.containerId = containerId;
    this.keyPrefix = keyPrefix;
    this.keyVersion = keyVersion;
  }

  static ContainerKeyPrefixImpl get(long containerId, String keyPrefix,
                                    long keyVersion) {
    return new ContainerKeyPrefixImpl(containerId, keyPrefix, keyVersion);
  }

  @Override
  public long getContainerId() {
    return containerId;
  }

  @Override
  public String getKeyPrefix() {
    return keyPrefix;
  }

  @Override
  public long getKeyVersion() {
    return keyVersion;
  }

  @Override
  public ContainerKeyPrefix toContainerKeyPrefix() {
    return this;
  }

  @Override
  public KeyPrefixContainer toKeyPrefixContainer() {
    return keyPrefix == null || keyPrefix.isEmpty() ? null : this;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(containerId).hashCode()
        + 13 * keyPrefix.hashCode()
        + 17 * Long.valueOf(keyVersion).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ContainerKeyPrefixImpl)) {
      return false;
    }
    final ContainerKeyPrefixImpl that = (ContainerKeyPrefixImpl) o;
    return this.containerId == that.containerId
        && Objects.equals(this.keyPrefix, that.keyPrefix)
        && this.keyVersion == that.keyVersion;
  }
}
