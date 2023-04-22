/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import java.util.Objects;

/**
 * Class to encapsulate the Key information needed for the Recon container DB.
 * Currently, it is the whole key + key version and the containerId.
 */
public class KeyPrefixContainer {

  private String keyPrefix;
  private long keyVersion = -1;
  private long containerId = -1;

  public KeyPrefixContainer(String keyPrefix, long keyVersion) {
    this.keyPrefix = keyPrefix;
    this.keyVersion = keyVersion;
  }

  public KeyPrefixContainer(String keyPrefix, long keyVersion,
      long containerId) {
    this.keyPrefix = keyPrefix;
    this.keyVersion = keyVersion;
    this.containerId = containerId;
  }

  public KeyPrefixContainer(ContainerKeyPrefix containerKeyPrefix) {
    this.keyPrefix = containerKeyPrefix.getKeyPrefix();
    this.keyVersion = containerKeyPrefix.getKeyVersion();
    this.containerId = containerKeyPrefix.getContainerId();
  }

  public KeyPrefixContainer(String keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }

  public void setKeyPrefix(String keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public long getKeyVersion() {
    return keyVersion;
  }

  public void setKeyVersion(long keyVersion) {
    this.keyVersion = keyVersion;
  }

  public long getContainerId() {
    return containerId;
  }

  public void setContainerId(long containerId) {
    this.containerId = containerId;
  }

  public ContainerKeyPrefix toContainerKeyPrefix() {
    return new ContainerKeyPrefix(this.containerId,
        this.keyPrefix, this.keyVersion);
  }

  @Override
  public boolean equals(Object o) {

    if (!(o instanceof KeyPrefixContainer)) {
      return false;
    }
    KeyPrefixContainer that = (KeyPrefixContainer) o;
    return (this.containerId == that.containerId) &&
        this.keyPrefix.equals(that.keyPrefix) &&
        this.keyVersion == that.keyVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerId, keyPrefix, keyPrefix);
  }

}
