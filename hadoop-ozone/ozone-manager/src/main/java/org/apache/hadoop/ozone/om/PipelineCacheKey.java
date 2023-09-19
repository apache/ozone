/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import java.util.Objects;

/**
 * Cache key used by {@link org.apache.hadoop.ozone.om.ScmClient}.
 */
public class PipelineCacheKey {
  private final Long containerId;
  private final boolean datanodeSorted;

  public PipelineCacheKey(Long containerId, boolean datanodeSorted) {
    this.containerId = containerId;
    this.datanodeSorted = datanodeSorted;
  }

  public Long getContainerId() {
    return containerId;
  }

  public boolean isDatanodeSorted() {
    return datanodeSorted;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineCacheKey that = (PipelineCacheKey) o;
    return containerId == that.containerId &&
        datanodeSorted == that.datanodeSorted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerId, datanodeSorted);
  }
}
